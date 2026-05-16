import asyncio
import logging
import random
import json
from datetime import datetime, timedelta
import pytz
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = "1408626"
OZON_API_KEY = "b96a8e5a-092b-44d8-ac4f-a6dc36d1a4fc"
TELEGRAM_TOKEN = "8917837150:AAHh0wOEyTCAub4_FsD3FDqG0uqdO9yZros"
OZON_API_URL = "https://api-seller.ozon.ru"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json",
}

# Кэш кластеров (user_id -> {idx -> {name, orders}})
cluster_cache: dict = {}

# Семафор для параллельных запросов (не более 5 одновременно)
_semaphore = asyncio.Semaphore(5)

# ===== OZON API =====

async def ozon_post(session, url, payload, retry=3, delay=0.5):
    for attempt in range(retry):
        await asyncio.sleep(delay)
        async with session.post(url, json=payload, headers=HEADERS) as resp:
            text = await resp.text()
            logger.info(f"POST {url} status={resp.status}")
            if resp.status == 429:
                wait = 15 * (attempt + 1)
                logger.warning(f"Rate limit, ждём {wait} сек")
                await asyncio.sleep(wait)
                continue
            if resp.status in (401, 403, 404):
                raise Exception(f"Ошибка {resp.status}: {text[:150]}")
            if resp.status != 200:
                raise Exception(f"Ошибка {resp.status}: {text[:200]}")
            return json.loads(text)
    raise Exception("Превышен лимит запросов, попробуй позже")


async def get_all_active_orders(session, max_orders=2000):
    """Получить все активные заявки с пагинацией (до max_orders штук)"""
    all_ids = []
    last_id = 0

    while len(all_ids) < max_orders:
        data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
            "limit": 100,
            "from_supply_order_id": last_id,
            "sort_by": 1,
            "sort_direction": 1,
            "filter": {"states": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
        })
        page_ids = data.get("order_ids", [])
        if not page_ids:
            break
        all_ids.extend(page_ids)
        if len(page_ids) < 100:
            break  # последняя страница
        last_id = page_ids[-1]

    if not all_ids:
        return []

    # Получаем детали батчами по 100
    all_orders = []
    for i in range(0, len(all_ids), 50):
        batch = all_ids[i:i+50]
        details = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
            "order_ids": batch
        })
        all_orders.extend(details.get("orders", []))

    skip = {"CANCELLED", "COMPLETED", "REJECTED"}
    active = [o for o in all_orders if o.get("state", "") not in skip]
    logger.info(f"Всего ID: {len(all_ids)}, получено: {len(all_orders)}, активных: {len(active)}")
    return active


async def get_data_filling_orders(session):
    """Только DATA_FILLING для переноса"""
    orders = await get_all_active_orders(session)
    return [o for o in orders if o.get("state") == "DATA_FILLING"]


async def get_cluster_names(session):
    """
    Получить маппинг macrolocal_cluster_id → название кластера.
    Структура ответа: clusters[].macrolocal_cluster_id + clusters[].name
    Пример: {4039: "Москва, МО и Дальние регионы", 4071: "Ростов", ...}
    """
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/cluster/list", {
            "cluster_type": "CLUSTER_TYPE_OZON"
        })
        result = {}
        for cluster in data.get("clusters", []):
            macro_id = str(cluster.get("macrolocal_cluster_id", ""))
            name = cluster.get("name", "")
            if macro_id and name:
                result[macro_id] = name
        logger.info(f"Cluster names map: {result}")
        return result
    except Exception as e:
        logger.warning(f"Cluster names error: {e}")
        return {}


async def get_bundle_items(session, bundle_id):
    """Получить SKU и товары из bundle заявки"""
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle", {
            "bundle_ids": [bundle_id],
            "limit": 100,
            "last_id": ""
        })
        return data
    except Exception as e:
        logger.warning(f"Bundle error: {e}")
        return {}


async def get_bundle_items_fast(session, bundle_id):
    """Быстрый вариант без лишнего логирования (для параллельной загрузки)"""
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle", {
            "bundle_ids": [bundle_id],
            "limit": 100,
            "last_id": ""
        }, delay=0.3)
        return data
    except Exception as e:
        logger.warning(f"Bundle error {bundle_id[:8]}: {e}")
        return {}


async def update_timeslot(session, supply_order_id, time_from, time_to):
    # delay=2 — пауза для write-операций, чтобы не превысить rate limit
    return await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/update", {
        "supply_order_id": supply_order_id,
        "timeslot": {"from": time_from, "to": time_to}
    }, delay=2)


# ===== ЛОГИКА ПЕРЕНОСА =====

def get_current_order_date(order):
    try:
        from_str = order["timeslot"]["timeslot"]["from"]
        dt_utc = datetime.fromisoformat(from_str.replace("Z", "+00:00"))
        return dt_utc.astimezone(MOSCOW_TZ).date()
    except (KeyError, TypeError, ValueError):
        return None


async def process_reschedule() -> str:
    results, errors = [], []
    today = datetime.now(MOSCOW_TZ).date()
    deadline = today + timedelta(days=3)  # отбираем заявки с датой до сегодня+3

    async with aiohttp.ClientSession() as session:
        orders = await get_data_filling_orders(session)
        if not orders:
            return "📭 Нет заявок со статусом «Заполнение данных»."

        # Фильтруем: только заявки с датой отгрузки от сегодня до +3 дней
        near_orders = [
            o for o in orders
            if (d := get_current_order_date(o)) is not None and today <= d <= deadline
        ]
        logger.info(f"DATA_FILLING: {len(orders)}, ближайших (до {deadline}): {len(near_orders)}")

        if not near_orders:
            return (
                f"📭 Нет заявок для переноса.\n"
                f"Всего DATA_FILLING: {len(orders)}\n"
                f"Ближайших (от сегодня до {deadline.strftime('%d.%m')}): 0\n\n"
                f"Все заявки уже достаточно далеко по датам."
            )

        for order in near_orders:
            oid = order.get("order_id")
            onum = order.get("order_number", str(oid))
            try:
                current_date = get_current_order_date(order)

                # Случайное смещение +10..+28 дней от СЕГОДНЯ
                random_days = random.randint(10, 28)
                target_date = today + timedelta(days=random_days)

                time_from = f"{target_date.strftime('%Y-%m-%d')}T16:00:00Z"
                time_to   = f"{target_date.strftime('%Y-%m-%d')}T17:00:00Z"

                result = await update_timeslot(session, oid, time_from, time_to)
                logger.info(f"Update {onum}: {result}")

                if not result.get("errors"):
                    cd = current_date.strftime('%d.%m') if current_date else '?'
                    results.append(f"✅ {onum}: {cd} → {target_date.strftime('%d.%m')} (+{random_days}д от сегодня)")
                else:
                    errors.append(f"❌ {onum}: {result.get('errors')}")
            except Exception as e:
                logger.exception(f"Ошибка для {onum}")
                errors.append(f"❌ {onum}: {str(e)}")

    lines = [
        f"📦 Всего DATA_FILLING: {len(orders)}",
        f"📅 Ближайших (до {deadline.strftime('%d.%m')}): {len(near_orders)}",
        f"🔀 Перенесены на +10..+28 дней случайно:\n",
    ]
    if results:
        lines.extend(results)
    if errors:
        lines.append("\nПроблемы:")
        lines.extend(errors)
    return "\n".join(lines)


# ===== ЛОГИКА КЛАСТЕРОВ =====

STATE_NAMES = {
    "DATA_FILLING": "🟡 Заполнение данных",
    "READY_TO_SUPPLY": "🟢 Готово к отгрузке",
    "ACCEPTED": "🟢 Принято",
    "IN_TRANSIT": "🚚 В пути",
}


async def load_clusters(user_id: int):
    """Загрузить заявки и сгруппировать по кластерам"""
    async with aiohttp.ClientSession() as session:
        orders = await get_all_active_orders(session)
        cluster_names = await get_cluster_names(session)

    clusters = {}
    for order in orders:
        # Берём macrolocal_cluster_id из supplies
        supplies = order.get("supplies", [])
        cluster_id = None
        if supplies:
            cluster_id = str(supplies[0].get("macrolocal_cluster_id") or "")
        if not cluster_id:
            cluster_id = "unknown"

        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(order)

    # Сохраняем в кэш как список для индексации
    cluster_list = []
    for cid, corders in clusters.items():
        cluster_list.append({"id": cid, "orders": corders, "names": cluster_names})

    cluster_cache[user_id] = cluster_list
    return cluster_list


async def load_cluster_skus(user_id: int, cluster_idx: int) -> dict:
    """Загрузить SKU для кластера. {sku: {name, total_qty, orders: [{...}]}}"""
    cluster_list = cluster_cache.get(user_id)
    if not cluster_list or cluster_idx >= len(cluster_list):
        return {}

    cluster = cluster_list[cluster_idx]
    all_orders = cluster["orders"]  # все активные
    sku_map = {}

    # Дедупликация по bundle_id (для получения списка уникальных SKU)
    seen_bundles = {}
    for order in all_orders:
        supplies = order.get("supplies", [])
        if supplies:
            bid = supplies[0].get("bundle_id")
            if bid and bid not in seen_bundles:
                seen_bundles[bid] = order
    unique_orders = list(seen_bundles.values())
    logger.info(f"Всего заявок: {len(all_orders)}, уникальных bundle: {len(unique_orders)}")

    async with aiohttp.ClientSession() as session:
        # Последовательно — rate limit не позволяет параллельные bundle запросы
        for order in unique_orders:
            onum = order.get("order_number", "?")
            raw_state = order.get("state", "")
            state_name = STATE_NAMES.get(raw_state, raw_state)
            try:
                from_str = order["timeslot"]["timeslot"]["from"]
                ship_dt = datetime.fromisoformat(from_str.replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
            except Exception:
                ship_dt = datetime.max.replace(tzinfo=MOSCOW_TZ)

            supplies = order.get("supplies", [])
            bundle_id = supplies[0].get("bundle_id") if supplies else None
            if not bundle_id:
                continue

            try:
                bundle_data = await get_bundle_items_fast(session, bundle_id)
                items = bundle_data.get("items") or []
                logger.info(f"Bundle {onum}: {len(items)} items, state={raw_state}")
            except Exception as e:
                logger.warning(f"Bundle error {onum}: {e}")
                continue
        for item in items:
            sku = str(item.get("sku") or item.get("product_id") or "")
            if not sku:
                continue
            name = item.get("name") or "—"
            qty = item.get("quantity") or 0
            if sku not in sku_map:
                sku_map[sku] = {"name": name, "orders": []}
            sku_map[sku]["orders"].append({
                "order_number": onum,
                "ship_dt": ship_dt,        # datetime для сортировки
                "date_str": ship_dt.strftime("%d.%m.%Y %H:%M"),
                "qty": qty,
                "state": state_name,
            })

    # Для каждого SKU: все заявки → фильтр DATA_FILLING → сортировка → 5 ближайших
    for sku in sku_map:
        all_sku_orders = sku_map[sku]["orders"]
        df_orders = [o for o in all_sku_orders if o.get("raw_state") == "DATA_FILLING"]
        df_orders.sort(key=lambda x: x["ship_dt"])
        sku_map[sku]["orders"] = df_orders[:5]
        sku_map[sku]["total_qty"] = sum(o["qty"] for o in sku_map[sku]["orders"])
        sku_map[sku]["all_count"] = len(all_sku_orders)  # всего заявок по SKU

    logger.info(f"SKU map: {list(sku_map.keys())}")
    cluster_list[cluster_idx]["sku_map"] = sku_map
    return sku_map


def format_sku_detail(cluster_name: str, sku: str, sku_map: dict) -> str:
    """Форматируем детали по конкретному SKU (5 ближайших заявок)"""
    info = sku_map.get(sku, {})
    product_name = info.get("name", "—")
    orders = info.get("orders", [])
    total_qty = info.get("total_qty", 0)

    all_count = info.get("all_count", len(orders))
    lines = [
        f"📍 {cluster_name}",
        f"🏷 {product_name}",
        f"SKU: {sku}",
        f"Всего заявок: {all_count} | Показаны 5 ближайших DATA_FILLING: {total_qty} шт.",
        "",
    ]
    for o in orders:
        lines.append(f"📋 {o['order_number']} | {o['state']}")
        lines.append(f"   📅 {o['date_str']} | {o['qty']} шт.")
        lines.append("")

    return "\n".join(lines)


async def get_cluster_details(user_id: int, cluster_idx: int) -> str:
    """Заглушка — теперь не используется напрямую"""
    return ""


# ===== TELEGRAM =====

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Перенести заявки на день вперёд", callback_data="reschedule")],
        [InlineKeyboardButton(text="🔍 Поиск по кластерам и SKU", callback_data="clusters")],
    ])


def again_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Запустить снова", callback_data="reschedule")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu")],
    ])


def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К списку кластеров", callback_data="clusters")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
    ])


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Привет! Я бот для управления заявками на поставку Ozon FBO.\n\n"
        "Выбери действие:",
        reply_markup=main_keyboard()
    )


@dp.callback_query(F.data == "menu")
async def on_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "👋 Привет! Я бот для управления заявками на поставку Ozon FBO.\n\n"
        "Выбери действие:",
        reply_markup=main_keyboard()
    )


@dp.callback_query(F.data == "reschedule")
async def on_reschedule(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("⏳ Обрабатываю заявки, подождите...")
    try:
        result = await process_reschedule()
    except Exception as e:
        logger.exception("reschedule error")
        result = f"❗ Ошибка: {str(e)}"
    await callback.message.edit_text(result, reply_markup=again_keyboard())


@dp.callback_query(F.data == "clusters")
async def on_clusters(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("⏳ Загружаю список кластеров...")
    try:
        cluster_list = await load_clusters(callback.from_user.id)
        if not cluster_list:
            await callback.message.edit_text(
                "📭 Активных заявок не найдено.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")
                ]])
            )
            return

        # Строим клавиатуру с кластерами
        buttons = []
        for i, cluster in enumerate(cluster_list):
            cid = cluster["id"]
            names = cluster.get("names", {})
            display = names.get(str(cid), f"Кластер {cid}")
            count = len(cluster["orders"])
            buttons.append([InlineKeyboardButton(
                text=f"📍 {display} ({count} заявок)",
                callback_data=f"cluster:{i}"
            )])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])

        await callback.message.edit_text(
            f"📍 Найдено кластеров: {len(cluster_list)}\nВыбери кластер:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
    except Exception as e:
        logger.exception("clusters error")
        await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=main_keyboard())


@dp.callback_query(F.data.startswith("cluster:"))
async def on_cluster_detail(callback: CallbackQuery):
    await callback.answer()
    idx = int(callback.data.split(":")[1])
    try:
        await callback.message.edit_text("⏳ Загружаю SKU кластера...")
    except Exception:
        pass
    try:
        uid = callback.from_user.id
        cluster_list = cluster_cache.get(uid, [])
        if not cluster_list or idx >= len(cluster_list):
            await callback.message.edit_text("❗ Данные устарели, нажми кнопку кластеров снова.")
            return

        cluster = cluster_list[idx]
        cluster_id = cluster["id"]
        names = cluster.get("names", {})
        cluster_display = names.get(str(cluster_id), f"Кластер {cluster_id}")

        sku_map = await load_cluster_skus(uid, idx)
        if not sku_map:
            await callback.message.edit_text(
                f"📍 {cluster_display}\n\nℹ️ Нет SKU в заявках этого кластера.",
                reply_markup=back_keyboard()
            )
            return

        # Строим кнопки по SKU
        buttons = []
        for sku, info in sku_map.items():
            name = info["name"]
            total_qty = sum(o["qty"] for o in info["orders"])
            # Укорачиваем название для кнопки
            short_name = name[:30] + "..." if len(name) > 30 else name
            buttons.append([InlineKeyboardButton(
                text=f"{short_name} | {total_qty} шт.",
                callback_data=f"sku:{idx}:{sku}"
            )])
        buttons.append([InlineKeyboardButton(text="◀️ К кластерам", callback_data="clusters")])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])

        order_count = len(cluster["orders"])
        await callback.message.edit_text(
            f"📍 {cluster_display}\n"
            f"Заявок: {order_count} | SKU: {len(sku_map)}\n\n"
            f"Выбери SKU для просмотра заявок:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
    except Exception as e:
        logger.exception("cluster detail error")
        try:
            await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=back_keyboard())
        except Exception:
            pass


@dp.callback_query(F.data.startswith("sku:"))
async def on_sku_detail(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split(":")
    cluster_idx = int(parts[1])
    sku = parts[2]

    try:
        uid = callback.from_user.id
        cluster_list = cluster_cache.get(uid, [])
        if not cluster_list or cluster_idx >= len(cluster_list):
            await callback.message.edit_text("❗ Данные устарели.")
            return

        cluster = cluster_list[cluster_idx]
        cluster_id = cluster["id"]
        names = cluster.get("names", {})
        cluster_display = names.get(str(cluster_id), f"Кластер {cluster_id}")
        sku_map = cluster.get("sku_map", {})

        text = format_sku_detail(cluster_display, sku, sku_map)
        if len(text) > 4000:
            text = text[:4000] + "\n...(обрезано)"

        back_to_cluster = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К SKU кластера", callback_data=f"cluster:{cluster_idx}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
        ])
        await callback.message.edit_text(text, reply_markup=back_to_cluster)
    except Exception as e:
        logger.exception("sku detail error")
        try:
            await callback.message.edit_text(f"❗ Ошибка: {str(e)}")
        except Exception:
            pass


async def main():
    logger.info("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

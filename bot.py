import asyncio
import logging
import random
import json
import os
from datetime import datetime, timedelta
import pytz
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = os.environ["OZON_CLIENT_ID"]
OZON_API_KEY   = os.environ["OZON_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OZON_API_URL   = "https://api-seller.ozon.ru"
MOSCOW_TZ      = pytz.timezone("Europe/Moscow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json",
}

# Кэш кластеров (user_id -> list[{id, orders, names}])
cluster_cache: dict = {}

STATE_NAMES = {
    "DATA_FILLING":    "🟡 Заполнение данных",
    "READY_TO_SUPPLY": "🟢 Готово к отгрузке",
    "ACCEPTED":        "🟢 Принято",
    "IN_TRANSIT":      "🚚 В пути",
}


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
    """Получить все активные заявки с пагинацией."""
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
            break
        last_id = page_ids[-1]

    if not all_ids:
        return []

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
    """Только DATA_FILLING."""
    orders = await get_all_active_orders(session)
    return [o for o in orders if o.get("state") == "DATA_FILLING"]


async def get_cluster_names(session):
    """
    Получить маппинг macrolocal_cluster_id → название кластера.
    Возвращает: {"4039": "Москва, МО и Дальние регионы", ...}
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
        logger.info(f"Cluster names: {result}")
        return result
    except Exception as e:
        logger.warning(f"Cluster names error: {e}")
        return {}


async def get_warehouse_names(session):
    """
    Получить маппинг warehouse_id → название склада через /v1/warehouse/fbo/list.
    Возвращает: {"12345": "Хоругвино", ...}
    """
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/warehouse/fbo/list", {})
        result = {}
        for wh in data.get("warehouses", []):
            wid = str(wh.get("warehouse_id", "") or wh.get("id", ""))
            name = wh.get("name", "") or wh.get("warehouse_name", "")
            if wid and name:
                result[wid] = name
        logger.info(f"Warehouse names: {result}")
        return result
    except Exception as e:
        logger.warning(f"Warehouse names error: {e}")
        return {}


async def get_bundle_items_fast(session, bundle_id):
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle", {
            "bundle_ids": [bundle_id],
            "limit": 100,
            "last_id": ""
        }, delay=0.3)
        return data
    except Exception as e:
        logger.warning(f"Bundle error {bundle_id[:8] if bundle_id else '?'}: {e}")
        return {}


async def update_timeslot(session, supply_order_id, time_from, time_to):
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
    deadline = today + timedelta(days=3)

    async with aiohttp.ClientSession() as session:
        orders = await get_data_filling_orders(session)
        if not orders:
            return "📭 Нет заявок со статусом «Заполнение данных»."

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
            oid  = order.get("order_id")
            onum = order.get("order_number", str(oid))
            try:
                current_date = get_current_order_date(order)
                random_days  = random.randint(10, 28)
                target_date  = today + timedelta(days=random_days)
                time_from    = f"{target_date.strftime('%Y-%m-%d')}T16:00:00Z"
                time_to      = f"{target_date.strftime('%Y-%m-%d')}T17:00:00Z"

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


# ===== ЛОГИКА КЛАСТЕРОВ (только DATA_FILLING) =====

async def load_clusters_data_filling(user_id: int):
    """
    Загружает только заявки DATA_FILLING, группирует по кластерам и складам.
    Структура кэша:
      cluster_cache[user_id] = [
        {
          "id": "4039",
          "display_name": "Москва, МО ...",
          "orders": [...],          # все DATA_FILLING заявки кластера
          "warehouses": {           # склады внутри кластера
            "12345": {
              "name": "Хоругвино",
              "orders": [...]
            }
          },
          "names": {...},           # общий маппинг cluster_id -> name
          "wh_names": {...},        # общий маппинг warehouse_id -> name
        },
        ...
      ]
    """
    async with aiohttp.ClientSession() as session:
        all_orders   = await get_all_active_orders(session)
        cluster_names = await get_cluster_names(session)
        wh_names      = await get_warehouse_names(session)

    # Фильтруем только DATA_FILLING
    df_orders = [o for o in all_orders if o.get("state") == "DATA_FILLING"]
    logger.info(f"DATA_FILLING заявок: {len(df_orders)} из {len(all_orders)} активных")

    # Группировка по кластерам
    clusters_map: dict = {}
    for order in df_orders:
        supplies   = order.get("supplies", [])
        cluster_id = None
        wh_id      = None

        if supplies:
            sup = supplies[0]
            cluster_id = str(sup.get("macrolocal_cluster_id") or "")
            # warehouse_id может быть в supplies или в order_tags
            wh_id = str(
                sup.get("warehouse_id")
                or sup.get("storage_warehouse_id")
                or order.get("order_tags", {}).get("seller_warehouse_id")
                or ""
            )

        if not cluster_id:
            cluster_id = "unknown"
        if not wh_id:
            wh_id = "unknown"

        if cluster_id not in clusters_map:
            clusters_map[cluster_id] = {"orders": [], "warehouses": {}}

        clusters_map[cluster_id]["orders"].append(order)

        if wh_id not in clusters_map[cluster_id]["warehouses"]:
            clusters_map[cluster_id]["warehouses"][wh_id] = {"orders": []}
        clusters_map[cluster_id]["warehouses"][wh_id]["orders"].append(order)

    # Собираем финальный список с именами
    cluster_list = []
    for cid, cdata in clusters_map.items():
        display = cluster_names.get(cid, f"Кластер {cid}") if cid != "unknown" else "Без кластера"

        # Обогащаем склады именами
        warehouses = {}
        for wid, wdata in cdata["warehouses"].items():
            wname = wh_names.get(wid, f"Склад {wid}") if wid != "unknown" else "Неизвестный склад"
            warehouses[wid] = {
                "name": wname,
                "orders": wdata["orders"],
            }

        cluster_list.append({
            "id":           cid,
            "display_name": display,
            "orders":       cdata["orders"],
            "warehouses":   warehouses,
            "names":        cluster_names,
            "wh_names":     wh_names,
        })

    # Сортируем по убыванию количества заявок
    cluster_list.sort(key=lambda x: len(x["orders"]), reverse=True)

    cluster_cache[user_id] = cluster_list
    logger.info(f"Кластеров с DATA_FILLING: {len(cluster_list)}")
    return cluster_list


async def load_cluster_skus(user_id: int, cluster_idx: int) -> dict:
    """
    Загружает SKU для заявок DATA_FILLING указанного кластера.
    Возвращает: {sku: {name, total_qty, orders: [{order_number, date_str, qty, state, wh_name}]}}
    """
    cluster_list = cluster_cache.get(user_id)
    if not cluster_list or cluster_idx >= len(cluster_list):
        return {}

    cluster   = cluster_list[cluster_idx]
    df_orders = cluster["orders"]   # уже только DATA_FILLING
    wh_names  = cluster.get("wh_names", {})
    sku_map   = {}

    # Дедупликация по bundle_id
    seen_bundles = {}
    for order in df_orders:
        supplies = order.get("supplies", [])
        if supplies:
            bid = supplies[0].get("bundle_id")
            if bid and bid not in seen_bundles:
                seen_bundles[bid] = order
    unique_orders = list(seen_bundles.values())
    logger.info(f"DATA_FILLING заявок: {len(df_orders)}, уникальных bundle: {len(unique_orders)}")

    async with aiohttp.ClientSession() as session:
        for order in unique_orders:
            onum     = order.get("order_number", "?")
            supplies = order.get("supplies", [])
            bundle_id = supplies[0].get("bundle_id") if supplies else None

            # Определяем склад для этой заявки
            wh_id = ""
            if supplies:
                sup = supplies[0]
                wh_id = str(
                    sup.get("warehouse_id")
                    or sup.get("storage_warehouse_id")
                    or order.get("order_tags", {}).get("seller_warehouse_id")
                    or ""
                )
            wh_name = wh_names.get(wh_id, f"Склад {wh_id}") if wh_id else "—"

            try:
                from_str = order["timeslot"]["timeslot"]["from"]
                ship_dt  = datetime.fromisoformat(from_str.replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
            except Exception:
                ship_dt = datetime.max.replace(tzinfo=MOSCOW_TZ)

            if not bundle_id:
                continue

            try:
                bundle_data = await get_bundle_items_fast(session, bundle_id)
                items = bundle_data.get("items") or []
                logger.info(f"Bundle {onum}: {len(items)} items")
            except Exception as e:
                logger.warning(f"Bundle error {onum}: {e}")
                continue

            # ИСПРАВЛЕН ОТСТУП: теперь for item in items внутри for order
            for item in items:
                sku = str(item.get("sku") or item.get("product_id") or "")
                if not sku:
                    continue
                name = item.get("name") or "—"
                qty  = item.get("quantity") or 0

                if sku not in sku_map:
                    sku_map[sku] = {"name": name, "orders": []}

                sku_map[sku]["orders"].append({
                    "order_number": onum,
                    "ship_dt":      ship_dt,
                    "date_str":     ship_dt.strftime("%d.%m.%Y %H:%M"),
                    "qty":          qty,
                    "state":        STATE_NAMES.get("DATA_FILLING", "DATA_FILLING"),
                    "wh_name":      wh_name,
                })

    # Сортировка и агрегация
    for sku in sku_map:
        orders = sku_map[sku]["orders"]
        orders.sort(key=lambda x: x["ship_dt"])
        sku_map[sku]["orders"]    = orders[:5]
        sku_map[sku]["total_qty"] = sum(o["qty"] for o in orders)
        sku_map[sku]["all_count"] = len(orders)

    logger.info(f"SKU map для кластера {cluster_idx}: {len(sku_map)} SKU")
    cluster_list[cluster_idx]["sku_map"] = sku_map
    return sku_map


def format_sku_detail(cluster_name: str, sku: str, sku_map: dict) -> str:
    info         = sku_map.get(sku, {})
    product_name = info.get("name", "—")
    orders       = info.get("orders", [])
    total_qty    = info.get("total_qty", 0)
    all_count    = info.get("all_count", len(orders))

    lines = [
        f"📍 {cluster_name}",
        f"🏷 {product_name}",
        f"SKU: {sku}",
        f"Всего заявок DATA_FILLING: {all_count} | Показаны 5 ближайших: {total_qty} шт.",
        "",
    ]
    for o in orders:
        lines.append(f"📋 {o['order_number']} | {o['state']}")
        lines.append(f"   🏭 {o['wh_name']}")
        lines.append(f"   📅 {o['date_str']} | {o['qty']} шт.")
        lines.append("")

    return "\n".join(lines)


def format_cluster_overview(cluster: dict) -> str:
    """Форматирует список складов внутри кластера с количеством DATA_FILLING заявок."""
    lines = [
        f"📍 *{cluster['display_name']}*",
        f"Заявок «Заполнение данных»: {len(cluster['orders'])}",
        "",
        "🏭 *Склады:*",
    ]
    warehouses = cluster.get("warehouses", {})
    for wid, wdata in sorted(warehouses.items(), key=lambda x: -len(x[1]["orders"])):
        wname  = wdata["name"]
        wcount = len(wdata["orders"])
        lines.append(f"  • {wname} — {wcount} заявок")

    return "\n".join(lines)


# ===== TELEGRAM =====

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Перенести заявки на день вперёд", callback_data="reschedule")],
        [InlineKeyboardButton(text="🔍 Поиск по кластерам и SKU",        callback_data="clusters")],
    ])


def again_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Запустить снова",   callback_data="reschedule")],
        [InlineKeyboardButton(text="◀️ Главное меню",       callback_data="menu")],
    ])


def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К списку кластеров", callback_data="clusters")],
        [InlineKeyboardButton(text="🏠 Главное меню",        callback_data="menu")],
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
    """
    Главный экран поиска — показывает все кластеры,
    в которых есть заявки со статусом DATA_FILLING.
    """
    await callback.answer()
    await callback.message.edit_text(
        "⏳ Загружаю кластеры с заявками «Заполнение данных»..."
    )
    try:
        cluster_list = await load_clusters_data_filling(callback.from_user.id)

        if not cluster_list:
            await callback.message.edit_text(
                "📭 Нет активных заявок со статусом «Заполнение данных».",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")
                ]])
            )
            return

        # Строим клавиатуру: каждый кластер — отдельная кнопка
        buttons = []
        for i, cluster in enumerate(cluster_list):
            count = len(cluster["orders"])
            wh_count = len(cluster["warehouses"])
            buttons.append([InlineKeyboardButton(
                text=f"📍 {cluster['display_name']} | {count} заявок | {wh_count} склад(ов)",
                callback_data=f"cluster:{i}"
            )])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])

        total_df = sum(len(c["orders"]) for c in cluster_list)
        await callback.message.edit_text(
            f"🟡 *Заявки со статусом «Заполнение данных»*\n\n"
            f"Всего заявок: {total_df}\n"
            f"Кластеров: {len(cluster_list)}\n\n"
            f"Выбери кластер для просмотра складов и SKU:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.exception("clusters error")
        await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=main_keyboard())


@dp.callback_query(F.data.startswith("cluster:"))
async def on_cluster_detail(callback: CallbackQuery):
    """
    Экран кластера — показывает склады с количеством заявок
    и предлагает перейти к SKU.
    """
    await callback.answer()
    idx = int(callback.data.split(":")[1])
    try:
        await callback.message.edit_text("⏳ Загружаю данные кластера...")
    except Exception:
        pass

    try:
        uid          = callback.from_user.id
        cluster_list = cluster_cache.get(uid, [])
        if not cluster_list or idx >= len(cluster_list):
            await callback.message.edit_text(
                "❗ Данные устарели, нажми «Поиск по кластерам» снова.",
                reply_markup=back_keyboard()
            )
            return

        cluster = cluster_list[idx]

        # Сначала загружаем SKU (чтобы кнопки уже были готовы)
        sku_map = await load_cluster_skus(uid, idx)

        # Форматируем обзор складов
        overview = format_cluster_overview(cluster)

        # Кнопки: сначала склады (информационные), потом SKU
        buttons = []

        # Раздел SKU
        if sku_map:
            for sku, info in sku_map.items():
                name       = info["name"]
                total_qty  = info["total_qty"]
                short_name = (name[:28] + "…") if len(name) > 28 else name
                buttons.append([InlineKeyboardButton(
                    text=f"🏷 {short_name} | {total_qty} шт.",
                    callback_data=f"sku:{idx}:{sku}"
                )])
        else:
            overview += "\n\nℹ️ SKU в заявках не найдены."

        buttons.append([InlineKeyboardButton(text="◀️ К кластерам",  callback_data="clusters")])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])

        text = overview
        if sku_map:
            text += f"\n\n📦 *SKU ({len(sku_map)} шт.)* — выбери для деталей:"

        if len(text) > 4000:
            text = text[:4000] + "\n...(обрезано)"

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="Markdown"
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
    parts       = callback.data.split(":")
    cluster_idx = int(parts[1])
    sku         = parts[2]

    try:
        uid          = callback.from_user.id
        cluster_list = cluster_cache.get(uid, [])
        if not cluster_list or cluster_idx >= len(cluster_list):
            await callback.message.edit_text("❗ Данные устарели.")
            return

        cluster      = cluster_list[cluster_idx]
        cluster_name = cluster["display_name"]
        sku_map      = cluster.get("sku_map", {})

        text = format_sku_detail(cluster_name, sku, sku_map)
        if len(text) > 4000:
            text = text[:4000] + "\n...(обрезано)"

        back_to_cluster = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К SKU кластера",  callback_data=f"cluster:{cluster_idx}")],
            [InlineKeyboardButton(text="🏠 Главное меню",     callback_data="menu")],
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

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
import os
OZON_CLIENT_ID = os.environ["OZON_CLIENT_ID"]
OZON_API_KEY   = os.environ["OZON_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OZON_API_URL = "https://api-seller.ozon.ru"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json",
}

cluster_cache: dict = {}

# Семафоры для параллельных запросов
_sem_read   = asyncio.Semaphore(3)   # параллельность для /get и /bundle
_sem_write  = asyncio.Semaphore(1)   # строго последовательно для timeslot/update


# ===== OZON API =====

async def ozon_post(session, url, payload, retry=3, delay=0.1):
    """Базовый POST с повторами при 429."""
    for attempt in range(retry):
        await asyncio.sleep(delay)
        try:
            async with session.post(url, json=payload, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                text = await resp.text()
                if resp.status == 429:
                    wait = 20 * (attempt + 1)
                    logger.warning(f"Rate limit на {url}, ждём {wait} сек (попытка {attempt+1})")
                    await asyncio.sleep(wait)
                    continue
                if resp.status in (401, 403, 404):
                    raise Exception(f"Ошибка {resp.status}: {text[:150]}")
                if resp.status != 200:
                    raise Exception(f"Ошибка {resp.status}: {text[:200]}")
                return json.loads(text)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout на {url}, попытка {attempt+1}")
            if attempt == retry - 1:
                raise
    raise Exception("Превышен лимит запросов, попробуй позже")


async def fetch_order_batch(session, batch: list) -> list:
    """Загрузить один батч заявок из /get — параллельно через семафор."""
    async with _sem_read:
        data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
            "order_ids": batch
        }, delay=0.05)
        return data.get("orders", [])


async def fetch_bundle(session, bundle_id: str) -> tuple[str, list]:
    """Загрузить один bundle — параллельно через семафор."""
    async with _sem_read:
        try:
            data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle", {
                "bundle_ids": [bundle_id],
                "limit": 100,
                "last_id": ""
            }, delay=0.05)
            items = data.get("items") or []
            return bundle_id, items
        except Exception as e:
            logger.warning(f"Bundle error {str(bundle_id)[:12]}: {e}")
            return bundle_id, []


async def get_all_active_orders(session):
    """
    Загрузить ВСЕ активные заявки без хардкода лимита.

    Стратегия: сортировка убывающая (новые первыми).
    Грузим страницы батчами по 5 страниц (500 ID), сразу получаем детали
    и проверяем сколько активных. Останавливаемся когда 3 батча подряд
    не принесли ни одной новой активной заявки — значит добрались до
    исторических завершённых заявок.
    """
    active_states = {"DATA_FILLING", "READY_TO_SUPPLY", "ACCEPTED", "IN_TRANSIT"}
    all_active = []
    last_id = 0
    page = 0
    empty_batches = 0        # счётчик батчей без активных заявок
    MAX_EMPTY_BATCHES = 3    # останавливаемся после 3 пустых батчей подряд
    PAGES_PER_BATCH = 5      # страниц за один цикл (500 ID)

    while True:
        # ── Собираем 500 ID (5 страниц по 100) ────────────────────────────────
        batch_ids = []
        for _ in range(PAGES_PER_BATCH):
            data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
                "limit": 100,
                "from_supply_order_id": last_id,
                "sort_by": 1,
                "sort_direction": 2,
            }, delay=0.1)
            page_ids = data.get("order_ids", [])
            if not page_ids:
                break
            batch_ids.extend(page_ids)
            page += 1
            if len(page_ids) < 100:
                break
            last_id = page_ids[-1]

        if not batch_ids:
            break

        # ── Параллельно получаем детали для этих 500 ID ───────────────────────
        sub_batches = [batch_ids[i:i + 50] for i in range(0, len(batch_ids), 50)]
        tasks = [fetch_order_batch(session, b) for b in sub_batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_active = 0
        for r in results:
            if isinstance(r, list):
                for order in r:
                    if order.get("state", "") in active_states:
                        all_active.append(order)
                        new_active += 1

        logger.info(f"Страница {page}: +{new_active} активных (всего {len(all_active)})")

        if new_active == 0:
            empty_batches += 1
            if empty_batches >= MAX_EMPTY_BATCHES:
                logger.info(f"Остановка: {MAX_EMPTY_BATCHES} батча подряд без активных заявок")
                break
        else:
            empty_batches = 0  # сбрасываем счётчик если нашли активные

        # Если последняя страница была неполной — больше нет данных
        if len(batch_ids) < PAGES_PER_BATCH * 100:
            break

    logger.info(f"Итого активных заявок: {len(all_active)}")
    return all_active


async def get_data_filling_orders(session):
    orders = await get_all_active_orders(session)
    return [o for o in orders if o.get("state") == "DATA_FILLING"]


async def get_cluster_names(session):
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


async def update_timeslot(session, supply_order_id, time_from, time_to):
    async with _sem_write:
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
            oid = order.get("order_id")
            onum = order.get("order_number", str(oid))
            try:
                current_date = get_current_order_date(order)
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


# ===== ЛОГИКА КЛАСТЕРОВ И СКЛАДОВ =====

STATE_NAMES = {
    "DATA_FILLING":    "🟡 Заполнение данных",
    "READY_TO_SUPPLY": "🟢 Готово к отгрузке",
    "ACCEPTED":        "🟢 Принято",
    "IN_TRANSIT":      "🚚 В пути",
}


def get_order_group_key(order, cluster_names: dict) -> tuple[str, str, str]:
    """
    Новые заявки → кластер (macrolocal_cluster_id).
    Старые заявки → склад (storage_warehouse).
    Возвращает (group_key, display_name, group_type)
    """
    supplies = order.get("supplies", [])
    cluster_id, warehouse_name = "", ""
    for supply in supplies:
        cid = str(supply.get("macrolocal_cluster_id") or "").strip()
        wh  = str(supply.get("storage_warehouse") or "").strip()
        if cid and cid != "0":
            cluster_id = cid
        if wh:
            warehouse_name = wh

    if cluster_id:
        name = cluster_names.get(cluster_id, f"Кластер {cluster_id}")
        return f"cluster:{cluster_id}", name, "cluster"
    elif warehouse_name:
        return f"warehouse:{warehouse_name}", warehouse_name, "warehouse"
    else:
        return "unknown", "Без группы", "unknown"


def get_order_warehouse(order) -> str:
    for supply in order.get("supplies", []):
        wh = str(supply.get("storage_warehouse") or "").strip()
        if wh:
            return wh
    return ""


async def load_clusters(user_id: int):
    """Загрузить ВСЕ заявки и сгруппировать по кластерам / складам."""
    async with aiohttp.ClientSession() as session:
        orders = await get_all_active_orders(session)
        cluster_names = await get_cluster_names(session)

    groups: dict = {}
    for order in orders:
        key, display_name, gtype = get_order_group_key(order, cluster_names)
        if key not in groups:
            groups[key] = {"display_name": display_name, "type": gtype, "orders": []}
        groups[key]["orders"].append(order)

    type_order = {"cluster": 0, "warehouse": 1, "unknown": 2}
    cluster_list = sorted(
        [
            {
                "key": key,
                "display_name": info["display_name"],
                "type": info["type"],
                "orders": info["orders"],
                "cluster_names": cluster_names,
                "sku_map": None,
            }
            for key, info in groups.items()
        ],
        key=lambda x: (type_order.get(x["type"], 9), x["display_name"])
    )

    cluster_cache[user_id] = cluster_list
    logger.info(
        f"Групп: {len(cluster_list)} "
        f"(кластеров: {sum(1 for g in cluster_list if g['type']=='cluster')}, "
        f"складов: {sum(1 for g in cluster_list if g['type']=='warehouse')})"
    )
    return cluster_list


async def load_cluster_skus(user_id: int, cluster_idx: int) -> dict:
    """
    Загрузить SKU для группы.
    bundle-запросы идут ПАРАЛЛЕЛЬНО (до 8 одновременно).
    """
    cluster_list = cluster_cache.get(user_id)
    if not cluster_list or cluster_idx >= len(cluster_list):
        return {}

    cluster = cluster_list[cluster_idx]
    if cluster.get("sku_map") is not None:
        return cluster["sku_map"]

    all_orders = cluster["orders"]
    logger.info(f"Группа '{cluster['display_name']}': {len(all_orders)} заявок")

    # ── Шаг 1: уникальные bundle_id ───────────────────────────────────────────
    seen_bundles: set = set()
    for order in all_orders:
        for supply in order.get("supplies", []):
            bid = supply.get("bundle_id")
            if bid:
                seen_bundles.add(bid)

    logger.info(f"Уникальных bundle_id: {len(seen_bundles)} — параллельная загрузка...")

    # ── Шаг 2: загружаем все bundle ПАРАЛЛЕЛЬНО ────────────────────────────────
    bundle_to_items: dict = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_bundle(session, bid) for bid in seen_bundles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"Bundle gather error: {r}")
        else:
            bid, items = r
            bundle_to_items[bid] = items
            skus = [str(i.get("sku") or i.get("product_id", "?")) for i in items]
            logger.info(f"Bundle {str(bid)[:12]}: {len(items)} items → SKU: {skus}")

    # ── Шаг 3: строим sku_map ─────────────────────────────────────────────────
    sku_map: dict = {}
    for order in all_orders:
        onum      = order.get("order_number", "?")
        raw_state = order.get("state", "")
        state_name = STATE_NAMES.get(raw_state, raw_state)
        warehouse = get_order_warehouse(order)

        try:
            from_str = order["timeslot"]["timeslot"]["from"]
            ship_dt = datetime.fromisoformat(from_str.replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
        except Exception:
            ship_dt = datetime.max.replace(tzinfo=MOSCOW_TZ)

        added_skus: set = set()
        for supply in order.get("supplies", []):
            bundle_id = supply.get("bundle_id")
            if not bundle_id:
                continue
            supply_wh = str(supply.get("storage_warehouse") or "").strip()
            effective_wh = supply_wh or warehouse

            for item in bundle_to_items.get(bundle_id, []):
                sku = str(item.get("sku") or item.get("product_id") or "")
                if not sku or sku in added_skus:
                    continue
                added_skus.add(sku)

                name = item.get("name") or "—"
                qty  = item.get("quantity") or 0

                if sku not in sku_map:
                    sku_map[sku] = {"name": name, "orders": []}

                sku_map[sku]["orders"].append({
                    "order_number": onum,
                    "ship_dt":      ship_dt,
                    "date_str":     ship_dt.strftime("%d.%m.%Y %H:%M"),
                    "qty":          qty,
                    "state":        state_name,
                    "raw_state":    raw_state,
                    "warehouse":    effective_wh,
                })

    # ── Шаг 4: фильтр DATA_FILLING → дедупликация → топ-5 ────────────────────
    for sku in sku_map:
        all_sku_orders = sku_map[sku]["orders"]
        seen_nums: set = set()
        dedup = []
        for o in all_sku_orders:
            if o["order_number"] not in seen_nums:
                seen_nums.add(o["order_number"])
                dedup.append(o)
        df_orders = [o for o in dedup if o.get("raw_state") == "DATA_FILLING"]
        df_orders.sort(key=lambda x: x["ship_dt"])
        sku_map[sku]["all_count"]  = len(dedup)
        sku_map[sku]["orders"]     = df_orders[:5]
        sku_map[sku]["total_qty"]  = sum(o["qty"] for o in sku_map[sku]["orders"])

    logger.info(f"SKU map: {len(sku_map)} SKU → {list(sku_map.keys())}")
    cluster["sku_map"] = sku_map
    return sku_map


def format_cluster_full(group_name: str, group_type: str, sku_map: dict) -> list[str]:
    icon = "📍" if group_type == "cluster" else "🏭"
    parts = []
    current = f"{icon} {group_name}\n\n"

    for sku, info in sku_map.items():
        product_name = info.get("name", "—")
        orders = info.get("orders", [])

        block = f"🏷 {product_name}\nSKU: {sku}\n"
        if orders:
            for o in orders:
                wh_line = f" | 🏭 {o['warehouse']}" if o.get("warehouse") else ""
                block += f"📋 {o['order_number']} | {o['state']}{wh_line}\n"
                block += f"📅 {o['date_str']} | {o['qty']} шт.\n"
        else:
            block += "ℹ️ Нет заявок «Заполнение данных»\n"
        block += "\n"

        if len(current) + len(block) > 3800:
            parts.append(current.rstrip())
            current = f"{icon} {group_name} (продолжение)\n\n"
        current += block

    if current.strip():
        parts.append(current.rstrip())

    return parts if parts else [f"{icon} {group_name}\n\nℹ️ Нет данных по SKU."]


# ===== TELEGRAM =====

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Перенести заявки",          callback_data="reschedule")],
        [InlineKeyboardButton(text="🔍 Поиск по кластерам и SKU",  callback_data="clusters")],
    ])


def again_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Запустить снова", callback_data="reschedule")],
        [InlineKeyboardButton(text="◀️ Главное меню",    callback_data="menu")],
    ])


def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К списку",    callback_data="clusters")],
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
    await callback.message.edit_text("⏳ Загружаю заявки (может занять 1–2 мин)...")
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

        buttons = []
        cluster_count   = sum(1 for g in cluster_list if g["type"] == "cluster")
        warehouse_count = sum(1 for g in cluster_list if g["type"] == "warehouse")

        for i, group in enumerate(cluster_list):
            icon  = "📍" if group["type"] == "cluster" else "🏭"
            count = len(group["orders"])
            buttons.append([InlineKeyboardButton(
                text=f"{icon} {group['display_name']} ({count} заявок)",
                callback_data=f"cluster:{i}"
            )])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])

        header_parts = []
        if cluster_count:
            header_parts.append(f"📍 Кластеров: {cluster_count}")
        if warehouse_count:
            header_parts.append(f"🏭 Складов: {warehouse_count}")

        await callback.message.edit_text(
            "\n".join(header_parts) + "\n\nВыбери кластер или склад:",
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
        await callback.message.edit_text("⏳ Загружаю SKU...")
    except Exception:
        pass

    try:
        uid = callback.from_user.id
        cluster_list = cluster_cache.get(uid, [])
        if not cluster_list or idx >= len(cluster_list):
            await callback.message.edit_text(
                "❗ Данные устарели, нажми кнопку кластеров снова.",
                reply_markup=back_keyboard()
            )
            return

        group      = cluster_list[idx]
        group_name = group["display_name"]
        group_type = group["type"]

        sku_map = await load_cluster_skus(uid, idx)

        if not sku_map:
            icon = "📍" if group_type == "cluster" else "🏭"
            await callback.message.edit_text(
                f"{icon} {group_name}\n\nℹ️ Нет SKU в заявках (bundle_id не найдены).\n"
                f"Заявок в группе: {len(group['orders'])}",
                reply_markup=back_keyboard()
            )
            return

        text_parts = format_cluster_full(group_name, group_type, sku_map)

        await callback.message.edit_text(
            text_parts[0],
            reply_markup=back_keyboard() if len(text_parts) == 1 else None
        )
        for i, part in enumerate(text_parts[1:], start=1):
            is_last = (i == len(text_parts) - 1)
            await callback.message.answer(
                part,
                reply_markup=back_keyboard() if is_last else None
            )

    except Exception as e:
        logger.exception("cluster detail error")
        try:
            await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=back_keyboard())
        except Exception:
            pass


async def main():
    logger.info("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

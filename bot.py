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
# Время последней загрузки кэша (user_id -> datetime)
cluster_cache_time: dict = {}
# TTL кэша — 10 минут
CACHE_TTL_SECONDS = 600

STATE_NAMES = {
    "DATA_FILLING":    "🟡 Заполнение данных",
    "READY_TO_SUPPLY": "🟢 Готово к отгрузке",
    "ACCEPTED":        "🟢 Принято",
    "IN_TRANSIT":      "🚚 В пути",
}


# ===== OZON API =====

async def ozon_post(session, url, payload, retry=5, delay=0.5):
    import aiohttp as _aiohttp
    for attempt in range(retry):
        await asyncio.sleep(delay)
        try:
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
        except (_aiohttp.ServerDisconnectedError, _aiohttp.ClientConnectorError,
                _aiohttp.ClientOSError) as e:
            wait = 3 * (attempt + 1)
            logger.warning(f"Соединение разорвано ({e}), ждём {wait}с, попытка {attempt+1}/{retry}")
            await asyncio.sleep(wait)
            continue
    raise Exception("Сервер недоступен, попробуй позже")


async def get_all_active_orders(session, max_orders=10000):
    """Получить все активные заявки с пагинацией."""
    return await _fetch_orders_by_states(session, states=[1,2,3,4,5,6,7,8,9,10], max_orders=max_orders)


async def get_data_filling_orders_fast(session, max_orders=5000):
    """Получить DATA_FILLING заявки — один проход DESC (новые сначала)."""
    orders = await _fetch_orders_by_states(session, states=[1], max_orders=max_orders, sort_direction=2)
    logger.info(f"DATA_FILLING получено: {len(orders)}")
    return orders


async def _fetch_orders_by_states(session, states: list, max_orders=10000, sort_direction=1):
    """Базовая функция: получить заявки с заданными статусами."""
    all_ids = []
    last_id = 0
    while len(all_ids) < max_orders:
        data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
            "limit": 100,
            "from_supply_order_id": last_id,
            "sort_by": 1,
            "sort_direction": sort_direction,
            "filter": {"states": states}
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
    for i in range(0, len(all_ids), 100):
        batch = all_ids[i:i+100]
        details = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
            "order_ids": batch
        }, retry=5, delay=1.0)
        all_orders.extend(details.get("orders", []))
        # Пауза каждые 1000 заявок
        if i > 0 and i % 1000 == 0:
            await asyncio.sleep(3)

    logger.info(f"states={states} dir={sort_direction}: ID={len(all_ids)}, получено={len(all_orders)}")
    return all_orders


async def get_data_filling_orders(session):
    """Только DATA_FILLING — используем быстрый фильтр по state=1."""
    return await get_data_filling_orders_fast(session)



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
    Получить маппинг warehouse_id → название склада.
    Пробуем несколько эндпоинтов, собираем объединённый результат.
    """
    result = {}

    def _parse(wh_list):
        for wh in wh_list:
            wid  = str(wh.get("warehouse_id") or wh.get("id") or "")
            name = wh.get("name") or wh.get("warehouse_name") or ""
            if wid and name:
                result[wid] = name

    # Имена складов берём из заявок напрямую — API-запросы не нужны
    return result



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
    deadline = today + timedelta(days=5)

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

async def load_clusters_data_filling(user_id: int, force_refresh: bool = False):
    """
    Загружает только заявки DATA_FILLING, группирует по кластерам и складам.
    Результат кэшируется на CACHE_TTL_SECONDS секунд.
    При force_refresh=True — всегда перезагружает с API.
    """
    # Проверяем кэш
    if not force_refresh and user_id in cluster_cache:
        cached_at = cluster_cache_time.get(user_id)
        age = (datetime.now() - cached_at).total_seconds() if cached_at else CACHE_TTL_SECONDS + 1
        if age < CACHE_TTL_SECONDS:
            logger.info(f"Кэш актуален (возраст {int(age)}с), возвращаем без запроса к API")
            return cluster_cache[user_id]
        else:
            logger.info(f"Кэш устарел ({int(age)}с > {CACHE_TTL_SECONDS}с), обновляем")

    async with aiohttp.ClientSession() as session:
        cluster_names = await get_cluster_names(session)
        wh_names      = await get_warehouse_names(session)

        # Умная загрузка: грузим порциями по 500, пока не стабилизируется
        # число кластеров и уникальных bundle (обычно хватает 500-1000 заявок)
        df_orders = []
        seen_clusters = set()
        seen_bundles  = set()
        stable_rounds = 0
        last_cluster_count = 0
        last_bundle_count  = 0

        async def fetch_batch(from_id, limit=500):
            ids = []
            last = from_id
            while len(ids) < limit:
                data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
                    "limit": 100,
                    "from_supply_order_id": last,
                    "sort_by": 1,
                    "sort_direction": 2,
                    "filter": {"states": [1]}
                })
                page = data.get("order_ids", [])
                if not page:
                    break
                ids.extend(page)
                if len(page) < 100:
                    break
                last = page[-1]
                if len(ids) >= limit:
                    break

            if not ids:
                return [], 0

            orders = []
            for i in range(0, len(ids), 100):
                batch = ids[i:i+100]
                det = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
                    "order_ids": batch
                }, retry=5, delay=1.0)
                orders.extend(det.get("orders", []))
            return orders, ids[-1] if ids else 0

        from_id = 0
        for round_num in range(10):  # максимум 10 раундов × 500 = 5000
            batch_orders, last_id = await fetch_batch(from_id, limit=500)
            if not batch_orders:
                break
            
            df_orders.extend(batch_orders)
            from_id = last_id

            # Считаем уникальные кластеры и bundle в новой порции
            for o in batch_orders:
                supplies = o.get("supplies", [])
                sup = supplies[0] if supplies else {}
                storage_clusters = o.get("storageClusters") or o.get("storage_clusters") or []
                sc = storage_clusters[0] if storage_clusters else {}
                cid = str(sc.get("id") or sup.get("macrolocal_cluster_id") or "")
                bid = sup.get("bundle_id") or ""
                if cid:
                    seen_clusters.add(cid)
                if bid:
                    seen_bundles.add(bid)

            logger.info(f"Раунд {round_num+1}: загружено {len(df_orders)} заявок, "
                        f"кластеров={len(seen_clusters)}, bundle={len(seen_bundles)}")

            # Если кластеры и bundle не растут 2 раунда подряд — хватит
            if len(seen_clusters) == last_cluster_count and len(seen_bundles) == last_bundle_count:
                stable_rounds += 1
                if stable_rounds >= 2:
                    logger.info("Кластеры и bundle стабилизировались, прекращаем загрузку")
                    break
            else:
                stable_rounds = 0

            last_cluster_count = len(seen_clusters)
            last_bundle_count  = len(seen_bundles)

            # Если достигли всех известных кластеров (22) и bundle не растут
            if len(seen_clusters) >= 22 and stable_rounds >= 1:
                logger.info(f"Все кластеры найдены ({len(seen_clusters)}), останавливаемся")
                break

    logger.info(f"DATA_FILLING итого: {len(df_orders)} заявок, {len(seen_clusters)} кластеров, {len(seen_bundles)} bundle")





    # Группировка по storageClusters/storageWarehouses — данные прямо в заявке
    # (из HTML страницы видно: storageClusters[{id, name}], storageWarehouses[{id, name}], supplyWarehouse{id, name})
    groups_map: dict = {}

    for order in df_orders:
        # Кластер размещения — из storageClusters
        storage_clusters   = order.get("storageClusters") or order.get("storage_clusters") or []
        storage_warehouses = order.get("storageWarehouses") or order.get("storage_warehouses") or []
        supply_warehouse   = order.get("supplyWarehouse") or order.get("supply_warehouse") or {}

        # Fallback через supplies
        supplies = order.get("supplies", [])
        sup = supplies[0] if supplies else {}

        sc = storage_clusters[0] if storage_clusters else {}
        cluster_id   = str(sc.get("id") or sup.get("macrolocal_cluster_id") or sup.get("storageClusterId") or "")
        cluster_name = sc.get("name") or cluster_names.get(cluster_id, f"Кластер {cluster_id}")

        sw = storage_warehouses[0] if storage_warehouses else {}
        wh_id   = str(sw.get("id") or sw.get("warehouse_id") or sup.get("storageWarehouseId") or "")
        wh_name = sw.get("name") or sw.get("warehouse_name") or (f"Склад {wh_id}" if wh_id else "Не определён")

        supply_wh_name = supply_warehouse.get("name") or supply_warehouse.get("warehouse_name") or ""

        if cluster_id:
            group_key = f"cluster:{cluster_id}"
            if group_key not in groups_map:
                groups_map[group_key] = {
                    "type": "cluster", "id": cluster_id,
                    "display_name": cluster_name,
                    "orders": [], "warehouses": {},
                    "supply_wh": supply_wh_name,
                }
            groups_map[group_key]["orders"].append(order)
            sub_key = wh_id or "unknown"
            if sub_key not in groups_map[group_key]["warehouses"]:
                groups_map[group_key]["warehouses"][sub_key] = {"name": wh_name, "orders": []}
            groups_map[group_key]["warehouses"][sub_key]["orders"].append(order)

        elif wh_id:
            group_key = f"wh:{wh_id}"
            if group_key not in groups_map:
                groups_map[group_key] = {
                    "type": "warehouse", "id": wh_id,
                    "display_name": f"🏭 {wh_name}",
                    "orders": [], "warehouses": {wh_id: {"name": wh_name, "orders": []}},
                    "supply_wh": supply_wh_name,
                }
            groups_map[group_key]["orders"].append(order)
            groups_map[group_key]["warehouses"][wh_id]["orders"].append(order)

        else:
            group_key = "unknown"
            if group_key not in groups_map:
                groups_map[group_key] = {
                    "type": "warehouse", "id": "unknown",
                    "display_name": "🏭 Без привязки",
                    "orders": [], "warehouses": {"unknown": {"name": "Не определён", "orders": []}},
                    "supply_wh": "",
                }
            groups_map[group_key]["orders"].append(order)
            groups_map[group_key]["warehouses"]["unknown"]["orders"].append(order)

    # Собираем финальный список
    cluster_list = []
    for gdata in groups_map.values():
        cluster_list.append({
            "id":           gdata["id"],
            "type":         gdata["type"],
            "display_name": gdata["display_name"],
            "orders":       gdata["orders"],
            "warehouses":   gdata["warehouses"],
            "supply_wh":    gdata.get("supply_wh", ""),
            "names":        cluster_names,
            "wh_names":     {},
        })


    # Сортируем по убыванию количества заявок
    cluster_list.sort(key=lambda x: len(x["orders"]), reverse=True)

    cluster_cache[user_id] = cluster_list
    cluster_cache_time[user_id] = datetime.now()
    logger.info(f"Кластеров с DATA_FILLING: {len(cluster_list)}, кэш обновлён")
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

    # Дедупликация по bundle_id: один bundle = один API-запрос,
    # но сохраняем ВСЕ заявки с этим bundle для правильного счёта заявок.
    bundle_to_orders: dict = {}
    for order in df_orders:
        supplies = order.get("supplies", [])
        bid = supplies[0].get("bundle_id") if supplies else None
        if not bid:
            continue
        if bid not in bundle_to_orders:
            bundle_to_orders[bid] = []
        bundle_to_orders[bid].append(order)
    logger.info(f"DATA_FILLING заявок в кластере: {len(df_orders)}, уникальных bundle: {len(bundle_to_orders)}, примеры bundle: {list(bundle_to_orders.keys())[:5]}")

    async with aiohttp.ClientSession() as session:
        for bundle_id, b_orders in bundle_to_orders.items():
            # Получаем список SKU из API один раз для этого bundle
            try:
                bundle_data = await get_bundle_items_fast(session, bundle_id)
                items = bundle_data.get("items") or []
                logger.info(f"Bundle {bundle_id[:8]}: {len(items)} items, {len(b_orders)} заявок")
            except Exception as e:
                logger.warning(f"Bundle error {bundle_id[:8]}: {e}")
                continue

            if not items:
                continue

            # Добавляем запись для каждой заявки с этим bundle
            for order in b_orders:
                onum     = order.get("order_number", "?")
                supplies = order.get("supplies", [])
                dow = order.get("drop_off_warehouse") or {}
                sup_s = supplies[0] if supplies else {}
                wh_id = str(
                    dow.get("warehouse_id")
                    or sup_s.get("warehouse_id")
                    or sup_s.get("storage_warehouse_id")
                    or order.get("order_tags", {}).get("seller_warehouse_id")
                    or ""
                )
                wh_name = (
                    dow.get("name") or dow.get("warehouse_name")
                    or wh_names.get(wh_id, "")
                    or (f"Склад {wh_id}" if wh_id else "—")
                )
                try:
                    from_str = order["timeslot"]["timeslot"]["from"]
                    ship_dt  = datetime.fromisoformat(from_str.replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
                except Exception:
                    ship_dt = datetime.max.replace(tzinfo=MOSCOW_TZ)

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
    supply_wh = cluster.get("supply_wh", "")
    lines = [
        f"📍 *{cluster['display_name']}*",
        f"Заявок «Заполнение данных»: {len(cluster['orders'])}",
    ]
    if supply_wh:
        lines.append(f"📦 Точка отгрузки: {supply_wh}")
    lines += ["", "🏭 *Склады хранения:*"]
    warehouses = cluster.get("warehouses", {})
    for wid, wdata in sorted(warehouses.items(), key=lambda x: -len(x[1]["orders"])):
        lines.append(f"  • {wdata['name']} — {len(wdata['orders'])} заявок")
    return "\n".join(lines)


# ===== TELEGRAM =====

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Перенести заявки на день вперёд", callback_data="reschedule")],
        [InlineKeyboardButton(text="🔍 Поиск по кластерам и SKU",        callback_data="clusters")],
        [InlineKeyboardButton(text="🧪 Найти все артикулы в заявках",    callback_data="scan_skus")],
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


@dp.message(F.text.startswith("/findsku"))
async def cmd_findsku(message: types.Message):
    """Ищет заявки с конкретным SKU: /findsku 3479900339"""
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Использование: /findsku <SKU>")
        return
    target_sku = parts[1].strip()
    await message.answer(f"🔍 Ищу SKU {target_sku} в заявках... (~1 мин)")
    
    found = []
    async with aiohttp.ClientSession() as session:
        # Грузим последние 2500 заявок
        orders = await get_data_filling_orders_fast(session)
        # Собираем уникальные bundle
        seen_bundles = {}
        for o in orders:
            supplies = o.get("supplies", [])
            bid = supplies[0].get("bundle_id") if supplies else None
            if bid and bid not in seen_bundles:
                seen_bundles[bid] = o
        
        logger.info(f"findsku: проверяем {len(seen_bundles)} bundle на SKU {target_sku}")
        
        for bid, order in seen_bundles.items():
            try:
                bd = await get_bundle_items_fast(session, bid)
                items = bd.get("items") or []
                for item in items:
                    sku = str(item.get("sku") or item.get("product_id") or "")
                    if sku == target_sku:
                        # Нашли! Берём кластер из заявки
                        sc = (order.get("storageClusters") or [{}])[0]
                        sw = (order.get("storageWarehouses") or [{}])[0]
                        found.append(
                            f"✅ Заявка {order.get('order_number')} | "
                            f"Кластер: {sc.get('name','?')} | "
                            f"Склад: {sw.get('name','?')} | "
                            f"{item.get('name','?')[:40]}"
                        )
            except Exception as e:
                pass
    
    if found:
        text = f"🎯 SKU {target_sku} найден в {len(found)} bundle:\n\n" + "\n".join(found[:20])
    else:
        text = f"❌ SKU {target_sku} не найден в последних 2500 заявках DATA_FILLING"
    
    if len(text) > 4000:
        text = text[:4000] + "\n...(обрезано)"
    await message.answer(text)


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


@dp.callback_query(F.data == "noop")
async def on_noop(callback: CallbackQuery):
    """Кнопка-разделитель — ничего не делает."""
    await callback.answer()


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


def _build_clusters_screen(cluster_list: list, uid: int) -> tuple[str, InlineKeyboardMarkup]:
    """Формирует текст и клавиатуру экрана списка кластеров."""
    cached_at = cluster_cache_time.get(uid)
    if cached_at:
        age_sec = int((datetime.now() - cached_at).total_seconds())
        if age_sec < 60:
            freshness = f"только что"
        elif age_sec < 3600:
            freshness = f"{age_sec // 60} мин. назад"
        else:
            freshness = f"{age_sec // 3600} ч. назад"
        cache_note = f"🕐 Данные обновлены: {freshness}"
    else:
        cache_note = ""

    new_clusters   = [c for c in cluster_list if c.get("type") == "cluster"]
    old_warehouses = [c for c in cluster_list if c.get("type") == "warehouse"]
    total_df = sum(len(c["orders"]) for c in cluster_list)

    text = (
        f"🟡 *Заявки со статусом «Заполнение данных»*\n\n"
        f"Всего заявок: {total_df}\n"
        f"📍 Кластеры (новые заявки): {len(new_clusters)}\n"
        f"🏭 Склады (старые заявки): {len(old_warehouses)}\n"
        f"{cache_note}\n\n"
        f"Выбери кластер или склад:"
    )

    buttons = []
    # Сначала кластеры, потом склады
    if new_clusters:
        for i, cluster in enumerate(cluster_list):
            if cluster.get("type") != "cluster":
                continue
            count    = len(cluster["orders"])
            wh_count = len(cluster["warehouses"])
            wh_label = f"{wh_count} склад" if wh_count == 1 else f"{wh_count} склад(ов)"
            buttons.append([InlineKeyboardButton(
                text=f"📍 {cluster['display_name']} — {count} заявок ({wh_label})",
                callback_data=f"cluster:{i}"
            )])

    if old_warehouses:
        if new_clusters:
            buttons.append([InlineKeyboardButton(
                text="── Старые заявки (по складу) ──",
                callback_data="noop"
            )])
        for i, cluster in enumerate(cluster_list):
            if cluster.get("type") != "warehouse":
                continue
            count = len(cluster["orders"])
            buttons.append([InlineKeyboardButton(
                text=f"{cluster['display_name']} — {count} заявок",
                callback_data=f"cluster:{i}"
            )])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить данные", callback_data="clusters_refresh")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню",    callback_data="menu")])

    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.callback_query(F.data == "scan_skus")
async def on_scan_skus(callback: CallbackQuery):
    """Сканирует заявки и выводит все уникальные артикулы."""
    await callback.answer()
    await callback.message.edit_text("⏳ Сканирую заявки в поисках уникальных артикулов...\n(загружаю порциями по 500)")

    found_skus = {}   # sku -> {name, bundle_id, order_number}
    seen_bundles = set()
    from_id = 0
    total_loaded = 0

    async with aiohttp.ClientSession() as session:
        for round_num in range(20):  # максимум 20 порций × 500 = 10000
            # Грузим 500 ID
            ids = []
            last = from_id
            while len(ids) < 500:
                data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
                    "limit": 100,
                    "from_supply_order_id": last,
                    "sort_by": 1,
                    "sort_direction": 2,
                    "filter": {"states": [1]}
                })
                page = data.get("order_ids", [])
                if not page:
                    break
                ids.extend(page)
                if len(page) < 100:
                    break
                last = page[-1]
            
            if not ids:
                break
            from_id = ids[-1]

            # Получаем детали
            orders = []
            for i in range(0, len(ids), 100):
                det = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
                    "order_ids": ids[i:i+100]
                }, retry=5, delay=1.0)
                orders.extend(det.get("orders", []))
            total_loaded += len(orders)

            # Собираем новые bundle
            new_bundles = []
            for o in orders:
                supplies = o.get("supplies", [])
                sup = supplies[0] if supplies else {}
                bid = sup.get("bundle_id") or ""
                if bid and bid not in seen_bundles:
                    seen_bundles.add(bid)
                    new_bundles.append((bid, o))

            # Запрашиваем bundle только для новых
            for bid, order in new_bundles:
                try:
                    bd = await get_bundle_items_fast(session, bid)
                    items = bd.get("items") or []
                    for item in items:
                        sku = str(item.get("sku") or item.get("product_id") or "")
                        if sku and sku not in found_skus:
                            found_skus[sku] = {
                                "name": item.get("name") or "—",
                                "bundle_id": bid[:8],
                                "order": order.get("order_number", "?"),
                            }
                except Exception:
                    pass

            logger.info(f"Скан раунд {round_num+1}: загружено {total_loaded}, bundle={len(seen_bundles)}, SKU={len(found_skus)}")

            # Обновляем сообщение прогресса каждые 2 раунда
            if round_num % 2 == 1:
                try:
                    await callback.message.edit_text(
                        f"⏳ Сканирую... загружено {total_loaded} заявок\n"
                        f"Найдено bundle: {len(seen_bundles)}, артикулов: {len(found_skus)}"
                    )
                except Exception:
                    pass

            # Если новых bundle не появилось 2 раунда подряд — хватит
            if not new_bundles and round_num > 0:
                break

    # Формируем результат
    if not found_skus:
        text = "❌ Артикулов не найдено"
    else:
        lines = [
            f"✅ Найдено *{len(found_skus)} уникальных артикулов* из {total_loaded} заявок:\n"
        ]
        for sku, info in found_skus.items():
            name = info["name"][:50] + "…" if len(info["name"]) > 50 else info["name"]
            lines.append(f"🏷 *{sku}*")
            lines.append(f"   {name}")
            lines.append(f"   Bundle: {info['bundle_id']}...")
            lines.append("")
        text = "\n".join(lines)

    if len(text) > 4000:
        text = text[:4000] + "\n...(обрезано)"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "clusters")
async def on_clusters(callback: CallbackQuery):
    """Экран кластеров — использует кэш если он свежий."""
    await callback.answer()
    uid = callback.from_user.id

    # Если кэш есть и свежий — показываем мгновенно без edit "Загружаю..."
    cached_at = cluster_cache_time.get(uid)
    has_fresh_cache = (
        uid in cluster_cache
        and cached_at is not None
        and (datetime.now() - cached_at).total_seconds() < CACHE_TTL_SECONDS
    )

    if not has_fresh_cache:
        await callback.message.edit_text("⏳ Загружаю кластеры с заявками «Заполнение данных»...")

    try:
        cluster_list = await load_clusters_data_filling(uid)

        if not cluster_list:
            await callback.message.edit_text(
                "📭 Нет активных заявок со статусом «Заполнение данных».",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить данные", callback_data="clusters_refresh")],
                    [InlineKeyboardButton(text="🏠 Главное меню",    callback_data="menu")],
                ])
            )
            return

        text, kb = _build_clusters_screen(cluster_list, uid)
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.exception("clusters error")
        await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=main_keyboard())


@dp.callback_query(F.data == "clusters_refresh")
async def on_clusters_refresh(callback: CallbackQuery):
    """Принудительное обновление кэша кластеров."""
    await callback.answer("🔄 Обновляю данные...")
    uid = callback.from_user.id
    await callback.message.edit_text("⏳ Загружаю свежие данные с Ozon...")
    try:
        cluster_list = await load_clusters_data_filling(uid, force_refresh=True)

        if not cluster_list:
            await callback.message.edit_text(
                "📭 Нет активных заявок со статусом «Заполнение данных».",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить данные", callback_data="clusters_refresh")],
                    [InlineKeyboardButton(text="🏠 Главное меню",    callback_data="menu")],
                ])
            )
            return

        text, kb = _build_clusters_screen(cluster_list, uid)
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.exception("clusters_refresh error")
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

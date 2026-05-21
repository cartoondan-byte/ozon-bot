import asyncio
import random
import logging
import json
import os
import time
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

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()

# ===== МАППИНГ КЛАСТЕРОВ =====
CLUSTER_NAMES: dict = {}

# ===== КЭШ =====
_cache: dict = {}

# ===== КОНСТАНТЫ ДЛЯ СУПЕРПОСТАВОК =====
STAVROPOL_WAREHOUSE_NAME = "СТАВРОПОЛЬ_АППЗ_2"
EXCLUDED_CLUSTERS = [
    "алматы", "астана", "калининград", "беларус",
    "армени", "казахстан", "кыргызстан", "киргиз"
]
SUPER_PRODUCT_TAG = "super"   # фильтр по названию/тегу Super-товаров


# ===== АДАПТИВНЫЙ RATE LIMITER =====
_bundle_semaphore = asyncio.Semaphore(3)
_bundle_delay     = 0.15

# ===== ГЛОБАЛЬНЫЙ СЕМАФОР ДЛЯ draft/create (1 запрос в минуту) =====
_draft_semaphore  = asyncio.Semaphore(1)
_last_draft_time  = 0.0   # время последнего успешного draft/create (time.monotonic)
_bundle_delay_min = 0.05
_bundle_delay_max = 2.0

def _adjust_delay(success: bool) -> None:
    global _bundle_delay
    if success:
        _bundle_delay = max(_bundle_delay_min, _bundle_delay * 0.9)
    else:
        _bundle_delay = min(_bundle_delay_max, _bundle_delay * 1.5)
    logging.debug(f"bundle_delay={_bundle_delay:.3f}s")


# ===== ЗАГОЛОВКИ =====
def ozon_headers() -> dict:
    return {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }


# ===== POST с retry на 429 =====
async def ozon_post(session: aiohttp.ClientSession, url: str, payload: dict, retries: int = 7) -> dict:
    for attempt in range(retries):
        async with session.post(url, headers=ozon_headers(), json=payload) as resp:
            raw = await resp.text()
            if resp.status == 429:
                wait = 3.0 * (attempt + 1)
                logging.warning(f"429 rate limit, жду {wait}с...")
                await asyncio.sleep(wait)
                continue
            logging.info(f"POST {url} → {resp.status} | {raw[:200]}")
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status} [{url}]: {raw[:200]}")
            return json.loads(raw)
    raise Exception(f"Превышен лимит запросов к {url}")


# ===== POST специально для draft/create (1 раз в минуту лимит) =====
async def ozon_post_draft(session: aiohttp.ClientSession, url: str, payload: dict) -> dict:
    """
    /v1/draft/create имеет лимит 1 запрос в минуту на весь аккаунт.
    Используем глобальный семафор (_draft_semaphore=1), чтобы никогда не
    отправлять два запроса параллельно, плюс гарантируем паузу ≥65 с
    после предыдущего успешного вызова.
    Экспоненциальный backoff при 429: 65 → 90 → 120 → 150 → 180 → 210 → 240 → 270 → 300 → 330 с
    """
    global _last_draft_time
    MAX_ATTEMPTS = 10
    BASE_WAIT    = 65   # секунд — минимальный интервал между запросами

    async with _draft_semaphore:
        for attempt in range(MAX_ATTEMPTS):
            # Гарантируем паузу ≥ BASE_WAIT с момента последнего вызова
            elapsed = time.monotonic() - _last_draft_time
            if elapsed < BASE_WAIT:
                pause = BASE_WAIT - elapsed + 1
                logging.info(f"draft/create: жду {pause:.0f}с до следующей попытки (attempt {attempt+1})...")
                await asyncio.sleep(pause)

            async with session.post(url, headers=ozon_headers(), json=payload) as resp:
                raw = await resp.text()
                logging.info(f"POST {url} → {resp.status} | {raw[:200]}")
                if resp.status == 429:
                    extra_wait = BASE_WAIT + attempt * 30  # 65, 95, 125, 155 …
                    logging.warning(f"draft 429 (attempt {attempt+1}/{MAX_ATTEMPTS}), жду {extra_wait}с...")
                    _last_draft_time = time.monotonic()    # сбрасываем таймер
                    await asyncio.sleep(extra_wait)
                    continue
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status} [{url}]: {raw[:200]}")
                _last_draft_time = time.monotonic()
                return json.loads(raw)

        raise Exception(f"Превышен лимит запросов к {url} после {MAX_ATTEMPTS} попыток")


# ===== POLLING =====
async def poll(session: aiohttp.ClientSession, url: str, payload: dict,
               success_statuses: list, fail_statuses: list,
               status_field: str = "status",
               interval: float = 3.0, max_attempts: int = 20) -> dict:
    for attempt in range(max_attempts):
        data   = await ozon_post(session, url, payload)
        status = data.get(status_field, "")
        logging.info(f"polling {url} → status={status} (attempt {attempt + 1})")
        if status in success_statuses:
            return data
        if status in fail_statuses:
            raise Exception(f"Операция завершилась с ошибкой: {status} | {data}")
        await asyncio.sleep(interval)
    raise Exception(f"Polling {url} превысил {max_attempts} попыток")


# ===== ЗАГРУЗКА НАЗВАНИЙ КЛАСТЕРОВ =====
async def resolve_cluster_names(cluster_ids: list[str]) -> None:
    global CLUSTER_NAMES
    if CLUSTER_NAMES:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{OZON_API_URL}/v1/cluster/list",
                headers=ozon_headers(),
                json={"cluster_type": "CLUSTER_TYPE_OZON"}
            ) as resp:
                raw  = await resp.text()
                logging.info(f"cluster/list OZON → {resp.status} | {raw[:400]}")
                if resp.status == 200:
                    data = json.loads(raw)
                    for c in data.get("clusters", []):
                        cid  = str(c.get("macrolocal_cluster_id", "") or c.get("id", ""))
                        name = c.get("name", "")
                        if cid and name:
                            CLUSTER_NAMES[cid] = name
                    logging.info(f"Загружено кластеров: {len(CLUSTER_NAMES)}")
    except Exception as e:
        logging.warning(f"Ошибка загрузки кластеров: {e}")


def cluster_name(cluster_id: str) -> str:
    name = CLUSTER_NAMES.get(str(cluster_id), "")
    if name:
        return f"Кластер {name}"
    return f"Кластер {cluster_id}"


# ===== НАЗВАНИЯ ТОВАРОВ =====
async def fetch_product_names(product_ids: list) -> dict:
    result = {}
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(product_ids), 1000):
            chunk = product_ids[i:i + 1000]
            try:
                data = await ozon_post(session, f"{OZON_API_URL}/v3/product/info/list", {"product_id": chunk})
                for item in data.get("items", []):
                    pid = item.get("id")
                    if pid:
                        result[pid] = item.get("name", "—")
            except Exception as e:
                logging.warning(f"product/info/list: {e}")
    return result


# ===== ПОЛУЧЕНИЕ order_id =====
async def fetch_supply_order_ids(session: aiohttp.ClientSession) -> list:
    order_ids = []
    last_id   = ""
    limit     = 50
    while True:
        payload = {"filter": {"states": ["DATA_FILLING"]}, "sort_by": 1, "limit": limit}
        if last_id:
            payload["last_id"] = last_id
        data    = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", payload)
        batch   = data.get("order_ids", [])
        if not batch:
            break
        order_ids.extend(batch)
        new_last_id = str(data.get("last_id", ""))
        if not new_last_id or new_last_id == last_id or len(batch) < limit:
            break
        last_id = new_last_id
    return order_ids


# ===== ДЕТАЛИ ЗАЯВОК =====
async def fetch_supply_order_details(session: aiohttp.ClientSession, order_ids: list) -> list[dict]:
    orders = []
    for i in range(0, len(order_ids), 50):
        chunk = order_ids[i:i + 50]
        try:
            data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {"order_ids": chunk})
            orders.extend(data.get("orders", []))
        except Exception as e:
            logging.warning(f"supply-order/get: {e}")
    return orders


# ===== ТОВАРЫ ИЗ БАНДЛОВ =====
async def _fetch_one_bundle(session: aiohttp.ClientSession, bid: str) -> tuple[str, list]:
    global _bundle_delay
    async with _bundle_semaphore:
        await asyncio.sleep(_bundle_delay)
        for attempt in range(4):
            try:
                async with session.post(
                    f"{OZON_API_URL}/v1/supply-order/bundle",
                    headers=ozon_headers(),
                    json={"bundle_ids": [bid], "last_id": "", "limit": 100}
                ) as resp:
                    if resp.status == 429:
                        _adjust_delay(False)
                        wait = _bundle_delay * (attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    raw = await resp.text()
                    if resp.status != 200:
                        return bid, []
                    _adjust_delay(True)
                    data = json.loads(raw)
                    return bid, data.get("items", [])
            except Exception as e:
                logging.warning(f"bundle {bid[:8]} attempt {attempt}: {e}")
                await asyncio.sleep(_bundle_delay)
        return bid, []


async def fetch_bundle_items(session: aiohttp.ClientSession, bundle_ids: list) -> dict:
    tasks = [_fetch_one_bundle(session, bid) for bid in bundle_ids]
    pairs = await asyncio.gather(*tasks)
    return dict(pairs)


# ===== КЛЮЧ И МЕТКА НАЗНАЧЕНИЯ =====
def get_dest_key(order: dict) -> str:
    for supply in order.get("supplies", []):
        storage_wh = supply.get("storage_warehouse") or {}
        wh_name    = storage_wh.get("name", "")
        if wh_name:
            return f"wh::{wh_name}"
        cluster_id = supply.get("macrolocal_cluster_id", "")
        if cluster_id:
            return f"cl::{cluster_id}"
    return "unknown::Без назначения"


def get_dest_label(dest_key: str) -> str:
    if dest_key.startswith("wh::"):
        return dest_key[4:]
    if dest_key.startswith("cl::"):
        return cluster_name(dest_key[4:])
    return dest_key


# ===== ЗАГРУЗКА И ГРУППИРОВКА ВСЕХ ЗАЯВОК =====
async def load_all_orders() -> dict:
    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        logging.info(f"DATA_FILLING: {len(order_ids)} заявок")
        if not order_ids:
            return {}
        orders = await fetch_supply_order_details(session, order_ids)
        bundle_ids = []
        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                if bid:
                    bundle_ids.append(bid)
        bundle_items = await fetch_bundle_items(session, bundle_ids) if bundle_ids else {}
        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                supply["_items"] = bundle_items.get(bid, []) if bid else []

    cluster_ids = set()
    for order in orders:
        for supply in order.get("supplies", []):
            cid = supply.get("macrolocal_cluster_id", "")
            if cid:
                cluster_ids.add(str(cid))
    if cluster_ids:
        await resolve_cluster_names(list(cluster_ids))

    grouped: dict = {}
    for order in orders:
        key = get_dest_key(order)
        grouped.setdefault(key, []).append(order)
    return grouped


# ===== ПЕРЕНОС БЛИЖАЙШИХ ЗАЯВОК =====
async def reschedule_near_orders() -> str:
    """Переносит заявки DATA_FILLING с датой поставки в ближайшие 5 дней
    на случайную дату от +15 до +27 дней от сегодня, слот 19:00-20:00 МСК (16:00-17:00 UTC)."""
    today    = datetime.now(MOSCOW_TZ).date()
    date_to  = today + timedelta(days=5)
    results, errors = [], []

    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        if not order_ids:
            return "📭 Нет заявок со статусом «Заполнение данных»."

        orders = await fetch_supply_order_details(session, order_ids)

    near_orders = []
    for order in orders:
        ts      = order.get("timeslot", {}).get("timeslot", {})
        ts_from = ts.get("from", "")
        if not ts_from:
            continue
        try:
            dt = datetime.strptime(ts_from[:19], "%Y-%m-%dT%H:%M:%S")
            dt = MOSCOW_TZ.localize(dt)
            if today <= dt.date() <= date_to:
                near_orders.append(order)
        except Exception:
            continue

    if not near_orders:
        return (
            f"📭 Заявок DATA_FILLING с датой поставки в ближайшие 5 дней нет.\n"
            f"Всего DATA_FILLING заявок: {len(orders)}"
        )

    async with aiohttp.ClientSession() as session:
        for order in near_orders[:5]:
            order_id  = order.get("order_id")
            order_num = order.get("order_number", str(order_id))
            try:
                ts_from = order["timeslot"]["timeslot"]["from"]
                cur_dt  = MOSCOW_TZ.localize(datetime.strptime(ts_from[:19], "%Y-%m-%dT%H:%M:%S"))
                cur_str = cur_dt.strftime("%d.%m.%Y")

                days_ahead  = random.randint(15, 27)
                target_date = today + timedelta(days=days_ahead)
                time_from = f"{target_date.strftime('%Y-%m-%d')}T16:00:00Z"
                time_to   = f"{target_date.strftime('%Y-%m-%d')}T17:00:00Z"

                resp = await ozon_post(
                    session,
                    f"{OZON_API_URL}/v1/supply-order/timeslot/update",
                    {"supply_order_id": order_id, "timeslot": {"from": time_from, "to": time_to}},
                    retries=3
                )
                logging.info(f"timeslot/update #{order_id} → {resp}")

                errs = resp.get("errors") or []
                op_id = resp.get("operation_id")
                success = not errs

                if op_id and not errs:
                    await asyncio.sleep(2)
                    try:
                        status_resp = await ozon_post(
                            session,
                            f"{OZON_API_URL}/v1/supply-order/timeslot/status",
                            {"supply_order_id": order_id},
                            retries=3
                        )
                        logging.info(f"timeslot/status #{order_id} → {status_resp}")
                        if status_resp.get("status") != "STATUS_SUCCESS":
                            success = False
                    except Exception:
                        pass

                if success:
                    results.append(
                        f"✅ #{order_num}\n"
                        f"   {cur_str} → {target_date.strftime('%d.%m.%Y')} (+{days_ahead}д)"
                    )
                else:
                    fallback_date = today + timedelta(days=15)
                    fb_from = f"{fallback_date.strftime('%Y-%m-%d')}T16:00:00Z"
                    fb_to   = f"{fallback_date.strftime('%Y-%m-%d')}T17:00:00Z"
                    try:
                        fb_resp = await ozon_post(
                            session,
                            f"{OZON_API_URL}/v1/supply-order/timeslot/update",
                            {"supply_order_id": order_id, "timeslot": {"from": fb_from, "to": fb_to}},
                            retries=3
                        )
                        logging.info(f"timeslot/fallback #{order_id} → {fb_resp}")
                        await asyncio.sleep(2)
                        fb_status = await ozon_post(
                            session,
                            f"{OZON_API_URL}/v1/supply-order/timeslot/status",
                            {"supply_order_id": order_id},
                            retries=3
                        )
                        if fb_status.get("status") == "STATUS_SUCCESS":
                            results.append(
                                f"⚠️ #{order_num} (fallback +15д)\n"
                                f"   {cur_str} → {fallback_date.strftime('%d.%m.%Y')}"
                            )
                        else:
                            errors.append(f"❌ #{order_num}: fallback тоже не удался: {fb_status.get('errors')}")
                    except Exception as fb_e:
                        errors.append(f"❌ #{order_num}: {errs} | fallback: {str(fb_e)[:80]}")
            except Exception as e:
                logging.exception(f"reschedule #{order_id}: {e}")
                errors.append(f"❌ #{order_num}: {str(e)[:100]}")

    lines = [
        f"🔄 Перенос ближайших заявок DATA_FILLING",
        f"Найдено: {len(near_orders)} заявок (дата поставки сегодня–{date_to.strftime('%d.%m')})",
        f"Целевое время: 19:00–20:00 МСК\n",
    ]
    if results:
        lines.extend(results)
    if errors:
        lines.append("\nОшибки:")
        lines.extend(errors)
    return "\n".join(lines)


# ===== ПЕРЕНОС ТАЙМСЛОТОВ =====
async def reschedule_near_orders(grouped: dict) -> str:
    moscow_tz = MOSCOW_TZ
    now       = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    date_to   = now + timedelta(days=5)

    near_orders = []
    for orders in grouped.values():
        for order in orders:
            if order.get("state") != "DATA_FILLING":
                continue
            ts      = order.get("timeslot", {}).get("timeslot", {})
            ts_from = ts.get("from", "")
            if not ts_from:
                continue
            try:
                dt = datetime.strptime(ts_from[:19], "%Y-%m-%dT%H:%M:%S")
                dt = moscow_tz.localize(dt)
                if now <= dt <= date_to:
                    near_orders.append(order)
            except Exception:
                continue

    if not near_orders:
        return "📭 Заявок DATA_FILLING в ближайшие 5 дней не найдено."

    results = []
    errors  = []

    async with aiohttp.ClientSession() as session:
        for order in near_orders:
            order_id  = order.get("order_id")
            order_num = order.get("order_number", str(order_id))
            try:
                ts_old = (order.get("timeslot", {}).get("timeslot", {}).get("from", "")[:10])
                random_days = random.randint(15, 27)
                target_date = (now + timedelta(days=random_days)).date()
                time_from = f"{target_date.strftime('%Y-%m-%d')}T16:00:00Z"
                time_to   = f"{target_date.strftime('%Y-%m-%d')}T17:00:00Z"

                resp = await ozon_post(
                    session,
                    f"{OZON_API_URL}/v1/supply-order/timeslot/update",
                    {"supply_order_id": order_id, "timeslot": {"from": time_from, "to": time_to}}
                )
                logging.info(f"timeslot/update #{order_id} → {resp}")

                errs = resp.get("errors", [])
                if not errs:
                    results.append(
                        f"✅ {order_num}: {ts_old} → {target_date.strftime('%d.%m')} (+{random_days}д) 19:00-20:00"
                    )
                    op_id = resp.get("operation_id", "")
                    if op_id:
                        await asyncio.sleep(2)
                        try:
                            status = await ozon_post(
                                session,
                                f"{OZON_API_URL}/v1/supply-order/timeslot/status",
                                {"operation_id": op_id}
                            )
                            logging.info(f"timeslot/status #{order_id} → {status}")
                        except Exception:
                            pass
                else:
                    errors.append(f"❌ {order_num}: {errs}")

                await asyncio.sleep(1)

            except Exception as e:
                logging.warning(f"timeslot update error #{order_id}: {e}")
                errors.append(f"❌ {order_num}: {e}")

    header = (
        "🔄 Перенос ближайших заявок DATA_FILLING\n"
        + f"Найдено: {len(near_orders)} заявок (сегодня–{date_to.strftime('%d.%m')})\n"
        + "Целевое время: 19:00–20:00 МСК\n"
    )
    lines = [header]
    if results:
        lines.extend(results)
    if errors:
        lines.append("\nОшибки:")
        lines.extend(errors)
    return "\n".join(lines)


# ===== СУПЕРПОСТАВКИ: получить все кластеры =====
async def fetch_all_ozon_clusters(session: aiohttp.ClientSession) -> list[dict]:
    data = await ozon_post(session, f"{OZON_API_URL}/v1/cluster/list",
                           {"cluster_type": "CLUSTER_TYPE_OZON"})
    return data.get("clusters", [])


# ===== СУПЕРПОСТАВКИ: найти точку отгрузки СТАВРОПОЛЬ_АППЗ_2 (DROPOFF) =====
async def find_drop_off_warehouse_id(session: aiohttp.ClientSession) -> int:
    """
    Ищет точку отгрузки СТАВРОПОЛЬ_АППЗ_2 через /v1/cluster/list.
    Для типа поставки CROSSDOCK — товар сдаётся в пункт приёма Ozon.
    Возвращает warehouse_id точки.
    """
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/cluster/list",
                               {"cluster_type": "CLUSTER_TYPE_OZON"})
        for cluster in data.get("clusters", []):
            for lc in cluster.get("logistic_clusters", []):
                for wh in lc.get("warehouses", []):
                    name = wh.get("name", "")
                    if STAVROPOL_WAREHOUSE_NAME.lower() in name.lower():
                        wh_id = wh.get("warehouse_id")
                        logging.info(f"Точка отгрузки найдена: id={wh_id} name={name}")
                        return int(wh_id)
        raise Exception(f"Точка '{STAVROPOL_WAREHOUSE_NAME}' не найдена в cluster/list")
    except Exception as e:
        raise Exception(f"Ошибка поиска точки отгрузки: {e}")


# ===== СУПЕРПОСТАВКИ: получить Super-товары =====
async def fetch_super_skus(session: aiohttp.ClientSession) -> list[dict]:
    """Возвращает список {'sku': int, 'name': str} для Super-товаров."""
    skus   = []
    last_id = ""
    limit   = 100

    while True:
        payload = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit":   limit,
            "sort_by": "id",
            "sort_dir": "ASC",
        }
        try:
            data = await ozon_post(session, f"{OZON_API_URL}/v3/product/list", payload)
        except Exception as e:
            logging.warning(f"product/list: {e}")
            break

        items = data.get("items", []) or data.get("result", {}).get("items", [])
        if not items:
            break

        product_ids = [it.get("product_id") for it in items if it.get("product_id")]
        if product_ids:
            try:
                info_data = await ozon_post(
                    session,
                    f"{OZON_API_URL}/v3/product/info/list",
                    {"product_id": product_ids}
                )
                for item in info_data.get("items", []):
                    name = item.get("name", "") or ""
                    # Ищем Super-товары по полю is_super
                    if item.get("is_super"):
                        sku = item.get("sku")
                        if not sku:
                            for src in item.get("sources", []):
                                if src.get("sku"):
                                    sku = src["sku"]
                                    break
                        if sku:
                            skus.append({"sku": int(sku), "name": name})
                            logging.info(f"SUPER найден: sku={sku} name={name[:60]}")
            except Exception as e:
                logging.warning(f"product/info/list batch: {e}")

        new_last_id = str(data.get("last_id", ""))
        if not new_last_id or new_last_id == last_id or len(items) < limit:
            break
        last_id = new_last_id

    logging.info(f"Super-товаров найдено: {len(skus)}")
    return skus


# ===== СУПЕРПОСТАВКИ: найти таймслот (v2 API) =====
async def find_best_timeslot(session: aiohttp.ClientSession,
                              draft_id: int,
                              cluster_id: str,
                              storage_warehouse_id: int,
                              supply_type: str = "DIRECT",
                              day_min: int = 14,
                              day_max: int = 19) -> tuple[str, str]:
    """
    Использует /v2/draft/timeslot/info.
    Принимает draft_id из /v2/draft/create/info (не из v1!).
    Возвращает (from_in_timezone, to_in_timezone) — строки в формате склада.
    """
    today     = datetime.now(MOSCOW_TZ).date()
    date_from = today + timedelta(days=day_min)
    date_to   = today + timedelta(days=day_max)

    payload = {
        "draft_id":    draft_id,
        "date_from":   date_from.strftime("%Y-%m-%d"),
        "date_to":     date_to.strftime("%Y-%m-%d"),
        "supply_type": supply_type,
        "selected_cluster_warehouses": [
            {
                "macrolocal_cluster_id": int(cluster_id),
                "storage_warehouse_id":  storage_warehouse_id,
            }
        ],
    }
    logging.info(f"timeslot/info payload: {json.dumps(payload, ensure_ascii=False)}")
    data = await ozon_post(session, f"{OZON_API_URL}/v2/draft/timeslot/info", payload)

    error_reason = data.get("error_reason", "UNSPECIFIED")
    if error_reason not in ("UNSPECIFIED", ""):
        raise Exception(f"timeslot/info ошибка: {error_reason}")

    # Структура v2: result.drop_off_warehouse_timeslots.days[]
    result_obj = data.get("result", {})
    ts_obj     = result_obj.get("drop_off_warehouse_timeslots", {})
    days       = ts_obj.get("days", [])

    all_slots = []
    for day in days:
        date_str = day.get("date_in_timezone", "")[:10]
        for ts in day.get("timeslots", []):
            all_slots.append((date_str, ts.get("from_in_timezone", ""), ts.get("to_in_timezone", "")))

    if not all_slots:
        raise Exception("Нет доступных таймслотов в диапазоне дат!")

    # Предпочтительные часы, случайный порядок дат
    available_dates = sorted(set(d for d, _, _ in all_slots))
    random.shuffle(available_dates)
    preferred_hours = ["19:00", "18:00", "17:00", "20:00"]

    for target_date in available_dates:
        slots = [(f, t) for d, f, t in all_slots if d == target_date]
        for hour in preferred_hours:
            for ts_from, ts_to in slots:
                if hour in ts_from:
                    return ts_from, ts_to
        if slots:
            return slots[0]

    raise Exception("Не удалось подобрать слот!")


# ===== СУПЕРПОСТАВКИ: создать одну заявку для кластера (v2 API) =====
async def create_supply_for_cluster(session: aiohttp.ClientSession,
                                     cluster_id: str,
                                     cluster_name_str: str,
                                     drop_off_warehouse_id: int,
                                     sku: int,
                                     sku_name: str) -> dict:
    """
    Создаёт одну заявку (1 SKU, 5 шт) для указанного кластера.
    Тип поставки: CROSSDOCK — товар сдаётся в точку СТАВРОПОЛЬ_АППЗ_2.
    Использует v2 API:
      /v1/draft/crossdock/create → draft_id
      /v2/draft/create/info      → polling + storage_warehouse_id
      /v2/draft/timeslot/info    → таймслоты
      /v2/draft/supply/create    → создание заявки
      /v2/draft/supply/create/status → polling по draft_id
    """
    result = {
        "cluster":   cluster_name_str,
        "sku":       sku,
        "sku_name":  sku_name,
        "qty":       5,
        "success":   False,
        "order_id":  None,
        "timeslot":  "",
        "error":     "",
    }

    try:
        # ── 1. Создать черновик CROSSDOCK ───────────────────────────────────
        draft_payload = {
            "cluster_info": {
                "items": [{"sku": sku, "quantity": 5}],
                "macrolocal_cluster_id": int(cluster_id),
            },
            "deletion_sku_mode": "PARTIAL",
            "delivery_info": {
                "type": "DROPOFF",
                "drop_off_warehouse": {
                    "warehouse_id":   drop_off_warehouse_id,
                    "warehouse_type": "ORDERS_RECEIVING_POINT",
                },
            },
        }
        logging.info(f"crossdock/create payload: {json.dumps(draft_payload, ensure_ascii=False)}")
        draft_resp = await ozon_post_draft(
            session, f"{OZON_API_URL}/v1/draft/crossdock/create", draft_payload
        )
        draft_id = draft_resp.get("draft_id")
        if not draft_id:
            errs = draft_resp.get("errors", [])
            raise Exception(f"direct/create не вернул draft_id: {draft_resp} | errors: {errs}")
        logging.info(f"draft_id={draft_id} для кластера {cluster_name_str}")

        # ── 2. Polling /v2/draft/create/info ───────────────────────────────
        info = await poll(
            session,
            f"{OZON_API_URL}/v2/draft/create/info",
            {"draft_id": draft_id},
            success_statuses=["SUCCESS"],
            fail_statuses=["FAILED"],
            status_field="status",
            interval=3.0,
            max_attempts=20,
        )
        logging.info(f"draft/create/info: {json.dumps(info, ensure_ascii=False)[:400]}")

        draft_errors = info.get("errors", [])
        if draft_errors:
            msgs = [e.get("error_message", "") for e in draft_errors]
            if any(m not in ("UNSPECIFIED", "") for m in msgs):
                raise Exception(f"Ошибки черновика: {msgs}")

        # ── 3. Найти storage_warehouse_id ─────────────────────────────────
        # В v2 структура: clusters[].warehouses[].storage_warehouse.warehouse_id
        storage_wh_id = None
        for cl in info.get("clusters", []):
            for wh in cl.get("warehouses", []):
                avail = wh.get("availability_status", {})
                # Берём доступный склад; если нет — берём первый как fallback
                wh_id = wh.get("storage_warehouse", {}).get("warehouse_id")
                if wh_id:
                    if avail.get("state", "") in ("", "UNSPECIFIED") or storage_wh_id is None:
                        storage_wh_id = wh_id
                    if avail.get("state", "") == "AVAILABLE":
                        storage_wh_id = wh_id
                        break
            if storage_wh_id:
                break

        if not storage_wh_id:
            raise Exception(f"Не найден storage_warehouse_id в info: {json.dumps(info)[:300]}")
        logging.info(f"storage_warehouse_id={storage_wh_id}")

        # ── 4. Получить таймслот (/v2/draft/timeslot/info) ─────────────────
        ts_from, ts_to = await find_best_timeslot(
            session,
            draft_id=draft_id,
            cluster_id=cluster_id,
            storage_warehouse_id=storage_wh_id,
            supply_type="CROSSDOCK",
        )
        logging.info(f"Выбран слот: {ts_from} – {ts_to}")

        # ── 5. Создать заявку (/v2/draft/supply/create) ────────────────────
        supply_payload = {
            "draft_id": draft_id,
            "selected_cluster_warehouses": [
                {
                    "macrolocal_cluster_id": int(cluster_id),
                    "storage_warehouse_id":  storage_wh_id,
                }
            ],
            "timeslot": {
                "from_in_timezone": ts_from,
                "to_in_timezone":   ts_to,
            },
            "supply_type": "CROSSDOCK",
        }
        logging.info(f"supply/create payload: {json.dumps(supply_payload, ensure_ascii=False)}")
        supply_resp = await ozon_post(
            session, f"{OZON_API_URL}/v2/draft/supply/create", supply_payload
        )
        logging.info(f"supply/create response: {supply_resp}")

        supply_errs = supply_resp.get("error_reasons", [])
        if supply_errs and any(e not in ("UNSPECIFIED", "") for e in supply_errs):
            raise Exception(f"supply/create ошибки: {supply_errs}")

        # v2 возвращает draft_id, не operation_id — поллим по нему
        resp_draft_id = supply_resp.get("draft_id") or draft_id

        # ── 6. Polling /v2/draft/supply/create/status ──────────────────────
        status_data = await poll(
            session,
            f"{OZON_API_URL}/v2/draft/supply/create/status",
            {"draft_id": resp_draft_id},
            success_statuses=["SUCCESS"],
            fail_statuses=["FAILED"],
            status_field="status",
            interval=3.0,
            max_attempts=20,
        )
        logging.info(f"supply/create/status: {status_data}")

        status_errs = status_data.get("error_reasons", [])
        if status_errs and any(e not in ("UNSPECIFIED", "") for e in status_errs):
            raise Exception(f"Ошибки создания заявки: {status_errs}")

        order_id = status_data.get("order_id")
        if not order_id:
            raise Exception(f"order_id не получен: {status_data}")

        # Форматируем слот для отображения (строки уже в часовом поясе склада)
        ts_str = f"{ts_from[:16]} – {ts_to[:16]}"

        result["success"]  = True
        result["order_id"] = order_id
        result["timeslot"] = ts_str

    except Exception as e:
        logging.exception(f"create_supply_for_cluster {cluster_name_str}: {e}")
        result["error"] = str(e)[:300]

    return result


# ===== СУПЕРПОСТАВКИ: ТЕСТ — одна заявка в Воронеж =====
async def run_super_supply_test() -> str:
    """
    Тестовый режим: создаёт одну заявку в кластер Воронеж
    для первого найденного Super-товара.
    """
    lines = ["🧪 ТЕСТ: Создание заявки в кластер Воронеж\n"]

    async with aiohttp.ClientSession() as session:
        # Найти точку отгрузки СТАВРОПОЛЬ_АППЗ_2
        try:
            drop_off_wh_id = await find_drop_off_warehouse_id(session)
            lines.append(f"📦 Точка отгрузки: {STAVROPOL_WAREHOUSE_NAME} (id={drop_off_wh_id})")
        except Exception as e:
            return f"❌ Не найдена точка отгрузки: {e}"

        # Найти Super-товары
        lines.append("🔍 Ищу Super-товары...")
        super_skus = await fetch_super_skus(session)
        if not super_skus:
            return "\n".join(lines) + "\n❌ Super-товары не найдены!"
        lines.append(f"✅ Super-товаров: {len(super_skus)}")

        # Берём первый SKU для теста
        test_item = super_skus[0]
        sku       = test_item["sku"]
        sku_name  = test_item["name"]
        lines.append(f"🏷 Тестовый SKU: {sku} | {sku_name[:50]}")

        # Найти кластер Воронеж
        clusters = await fetch_all_ozon_clusters(session)
        voronezh = None
        for c in clusters:
            if "воронеж" in c.get("name", "").lower():
                voronezh = c
                break
        if not voronezh:
            return "\n".join(lines) + "\n❌ Кластер Воронеж не найден!"

        cluster_id  = str(voronezh.get("id") or voronezh.get("macrolocal_cluster_id", ""))
        cluster_nm  = voronezh.get("name", "Воронеж")
        lines.append(f"🏭 Кластер: {cluster_nm} (id={cluster_id})\n")
        lines.append("⏳ Создаю заявку...")

        # Создать заявку
        res = await create_supply_for_cluster(
            session,
            cluster_id=cluster_id,
            cluster_name_str=cluster_nm,
            drop_off_warehouse_id=drop_off_wh_id,
            sku=sku,
            sku_name=sku_name
        )

    lines.append("")
    if res["success"]:
        lines.append(f"✅ Заявка создана!")
        lines.append(f"   Кластер:  {res['cluster']}")
        lines.append(f"   SKU:      {res['sku']}")
        lines.append(f"   Товар:    {res['sku_name'][:60]}")
        lines.append(f"   Кол-во:   {res['qty']} шт")
        lines.append(f"   Слот:     {res['timeslot']}")
        lines.append(f"   Order ID: {res['order_id']}")
    else:
        lines.append(f"❌ Заявка НЕ создана в кластер {res['cluster']}")
        lines.append(f"   Ошибка: {res['error']}")

    return "\n".join(lines)


# ===== СУПЕРПОСТАВКИ: ПОЛНЫЙ ЗАПУСК — все кластеры =====
async def run_super_supply_all() -> str:
    """
    Создаёт заявки во все кластеры OZON кроме исключённых.
    Для каждого кластера: 1 артикул (первый Super-товар), 5 шт.
    """
    lines = ["🚀 Суперпоставки: создание заявок во все кластеры\n"]

    async with aiohttp.ClientSession() as session:
        # Найти точку отгрузки СТАВРОПОЛЬ_АППЗ_2
        try:
            drop_off_wh_id = await find_drop_off_warehouse_id(session)
            lines.append(f"📦 Точка отгрузки: {STAVROPOL_WAREHOUSE_NAME} (id={drop_off_wh_id})")
        except Exception as e:
            return f"❌ Не найдена точка отгрузки: {e}"

        # Найти Super-товары
        super_skus = await fetch_super_skus(session)
        if not super_skus:
            return "\n".join(lines) + "\n❌ Super-товары не найдены!"
        lines.append(f"✅ Super-товаров: {len(super_skus)}")

        # Берём первый SKU
        test_item = super_skus[0]
        sku       = test_item["sku"]
        sku_name  = test_item["name"]
        lines.append(f"🏷 SKU: {sku} | {sku_name[:50]}\n")

        # Получить все кластеры
        clusters = await fetch_all_ozon_clusters(session)
        lines.append(f"📋 Всего кластеров OZON: {len(clusters)}")

        # Фильтруем исключённые
        active_clusters = []
        for c in clusters:
            name_lower = c.get("name", "").lower()
            if any(excl in name_lower for excl in EXCLUDED_CLUSTERS):
                lines.append(f"⏭ Пропуск: {c.get('name')} (в списке исключений)")
                continue
            active_clusters.append(c)

        lines.append(f"✅ Кластеров для создания заявок: {len(active_clusters)}\n")
        lines.append("⏳ Создаю заявки...\n")

        results  = []
        errors   = []

        for c in active_clusters:
            cluster_id = str(c.get("id") or c.get("macrolocal_cluster_id", ""))
            cluster_nm = c.get("name", cluster_id)

            res = await create_supply_for_cluster(
                session,
                cluster_id=cluster_id,
                cluster_name_str=cluster_nm,
                drop_off_warehouse_id=drop_off_wh_id,
                sku=sku,
                sku_name=sku_name
            )

            if res["success"]:
                results.append(
                    f"✅ {cluster_nm}\n"
                    f"   SKU: {res['sku']} | {res['sku_name'][:40]}\n"
                    f"   Кол-во: {res['qty']} шт | {res['timeslot']}\n"
                    f"   Order ID: {res['order_id']}"
                )
            else:
                errors.append(
                    f"❌ Заявка НЕ создана → {cluster_nm}\n"
                    f"   Ошибка: {res['error']}"
                )

            await asyncio.sleep(1)  # пауза между кластерами

    lines.append(f"━━━ ИТОГ ━━━")
    lines.append(f"Создано: {len(results)} | Ошибок: {len(errors)}\n")
    lines.extend(results)
    if errors:
        lines.append("\n— Ошибки —")
        lines.extend(errors)

    return "\n".join(lines)


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Ближайшие заявки на перенос", callback_data="show_skus")],
        [InlineKeyboardButton(text="🔄 Перенести ближайшие заявки",  callback_data="do_reschedule")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку",          callback_data="show_supplies")],
        [InlineKeyboardButton(text="➕ Создание заявок",             callback_data="create_orders_menu")],
    ])


# ===== МЕНЮ СКЛАДОВ/КЛАСТЕРОВ =====
def dest_menu(grouped: dict) -> InlineKeyboardMarkup:
    buttons = []
    for dest_key, orders in sorted(grouped.items(), key=lambda x: get_dest_label(x[0])):
        label = f"{get_dest_label(dest_key)} ({len(orders)})"
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"dest::{dest_key[:50]}"
        )])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить",     callback_data="show_supplies")])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ===== СТАРТ =====
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass
    chat_id    = message.chat.id
    message_id = message.message_id
    for mid in range(message_id - 1, max(message_id - 50, 0), -1):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text="Привет! Выбери действие:", reply_markup=main_menu())


# ===== ГЛАВНОЕ МЕНЮ (callback) =====
@dp.callback_query(F.data == "main_menu")
async def handle_main_menu(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("Выбери действие:", reply_markup=main_menu())


# ===== БЛИЖАЙШИЕ ЗАЯВКИ НА ПЕРЕНОС =====
@dp.callback_query(F.data == "show_skus")
async def handle_show_skus(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Ищу заявки с датой поставки в ближайшие 5 дней...")

    grouped = _cache.get("grouped")
    if not grouped:
        try:
            grouped = await load_all_orders()
            _cache["grouped"] = grouped
        except Exception as e:
            await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
            return

    all_orders = [order for orders in grouped.values() for order in orders]
    now        = datetime.now(MOSCOW_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    date_to    = now + timedelta(days=5)

    near_orders = []
    for order in all_orders:
        ts      = order.get("timeslot", {}).get("timeslot", {})
        ts_from = ts.get("from", "")
        if not ts_from:
            continue
        try:
            dt = datetime.strptime(ts_from[:19], "%Y-%m-%dT%H:%M:%S")
            dt = MOSCOW_TZ.localize(dt)
            if now <= dt <= date_to:
                near_orders.append((dt, order))
        except Exception:
            continue

    near_orders.sort(key=lambda x: x[0])

    if not near_orders:
        await call.message.edit_text(
            "📭 Заявок с датой поставки в ближайшие 5 дней не найдено.",
            reply_markup=main_menu()
        )
        return

    all_pids = set()
    for _, order in near_orders:
        for supply in order.get("supplies", []):
            for item in supply.get("_items", []):
                pid = item.get("product_id")
                if pid:
                    all_pids.add(pid)

    names = await fetch_product_names(list(all_pids)) if all_pids else {}
    lines = [f"📅 Заявок в ближайшие 5 дней: {len(near_orders)}\n"]

    for dt, order in near_orders:
        order_id  = order.get("order_id", "—")
        order_num = order.get("order_number", "")
        created   = order.get("created_date", "")[:10]
        deadline  = (order.get("data_filling_deadline") or "")[:10]
        dest      = get_dest_label(get_dest_key(order))

        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"📋 #{order_id} ({order_num})")
        lines.append(f"🏭 Назначение: {dest}")
        lines.append(f"🗓 Дата поставки: {dt.strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"📅 Создана: {created}")
        if deadline:
            lines.append(f"⏰ Дедлайн: {deadline}")

        for supply in order.get("supplies", []):
            items = supply.get("_items", [])
            if items:
                lines.append("Товары:")
                for item in items:
                    sku  = item.get("sku", "—")
                    pid  = item.get("product_id")
                    qty  = item.get("quantity", "—")
                    name = names.get(pid, item.get("name", "—"))
                    lines.append(f"  • SKU: {sku} | {name} | кол-во: {qty}")
        lines.append("")

    text_full = "\n".join(lines)
    chunks    = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]
    back_kb   = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])

    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        await call.message.answer(chunk, reply_markup=back_kb if i == len(chunks) - 1 else None)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Список выше", reply_markup=back_kb)


# ===== ПЕРЕНОС — ОБРАБОТЧИК =====
@dp.callback_query(F.data == "do_reschedule")
async def handle_do_reschedule(call: CallbackQuery):
    await call.answer()

    grouped = _cache.get("grouped")
    if not grouped:
        await call.message.edit_text("⏳ Загружаю заявки...")
        try:
            grouped = await load_all_orders()
            _cache["grouped"] = grouped
        except Exception as e:
            await call.message.edit_text(f"❌ Ошибка загрузки:\n{e}", reply_markup=main_menu())
            return

    moscow_tz = MOSCOW_TZ
    now       = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    date_to   = now + timedelta(days=5)
    count = 0
    for orders in grouped.values():
        for order in orders:
            if order.get("state") != "DATA_FILLING":
                continue
            ts = order.get("timeslot", {}).get("timeslot", {}).get("from", "")
            try:
                dt = moscow_tz.localize(datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S"))
                if now <= dt <= date_to:
                    count += 1
            except Exception:
                pass

    if count == 0:
        await call.message.edit_text(
            "📭 Нет заявок DATA_FILLING с датой поставки в ближайшие 5 дней.",
            reply_markup=main_menu()
        )
        return

    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Да, перенести {count} заявок", callback_data="confirm_reschedule")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")],
    ])
    await call.message.edit_text(
        f"🔄 Найдено заявок DATA_FILLING в ближайшие 5 дней: {count}\n\n"
        f"Они будут перенесены на случайную дату +15..+27 дней от сегодня, слот 19:00-20:00 МСК.\n\n"
        f"Продолжить?",
        reply_markup=confirm_kb
    )


@dp.callback_query(F.data == "confirm_reschedule")
async def handle_confirm_reschedule(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Переношу заявки... Это может занять 1-2 минуты.")

    grouped = _cache.get("grouped")
    if not grouped:
        await call.message.edit_text("❌ Данные устарели. Обновите заявки через «Заявки на поставку».",
                                     reply_markup=main_menu())
        return

    try:
        result = await reschedule_near_orders(grouped)
    except Exception as e:
        logging.exception("reschedule error")
        result = f"❌ Ошибка: {e}"

    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])
    if len(result) > 4000:
        chunks = [result[i:i+4000] for i in range(0, len(result), 4000)]
        await call.message.edit_text(chunks[0])
        for chunk in chunks[1:]:
            await call.message.answer(chunk)
        await call.message.answer("⬆️ Готово!", reply_markup=back_kb)
    else:
        await call.message.edit_text(result, reply_markup=back_kb)


# ===== ПЕРЕНОС ЗАЯВОК (callback) =====
@dp.callback_query(F.data == "do_reschedule")
async def handle_do_reschedule(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text(
        "⏳ Ищу заявки с датой поставки в ближайшие 5 дней и переношу...\n"
        "Это может занять до 1 минуты."
    )
    try:
        result = await reschedule_near_orders()
    except Exception as e:
        result = f"❌ Ошибка: {e}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Запустить снова",  callback_data="do_reschedule")],
        [InlineKeyboardButton(text="🏠 Главное меню",     callback_data="main_menu")],
    ])
    if len(result) > 4000:
        result = result[:4000] + "\n...(обрезано)"
    await call.message.edit_text(result, reply_markup=kb)


# ===== ЗАЯВКИ — ПОКАЗАТЬ СКЛАДЫ/КЛАСТЕРЫ =====
@dp.callback_query(F.data == "show_supplies")
async def handle_show_supplies(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Загружаю заявки «Заполнение данных»...\nЭто может занять ~1 мин")

    try:
        grouped = await load_all_orders()
        _cache["grouped"] = grouped
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
        return

    if not grouped:
        await call.message.edit_text("📭 Заявок со статусом «Заполнение данных» нет.", reply_markup=main_menu())
        return

    total = sum(len(v) for v in grouped.values())
    await call.message.edit_text(
        f"🚚 Заявки «Заполнение данных»: {total}\nВыбери склад или кластер:",
        reply_markup=dest_menu(grouped)
    )


# ===== ВЫБРАН СКЛАД/КЛАСТЕР =====
@dp.callback_query(F.data.startswith("dest::"))
async def handle_dest_select(call: CallbackQuery):
    await call.answer()
    dest_key = call.data[6:]
    grouped  = _cache.get("grouped", {})

    matched_key = None
    for k in grouped:
        if k[:50] == dest_key:
            matched_key = k
            break

    if not matched_key:
        await call.message.edit_text("❌ Данные устарели, нажми «Заявки на поставку» снова.", reply_markup=main_menu())
        return

    orders = grouped[matched_key]
    label  = get_dest_label(matched_key)

    all_pids = set()
    for order in orders:
        for supply in order.get("supplies", []):
            for item in supply.get("_items", []):
                pid = item.get("product_id")
                if pid:
                    all_pids.add(pid)

    names = await fetch_product_names(list(all_pids)) if all_pids else {}
    lines = [f"🏭 {label} — заявок: {len(orders)}\n"]

    for order in orders:
        order_id  = order.get("order_id", "—")
        order_num = order.get("order_number", "")
        created   = order.get("created_date", "")[:10]
        deadline  = (order.get("data_filling_deadline") or "")[:10]
        ts        = order.get("timeslot", {}).get("timeslot", {})
        ts_from   = (ts.get("from") or "")[:10]

        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"📋 #{order_id} ({order_num})")
        lines.append(f"📅 Создана: {created}")
        if ts_from:
            lines.append(f"🗓 Дата поставки: {ts_from}")
        if deadline:
            lines.append(f"⏰ Дедлайн: {deadline}")

        for supply in order.get("supplies", []):
            items = supply.get("_items", [])
            if items:
                lines.append("Товары:")
                for item in items:
                    sku  = item.get("sku", "—")
                    pid  = item.get("product_id")
                    qty  = item.get("quantity", "—")
                    name = names.get(pid, item.get("name", "—"))
                    lines.append(f"  • SKU: {sku} | {name} | кол-во: {qty}")
        lines.append("")

    text_full = "\n".join(lines)
    chunks    = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]
    back_kb   = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад к складам", callback_data="back_to_dests")],
        [InlineKeyboardButton(text="🏠 Главное меню",    callback_data="main_menu")],
    ])

    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        await call.message.answer(chunk, reply_markup=back_kb if i == len(chunks) - 1 else None)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Список выше", reply_markup=back_kb)


# ===== НАЗАД К СКЛАДАМ =====
@dp.callback_query(F.data == "back_to_dests")
async def handle_back_to_dests(call: CallbackQuery):
    await call.answer()
    grouped = _cache.get("grouped", {})
    if not grouped:
        await call.message.edit_text("⏳ Данные устарели, перезагружаю...")
        try:
            grouped = await load_all_orders()
            _cache["grouped"] = grouped
        except Exception as e:
            await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
            return

    total = sum(len(v) for v in grouped.values())
    await call.message.edit_text(
        f"🚚 Заявки «Заполнение данных»: {total}\nВыбери склад или кластер:",
        reply_markup=dest_menu(grouped)
    )


# ===== МЕНЮ СОЗДАНИЯ ЗАЯВОК =====
@dp.callback_query(F.data == "create_orders_menu")
async def handle_create_orders_menu(call: CallbackQuery):
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Суперпоставки (тест — Воронеж)", callback_data="super_supply_test")],
        [InlineKeyboardButton(text="🚀 Суперпоставки (все кластеры)",   callback_data="super_supply_confirm")],
        [InlineKeyboardButton(text="◀️ Главное меню",                   callback_data="main_menu")],
    ])
    await call.message.edit_text(
        "➕ Создание заявок\n\nВыбери режим:",
        reply_markup=kb
    )


# ===== СУПЕРПОСТАВКИ: ТЕСТ (Воронеж) =====
@dp.callback_query(F.data == "super_supply_test")
async def handle_super_supply_test(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Запускаю тест: создаю заявку в кластер Воронеж...")
    try:
        result = await run_super_supply_test()
    except Exception as e:
        logging.exception("super_supply_test error")
        result = f"❌ Ошибка: {e}"

    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="create_orders_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])
    chunks = [result[i:i + 4000] for i in range(0, len(result), 4000)]
    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        await call.message.answer(chunk, reply_markup=back_kb if i == len(chunks) - 1 else None)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Результат выше", reply_markup=back_kb)


# ===== СУПЕРПОСТАВКИ: ПОДТВЕРЖДЕНИЕ (все кластеры) =====
@dp.callback_query(F.data == "super_supply_confirm")
async def handle_super_supply_confirm(call: CallbackQuery):
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, создать заявки", callback_data="super_supply_run")],
        [InlineKeyboardButton(text="❌ Отмена",             callback_data="create_orders_menu")],
    ])
    await call.message.edit_text(
        "🚀 Суперпоставки — все кластеры\n\n"
        "Будут созданы заявки во все кластеры OZON кроме:\n"
        "Алматы, Астана, Калининград, Беларусь, Армения, Казахстан, Кыргызстан\n\n"
        "Параметры:\n"
        "• Товар: первый Super-товар\n"
        "• Кол-во: 5 шт\n"
        "• Точка отгрузки: СТАВРОПОЛЬ_АППЗ_2\n"
        "• Дата: +14..+19 дней, слот 19:00-20:00 МСК\n\n"
        "Продолжить?",
        reply_markup=kb
    )


# ===== СУПЕРПОСТАВКИ: ЗАПУСК =====
@dp.callback_query(F.data == "super_supply_run")
async def handle_super_supply_run(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Создаю заявки во все кластеры...\nЭто может занять несколько минут.")
    try:
        result = await run_super_supply_all()
    except Exception as e:
        logging.exception("super_supply_run error")
        result = f"❌ Ошибка: {e}"

    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="create_orders_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])
    chunks = [result[i:i + 4000] for i in range(0, len(result), 4000)]
    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        await call.message.answer(chunk, reply_markup=back_kb if i == len(chunks) - 1 else None)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Результат выше", reply_markup=back_kb)


# ===== ЗАПУСК =====
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

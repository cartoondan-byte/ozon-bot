"""
ТЕСТ: Создание одной заявки на поставку в кластер Воронеж
==========================================================
Workflow:
  1. /v1/cluster/list           → найти cluster_id Воронежа
  2. /v1/warehouse/fbo/list     → найти warehouse_id точки СТАВРОПОЛЬ_АППЗ_2
  3. /v1/draft/create           → создать черновик (1 SKU, 5 шт)
  4. /v1/draft/create/info      → polling до CALCULATION_STATUS_SUCCESS → draft_id + warehouse_id
  5. /v1/draft/timeslot/info    → получить доступные слоты (+14..+19 дней)
  6. /v1/draft/supply/create    → создать заявку (слот 19-20 МСК или 18-19 МСК)
  7. /v1/draft/supply/create/status → polling до Success/Failed

Запуск:
  OZON_CLIENT_ID=xxx OZON_API_KEY=yyy SUPER_SKU=123456 python test_create_voronezh.py
"""

import asyncio
import json
import logging
import os
import random
from datetime import datetime, timedelta

import aiohttp
import pytz

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = os.environ["OZON_CLIENT_ID"]
OZON_API_KEY   = os.environ["OZON_API_KEY"]
SUPER_SKU      = int(os.environ["SUPER_SKU"])   # SKU одного Super-товара для теста
OZON_API_URL   = "https://api-seller.ozon.ru"
MOSCOW_TZ      = pytz.timezone("Europe/Moscow")

STAVROPOL_WAREHOUSE_NAME = "СТАВРОПОЛЬ_АППЗ_2"  # точка отгрузки
TARGET_CLUSTER_NAME      = "Воронеж"             # тестовый кластер

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

def ozon_headers() -> dict:
    return {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }


async def ozon_post(session: aiohttp.ClientSession, url: str, payload: dict, retries: int = 5) -> dict:
    """POST с retry на 429."""
    for attempt in range(retries):
        async with session.post(url, headers=ozon_headers(), json=payload) as resp:
            raw = await resp.text()
            if resp.status == 429:
                wait = 1.5 * (attempt + 1)
                log.warning(f"429 rate limit → жду {wait}с (attempt {attempt + 1})")
                await asyncio.sleep(wait)
                continue
            log.info(f"POST {url} → HTTP {resp.status} | {raw[:300]}")
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status} [{url}]: {raw[:300]}")
            return json.loads(raw)
    raise Exception(f"Превышен лимит запросов к {url}")


async def poll(session: aiohttp.ClientSession, url: str, payload: dict,
               success_statuses: list, fail_statuses: list,
               status_field: str = "status",
               interval: float = 3.0, max_attempts: int = 20) -> dict:
    """Polling до получения финального статуса."""
    for attempt in range(max_attempts):
        data   = await ozon_post(session, url, payload)
        status = data.get(status_field, "")
        log.info(f"polling {url} → status={status} (attempt {attempt + 1})")
        if status in success_statuses:
            return data
        if status in fail_statuses:
            raise Exception(f"Операция завершилась с ошибкой: {status} | {data}")
        await asyncio.sleep(interval)
    raise Exception(f"Polling {url} превысил {max_attempts} попыток")


# ===== ШАГ 1: Найти cluster_id Воронежа =====

async def find_voronezh_cluster(session: aiohttp.ClientSession) -> tuple[str, list[int]]:
    """
    Возвращает (cluster_id, [warehouse_id, ...]) для кластера Воронеж.
    cluster_id — строка (macrolocal_cluster_id из /v1/cluster/list).
    """
    data = await ozon_post(session, f"{OZON_API_URL}/v1/cluster/list",
                           {"cluster_type": "CLUSTER_TYPE_OZON"})
    clusters = data.get("clusters", [])
    log.info(f"Всего кластеров OZON: {len(clusters)}")

    for c in clusters:
        name = c.get("name", "")
        log.info(f"  Кластер: id={c.get('id')} name={name}")
        if TARGET_CLUSTER_NAME.lower() in name.lower():
            cluster_id  = str(c.get("id"))
            wh_ids = []
            for lc in c.get("logistic_clusters", []):
                for wh in lc.get("warehouses", []):
                    wh_ids.append(wh.get("warehouse_id"))
            log.info(f"✅ Найден кластер: id={cluster_id} name={name} warehouses={wh_ids}")
            return cluster_id, wh_ids

    raise Exception(f"Кластер '{TARGET_CLUSTER_NAME}' не найден!")


# ===== ШАГ 2: Найти warehouse_id точки отгрузки СТАВРОПОЛЬ_АППЗ_2 =====

async def find_stavropol_warehouse(session: aiohttp.ClientSession) -> int:
    """
    Ищет точку отгрузки через /v1/warehouse/fbo/list.
    Пробует оба типа поставки.
    """
    for supply_type in ["CREATE_TYPE_CROSSDOCK", "CREATE_TYPE_DIRECT"]:
        try:
            data = await ozon_post(
                session,
                f"{OZON_API_URL}/v1/warehouse/fbo/list",
                {
                    "filter_by_supply_type": [supply_type],
                    "search": STAVROPOL_WAREHOUSE_NAME
                }
            )
            results = data.get("search", [])
            log.info(f"warehouse/fbo/list ({supply_type}) → {len(results)} результатов")
            for wh in results:
                log.info(f"  warehouse: id={wh.get('warehouse_id')} name={wh.get('name')}")
                if STAVROPOL_WAREHOUSE_NAME.lower() in wh.get("name", "").lower():
                    wh_id = wh["warehouse_id"]
                    log.info(f"✅ Найдена точка отгрузки: id={wh_id} name={wh['name']} type={supply_type}")
                    return wh_id, supply_type
        except Exception as e:
            log.warning(f"warehouse/fbo/list ({supply_type}): {e}")

    # Если не нашли точное совпадение — берём первый результат из DIRECT
    data = await ozon_post(
        session,
        f"{OZON_API_URL}/v1/warehouse/fbo/list",
        {"filter_by_supply_type": ["CREATE_TYPE_DIRECT"], "search": "СТАВРОПОЛЬ"}
    )
    results = data.get("search", [])
    if results:
        wh = results[0]
        log.warning(f"⚠️ Точное совпадение не найдено, берём первый результат: {wh}")
        return wh["warehouse_id"], "CREATE_TYPE_DIRECT"

    raise Exception(f"Точка отгрузки '{STAVROPOL_WAREHOUSE_NAME}' не найдена!")


# ===== ШАГ 3-4: Создать черновик и получить draft_id =====

async def create_draft(session: aiohttp.ClientSession,
                       cluster_id: str,
                       drop_off_warehouse_id: int,
                       supply_type: str,
                       sku: int,
                       quantity: int) -> tuple[int, list[dict]]:
    """
    Создаёт черновик заявки.
    Возвращает (draft_id, clusters_info) из /v1/draft/create/info.
    """
    payload = {
        "cluster_ids": [cluster_id],
        "items":       [{"sku": sku, "quantity": quantity}],
        "type":        supply_type,
    }
    if supply_type == "CREATE_TYPE_CROSSDOCK":
        payload["drop_off_point_warehouse_id"] = drop_off_warehouse_id

    resp = await ozon_post(session, f"{OZON_API_URL}/v1/draft/create", payload)
    operation_id = resp.get("operation_id")
    if not operation_id:
        raise Exception(f"draft/create не вернул operation_id: {resp}")
    log.info(f"draft/create → operation_id={operation_id}")

    # Polling /v1/draft/create/info
    info = await poll(
        session,
        f"{OZON_API_URL}/v1/draft/create/info",
        {"operation_id": operation_id},
        success_statuses=["CALCULATION_STATUS_SUCCESS"],
        fail_statuses=["CALCULATION_STATUS_FAILED", "CALCULATION_STATUS_EXPIRED"],
        interval=3.0,
        max_attempts=20
    )

    draft_id = info.get("draft_id")
    clusters = info.get("clusters", [])
    errors   = info.get("errors", [])

    if errors:
        log.warning(f"draft/create/info errors: {errors}")
    if not draft_id:
        raise Exception(f"draft/create/info не вернул draft_id: {info}")

    log.info(f"✅ draft_id={draft_id}, кластеров в ответе: {len(clusters)}")
    return draft_id, clusters


# ===== ШАГ 5: Получить доступные слоты =====

async def find_timeslot(session: aiohttp.ClientSession,
                        draft_id: int,
                        drop_off_warehouse_id: int,
                        day_offset_min: int = 14,
                        day_offset_max: int = 19) -> tuple[str, str]:
    """
    Ищет доступный слот 19:00-20:00 МСК (16:00-17:00 UTC) или 18:00-19:00 МСК (15:00-16:00 UTC)
    в диапазоне +14..+19 дней от сегодня.
    Если нужный слот недоступен в выбранный день — пробует другой день в диапазоне.
    Возвращает (from_in_timezone, to_in_timezone) в ISO формате.
    """
    today    = datetime.now(MOSCOW_TZ).date()
    date_from = today + timedelta(days=day_offset_min)
    date_to   = today + timedelta(days=day_offset_max)

    data = await ozon_post(
        session,
        f"{OZON_API_URL}/v1/draft/timeslot/info",
        {
            "draft_id":      draft_id,
            "date_from":     date_from.strftime("%Y-%m-%dT00:00:00Z"),
            "date_to":       date_to.strftime("%Y-%m-%dT23:59:59Z"),
            "warehouse_ids": [str(drop_off_warehouse_id)],
        }
    )

    wh_slots = data.get("drop_off_warehouse_timeslots", [])
    log.info(f"timeslot/info → {len(wh_slots)} складов с слотами")

    # Предпочтительные часы МСК (UTC = МСК - 3)
    preferred_slots = [
        ("19:00", "20:00", "16:00", "17:00"),   # 19-20 МСК = 16-17 UTC
        ("18:00", "19:00", "15:00", "16:00"),   # 18-19 МСК = 15-16 UTC
    ]

    # Собираем все доступные дни и их слоты
    days_pool = []
    for wh in wh_slots:
        tz = wh.get("warehouse_timezone", "Europe/Moscow")
        for day in wh.get("days", []):
            date_str = day.get("date_in_timezone", "")[:10]
            for ts in day.get("timeslots", []):
                ts_from = ts.get("from_in_timezone", "")
                ts_to   = ts.get("to_in_timezone", "")
                days_pool.append((date_str, ts_from, ts_to))
                log.info(f"  доступный слот: {date_str} {ts_from} – {ts_to}")

    if not days_pool:
        raise Exception("Нет доступных слотов в указанном диапазоне дат!")

    # Выбираем случайный день из диапазона и ищем предпочтительный слот
    available_dates = sorted(set(d[0] for d in days_pool))
    random.shuffle(available_dates)  # случайный порядок дат

    for target_date in available_dates:
        slots_on_date = [(f, t) for d, f, t in days_pool if d == target_date]
        for (msk_from, msk_to, _, _) in preferred_slots:
            for (ts_from, ts_to) in slots_on_date:
                if msk_from in ts_from or msk_from in ts_to:
                    log.info(f"✅ Выбран слот: {target_date} {ts_from} – {ts_to} (МСК {msk_from}-{msk_to})")
                    return ts_from, ts_to

        # Если предпочтительных слотов нет — берём любой на эту дату
        if slots_on_date:
            ts_from, ts_to = slots_on_date[0]
            log.warning(f"⚠️ Предпочтительные слоты недоступны на {target_date}, берём: {ts_from} – {ts_to}")
            return ts_from, ts_to

    raise Exception("Не удалось найти подходящий слот!")


# ===== ШАГ 6-7: Создать заявку из черновика =====

async def create_supply_from_draft(session: aiohttp.ClientSession,
                                   draft_id: int,
                                   placement_warehouse_id: int,
                                   ts_from: str,
                                   ts_to: str) -> list[str]:
    """
    Создаёт заявку из черновика.
    Возвращает список order_ids.
    """
    resp = await ozon_post(
        session,
        f"{OZON_API_URL}/v1/draft/supply/create",
        {
            "draft_id":     draft_id,
            "warehouse_id": placement_warehouse_id,
            "timeslot": {
                "from_in_timezone": ts_from,
                "to_in_timezone":   ts_to,
            }
        }
    )
    operation_id = resp.get("operation_id")
    if not operation_id:
        raise Exception(f"draft/supply/create не вернул operation_id: {resp}")
    log.info(f"draft/supply/create → operation_id={operation_id}")

    # Polling статуса
    status_data = await poll(
        session,
        f"{OZON_API_URL}/v1/draft/supply/create/status",
        {"operation_id": operation_id},
        success_statuses=["DraftSupplyCreateStatusSuccess"],
        fail_statuses=["DraftSupplyCreateStatusFailed"],
        interval=3.0,
        max_attempts=20
    )

    order_ids = status_data.get("result", {}).get("order_ids", [])
    errors    = status_data.get("error_messages", [])

    if errors:
        raise Exception(f"draft/supply/create/status errors: {errors}")
    if not order_ids:
        raise Exception(f"Заявки не созданы, ответ: {status_data}")

    log.info(f"✅ Созданы заявки: {order_ids}")
    return order_ids


# ===== ГЛАВНАЯ ФУНКЦИЯ ТЕСТА =====

async def test_create_voronezh():
    log.info("=" * 60)
    log.info(f"ТЕСТ: Создание заявки в кластер '{TARGET_CLUSTER_NAME}'")
    log.info(f"SKU: {SUPER_SKU} | Количество: 5 шт")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        # Шаг 1: найти кластер Воронеж
        log.info("\n--- ШАГ 1: Поиск кластера Воронеж ---")
        cluster_id, cluster_wh_ids = await find_voronezh_cluster(session)

        # Шаг 2: найти точку отгрузки
        log.info("\n--- ШАГ 2: Поиск точки отгрузки ---")
        drop_off_wh_id, supply_type = await find_stavropol_warehouse(session)

        # Шаг 3-4: создать черновик
        log.info(f"\n--- ШАГ 3-4: Создание черновика (тип={supply_type}) ---")
        draft_id, clusters_info = await create_draft(
            session,
            cluster_id=cluster_id,
            drop_off_warehouse_id=drop_off_wh_id,
            supply_type=supply_type,
            sku=SUPER_SKU,
            quantity=5
        )

        # Из clusters_info получаем placement warehouse_id
        # (склад размещения — куда едет товар, может отличаться от drop_off)
        placement_wh_id = None
        for cl in clusters_info:
            for wh in cl.get("warehouses", []):
                if wh.get("status", {}).get("is_available"):
                    placement_wh_id = wh.get("supply_warehouse", {}).get("warehouse_id")
                    log.info(f"  Склад размещения: {wh.get('supply_warehouse', {}).get('name')} id={placement_wh_id}")
                    break
            if placement_wh_id:
                break

        if not placement_wh_id:
            # Fallback: берём первый склад из кластера
            for cl in clusters_info:
                for wh in cl.get("warehouses", []):
                    placement_wh_id = wh.get("supply_warehouse", {}).get("warehouse_id")
                    log.warning(f"⚠️ Нет доступного склада, берём первый: id={placement_wh_id}")
                    break
                if placement_wh_id:
                    break

        if not placement_wh_id:
            raise Exception("Не найден склад размещения в ответе draft/create/info!")

        # Шаг 5: получить слот
        log.info("\n--- ШАГ 5: Поиск таймслота ---")
        # Для timeslot/info используем drop_off warehouse (точку отгрузки)
        ts_from, ts_to = await find_timeslot(
            session,
            draft_id=draft_id,
            drop_off_warehouse_id=drop_off_wh_id,
            day_offset_min=14,
            day_offset_max=19
        )

        # Шаг 6-7: создать заявку
        log.info("\n--- ШАГ 6-7: Создание заявки из черновика ---")
        order_ids = await create_supply_from_draft(
            session,
            draft_id=draft_id,
            placement_warehouse_id=placement_wh_id,
            ts_from=ts_from,
            ts_to=ts_to
        )

    # Итог
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЁН УСПЕШНО")
    print(f"Кластер:       {TARGET_CLUSTER_NAME} (id={cluster_id})")
    print(f"SKU:           {SUPER_SKU}")
    print(f"Количество:    5 шт")
    print(f"Точка отгрузки: {STAVROPOL_WAREHOUSE_NAME} (id={drop_off_wh_id})")
    print(f"Тип поставки:  {supply_type}")
    print(f"Слот:          {ts_from} – {ts_to}")
    print(f"Заявки (order_ids): {order_ids}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_create_voronezh())

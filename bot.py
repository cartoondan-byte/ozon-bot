import asyncio
import logging
import json
from datetime import datetime, timedelta
import pytz
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = "1360213"
OZON_API_KEY = "dd0e57bc-1497-4e70-a642-63266dbddcc7"
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

# ===== OZON API =====

async def ozon_post(session, url, payload, retry=3):
    for attempt in range(retry):
        await asyncio.sleep(2)
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


async def get_all_active_orders(session):
    """Получить все активные заявки (DATA_FILLING + READY_TO_SUPPLY и др.)"""
    data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
        "limit": 50,
        "from_supply_order_id": 0,
        "sort_by": 1,
        "sort_direction": 1,
        "filter": {"states": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
    })
    order_ids = data.get("order_ids", [])
    if not order_ids:
        return []

    details = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
        "order_ids": order_ids
    })
    all_orders = details.get("orders", [])

    # Активные = исключаем CANCELLED и COMPLETED
    skip = {"CANCELLED", "COMPLETED", "REJECTED"}
    active = [o for o in all_orders if o.get("state", "") not in skip]
    logger.info(f"Всего: {len(all_orders)}, активных: {len(active)}")
    return active


async def get_data_filling_orders(session):
    """Только DATA_FILLING для переноса"""
    orders = await get_all_active_orders(session)
    return [o for o in orders if o.get("state") == "DATA_FILLING"]


async def get_cluster_names(session):
    """Получить названия кластеров"""
    try:
        data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/cluster/list", {})
        logger.info(f"Cluster list: {json.dumps(data)[:500]}")
        # Строим словарь id -> name
        clusters = {}
        for c in data.get("clusters", data.get("result", [])):
            cid = str(c.get("id") or c.get("cluster_id") or "")
            name = c.get("name") or c.get("cluster_name") or cid
            if cid:
                clusters[cid] = name
        return clusters
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
        logger.info(f"Bundle FULL RESPONSE: {json.dumps(data)}")
        return data
    except Exception as e:
        logger.warning(f"Bundle error: {e}")
        return {}


async def update_timeslot(session, supply_order_id, time_from, time_to):
    return await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/update", {
        "supply_order_id": supply_order_id,
        "timeslot": {"from": time_from, "to": time_to}
    })


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
    async with aiohttp.ClientSession() as session:
        orders = await get_data_filling_orders(session)
        if not orders:
            return "📭 Нет заявок со статусом «Заполнение данных»."

        for order in orders:
            oid = order.get("order_id")
            onum = order.get("order_number", str(oid))
            try:
                current_date = get_current_order_date(order)
                target_date = (current_date + timedelta(days=1)) if current_date else \
                              (datetime.now(MOSCOW_TZ).date() + timedelta(days=1))

                time_from = f"{target_date.strftime('%Y-%m-%d')}T16:00:00Z"
                time_to   = f"{target_date.strftime('%Y-%m-%d')}T17:00:00Z"

                result = await update_timeslot(session, oid, time_from, time_to)
                if not result.get("errors"):
                    cd = current_date.strftime('%d.%m') if current_date else '?'
                    results.append(f"✅ {onum}: {cd} → {target_date.strftime('%d.%m.%Y')} 19:00–20:00")
                else:
                    errors.append(f"❌ {onum}: {result.get('errors')}")
            except Exception as e:
                logger.exception(f"Ошибка для {onum}")
                errors.append(f"❌ {onum}: {str(e)}")

    lines = [f"📦 Заявок для переноса: {len(orders)}\n"]
    if results:
        lines.append("Успешно перенесено:")
        lines.extend(results)
    if errors:
        if results: lines.append("")
        lines.append("Проблемы:")
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
        supplies = order.get("supplies", [])
        cluster_id = None
        if supplies:
            cluster_id = supplies[0].get("macrolocal_cluster_id") or "Без кластера"
        else:
            cluster_id = "Без кластера"

        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(order)

    # Сохраняем в кэш как список для индексации
    cluster_list = []
    for cid, corders in clusters.items():
        cluster_list.append({"id": cid, "orders": corders, "names": cluster_names})

    cluster_cache[user_id] = cluster_list
    return cluster_list


async def get_cluster_details(user_id: int, cluster_idx: int) -> str:
    """Получить детали заявок по кластеру с SKU"""
    cluster_list = cluster_cache.get(user_id)
    if not cluster_list or cluster_idx >= len(cluster_list):
        return "❗ Данные устарели, нажми кнопку снова."

    cluster = cluster_list[cluster_idx]
    cluster_id = cluster["id"]
    orders = cluster["orders"]
    names = cluster.get("names", {})
    cluster_display = names.get(str(cluster_id), f"Кластер {cluster_id}")

    lines = [f"📍 {cluster_display}\n"]

    async with aiohttp.ClientSession() as session:
        for order in orders:
            onum = order.get("order_number", "?")
            state = order.get("state", "?")
            state_name = STATE_NAMES.get(state, state)

            # Дата отгрузки
            try:
                from_str = order["timeslot"]["timeslot"]["from"]
                dt = datetime.fromisoformat(from_str.replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
                date_str = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                date_str = "неизвестно"

            lines.append(f"📋 Заявка {onum} | {state_name}")
            lines.append(f"   📅 Дата отгрузки: {date_str}")

            # Получаем bundle для SKU
            supplies = order.get("supplies", [])
            if supplies:
                bundle_id = supplies[0].get("bundle_id")
                if bundle_id:
                    bundle_data = await get_bundle_items(session, bundle_id)
                    # Ищем items во всех возможных местах ответа
                    items = (bundle_data.get("items") or
                             bundle_data.get("products") or
                             bundle_data.get("bundle_items") or
                             bundle_data.get("result", {}).get("items") if isinstance(bundle_data.get("result"), dict) else None or
                             [])
                    # Если result это список
                    if not items and isinstance(bundle_data.get("result"), list):
                        items = bundle_data["result"]

                    if items:
                        for item in items:
                            name = (item.get("name") or item.get("product_name") or
                                    item.get("title") or "—")
                            sku  = (item.get("sku") or item.get("product_id") or
                                    item.get("offer_id") or "—")
                            qty  = (item.get("quantity") or item.get("qty") or
                                    item.get("count") or "—")
                            lines.append(f"   🏷 {name}")
                            lines.append(f"      SKU: {sku} | Кол-во: {qty}")
                    else:
                        lines.append(f"   ℹ️ Ключи ответа: {list(bundle_data.keys())}")
            lines.append("")

    return "\n".join(lines)


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
    await callback.message.edit_text("⏳ Загружаю данные кластера...")
    try:
        text = await get_cluster_details(callback.from_user.id, idx)
        # Telegram лимит 4096 символов
        if len(text) > 4000:
            text = text[:4000] + "\n...(обрезано)"
        await callback.message.edit_text(text, reply_markup=back_keyboard())
    except Exception as e:
        logger.exception("cluster detail error")
        await callback.message.edit_text(f"❗ Ошибка: {str(e)}", reply_markup=back_keyboard())


async def main():
    logger.info("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

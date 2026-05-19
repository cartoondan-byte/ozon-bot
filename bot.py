import asyncio
import logging
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

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()

# ===== МАППИНГ КЛАСТЕРОВ (macrolocal_cluster_id → название) =====
# Берём из официальной документации Ozon / личного кабинета
CLUSTER_NAMES = {
    "4071": "Ростов-на-Дону",
    "4072": "Краснодар",
    "4073": "Ставрополь",
    "4074": "Воронеж",
    "4075": "Волгоград",
    "4076": "Саратов",
    "4077": "Самара",
    "4078": "Казань",
    "4079": "Нижний Новгород",
    "4080": "Уфа",
    "4081": "Пермь",
    "4082": "Екатеринбург",
    "4083": "Тюмень",
    "4084": "Новосибирск",
    "4085": "Омск",
    "4086": "Красноярск",
    "4087": "Иркутск",
    "4088": "Хабаровск",
    "4089": "Владивосток",
    "4090": "Москва",
    "4091": "Санкт-Петербург",
    "4092": "Челябинск",
    "4093": "Белгород",
    "4094": "Липецк",
    "4095": "Тула",
    "4096": "Рязань",
    "4097": "Пенза",
    "4098": "Тольятти",
    "4099": "Оренбург",
    "4100": "Ижевск",
    "4101": "Киров",
    "4102": "Чебоксары",
    "4103": "Ярославль",
    "4104": "Иваново",
    "4105": "Тверь",
    "4106": "Смоленск",
    "4107": "Брянск",
    "4108": "Орёл",
    "4109": "Курск",
    "4110": "Тамбов",
    "4111": "Астрахань",
    "4112": "Махачкала",
    "4113": "Владикавказ",
    "4114": "Нальчик",
    "4115": "Грозный",
    "4116": "Черкесск",
    "4117": "Майкоп",
    "4118": "Сочи",
    "4119": "Новороссийск",
    "4120": "Барнаул",
    "4121": "Кемерово",
    "4122": "Томск",
    "4123": "Улан-Удэ",
    "4124": "Чита",
    "4125": "Якутск",
    "4126": "Благовещенск",
    "4127": "Южно-Сахалинск",
}

def cluster_name(cluster_id: str) -> str:
    """Возвращает название кластера или 'Кластер {id}' если не найден."""
    return CLUSTER_NAMES.get(str(cluster_id), f"Кластер {cluster_id}")


# ===== КЭШ ДАННЫХ (чтобы не перезапрашивать при навигации) =====
_cache: dict = {}


# ===== ЗАГОЛОВКИ =====
def ozon_headers() -> dict:
    return {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }


# ===== POST-запрос с retry на 429 =====
async def ozon_post(session: aiohttp.ClientSession, url: str, payload: dict, retries: int = 3) -> dict:
    for attempt in range(retries):
        async with session.post(url, headers=ozon_headers(), json=payload) as resp:
            raw = await resp.text()
            if resp.status == 429:
                wait = 1.0 * (attempt + 1)
                logging.warning(f"429 rate limit, жду {wait}с...")
                await asyncio.sleep(wait)
                continue
            logging.info(f"POST {url} → {resp.status} | {raw[:200]}")
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status} [{url}]: {raw[:200]}")
            return json.loads(raw)
    raise Exception(f"Превышен лимит запросов к {url}")


# ===== ПОЛУЧЕНИЕ ВСЕХ SKU =====
async def fetch_all_skus() -> list[dict]:
    skus    = []
    last_id = ""
    limit   = 1000

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": limit}
            data    = await ozon_post(session, f"{OZON_API_URL}/v3/product/list", payload)
            items   = data.get("result", {}).get("items", [])
            last_id = data.get("result", {}).get("last_id", "")
            if not items:
                break
            skus.extend(items)
            if len(items) < limit:
                break

    return skus


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


# ===== ПОЛУЧЕНИЕ order_id — только DATA_FILLING =====
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


# ===== ТОВАРЫ ИЗ БАНДЛОВ (с задержкой чтобы не получить 429) =====
async def fetch_bundle_items(session: aiohttp.ClientSession, bundle_ids: list) -> dict:
    result = {}
    for bid in bundle_ids:
        try:
            data  = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle", {"bundle_ids": [bid], "last_id": "", "limit": 100})
            items = data.get("items", [])
            result[bid] = items
        except Exception as e:
            logging.warning(f"bundle {bid}: {e}")

    return result


# ===== КЛЮЧ НАЗНАЧЕНИЯ ЗАЯВКИ (склад или кластер) =====
def get_dest_key(order: dict) -> str:
    """Возвращает уникальный ключ назначения для группировки."""
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
    """Человекочитаемое название для кнопки."""
    if dest_key.startswith("wh::"):
        return dest_key[4:]
    if dest_key.startswith("cl::"):
        return cluster_name(dest_key[4:])
    return dest_key


# ===== ЗАГРУЗКА ВСЕХ ДАННЫХ И ГРУППИРОВКА =====
async def load_all_orders() -> dict:
    """Загружает все заявки DATA_FILLING, возвращает {dest_key: [order, ...]}"""
    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        logging.info(f"DATA_FILLING: {len(order_ids)} заявок")
        if not order_ids:
            return {}

        orders = await fetch_supply_order_details(session, order_ids)

        # Собираем bundle_ids
        bundle_ids = []
        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                if bid:
                    bundle_ids.append(bid)

        bundle_items = await fetch_bundle_items(session, bundle_ids)

        # Прикрепляем товары
        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                supply["_items"] = bundle_items.get(bid, []) if bid else []

    # Группируем по назначению
    grouped: dict = {}
    for order in orders:
        key = get_dest_key(order)
        grouped.setdefault(key, []).append(order)

    return grouped


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все SKU",            callback_data="show_skus")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку", callback_data="show_supplies")],
    ])


# ===== МЕНЮ СКЛАДОВ/КЛАСТЕРОВ =====
def dest_menu(grouped: dict) -> InlineKeyboardMarkup:
    buttons = []
    for dest_key, orders in sorted(grouped.items(), key=lambda x: get_dest_label(x[0])):
        label = f"{get_dest_label(dest_key)} ({len(orders)})"
        # callback_data ограничен 64 байтами — используем индекс
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"dest::{dest_key[:50]}"
        )])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="show_supplies")])
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


# ===== КНОПКА: ГЛАВНОЕ МЕНЮ =====
@dp.callback_query(F.data == "main_menu")
async def handle_main_menu(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("Выбери действие:", reply_markup=main_menu())


# ===== КНОПКА: ВСЕ SKU =====
@dp.callback_query(F.data == "show_skus")
async def handle_show_skus(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Загружаю список артикулов...")

    try:
        items = await fetch_all_skus()
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
        return

    if not items:
        await call.message.edit_text("📭 Артикулы не найдены.", reply_markup=main_menu())
        return

    lines = [f"{i}. {item.get('offer_id','—')} | product_id: {item.get('product_id','—')}"
             for i, item in enumerate(items, 1)]
    text_full = "\n".join(lines)
    chunks    = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]

    await call.message.edit_text(f"📦 Найдено артикулов: {len(items)}\n\n" + chunks[0])
    for chunk in chunks[1:]:
        await call.message.answer(chunk)
    await call.message.answer("Готово!", reply_markup=main_menu())


# ===== КНОПКА: ЗАЯВКИ — ПОКАЗАТЬ СКЛАДЫ/КЛАСТЕРЫ =====
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


# ===== КНОПКА: ВЫБРАН СКЛАД/КЛАСТЕР =====
@dp.callback_query(F.data.startswith("dest::"))
async def handle_dest_select(call: CallbackQuery):
    await call.answer()
    dest_key = call.data[6:]  # убираем "dest::"

    grouped  = _cache.get("grouped", {})

    # Ищем ключ (может быть обрезан до 50 символов)
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

    # Собираем product_id для названий
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

    # Кнопка назад
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад к складам", callback_data="back_to_dests")],
        [InlineKeyboardButton(text="🏠 Главное меню",    callback_data="main_menu")],
    ])

    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        kb = back_kb if i == len(chunks) - 1 else None
        await call.message.answer(chunk, reply_markup=kb)

    if len(chunks) == 1:
        pass  # кнопка уже добавлена выше
    else:
        await call.message.answer("⬆️ Список выше", reply_markup=back_kb)


# ===== КНОПКА: НАЗАД К СКЛАДАМ =====
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


# ===== ЗАПУСК =====
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

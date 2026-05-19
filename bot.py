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


# ===== ЗАГОЛОВКИ =====
def ozon_headers() -> dict:
    return {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }


# ===== ВСПОМОГАТЕЛЬНАЯ: POST-запрос с логированием =====
async def ozon_post(session: aiohttp.ClientSession, url: str, payload: dict) -> dict:
    async with session.post(url, headers=ozon_headers(), json=payload) as resp:
        raw = await resp.text()
        logging.info(f"POST {url} → {resp.status} | {raw[:400]}")
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status} [{url}]: {raw[:300]}")
        return json.loads(raw)


# ===== ПОЛУЧЕНИЕ ВСЕХ SKU =====
async def fetch_all_skus() -> list[dict]:
    skus    = []
    last_id = ""
    limit   = 1000

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {
                "filter":  {"visibility": "ALL"},
                "last_id": last_id,
                "limit":   limit,
            }
            data    = await ozon_post(session, f"{OZON_API_URL}/v3/product/list", payload)
            items   = data.get("result", {}).get("items", [])
            last_id = data.get("result", {}).get("last_id", "")

            if not items:
                break
            skus.extend(items)
            if len(items) < limit:
                break

    return skus


# ===== ПОЛУЧЕНИЕ НАЗВАНИЙ ТОВАРОВ =====
async def fetch_product_names(product_ids: list[int]) -> dict[int, str]:
    result     = {}
    chunk_size = 1000

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            try:
                data = await ozon_post(
                    session,
                    f"{OZON_API_URL}/v2/product/info/list",
                    {"product_id": chunk}
                )
                for item in data.get("result", {}).get("items", []):
                    pid = item.get("id")
                    if pid:
                        result[pid] = item.get("name", "—")
            except Exception as e:
                logging.warning(f"Не удалось получить названия товаров: {e}")

    return result


# ===== ШАГ 1: получить список order_id через /v3/supply-order/list =====
async def fetch_supply_order_ids(session: aiohttp.ClientSession) -> list:
    order_ids = []
    last_id   = ""   # строка, как требует API
    limit     = 50

    while True:
        payload = {"limit": limit}
        if last_id:
            payload["last_id"] = last_id

        try:
            data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", payload)
        except Exception as e:
            logging.warning(f"supply-order/list ошибка: {e}")
            break

        # Поддержка разных форматов ответа
        batch = data.get("order_ids", data.get("orders", []))

        if not batch:
            break

        # Список int или список dict
        if batch and isinstance(batch[0], dict):
            ids = [o.get("order_id") for o in batch if o.get("order_id")]
        else:
            ids = [oid for oid in batch if oid]

        order_ids.extend(ids)

        new_last_id = str(data.get("last_id", ""))
        if not new_last_id or new_last_id == last_id or len(ids) < limit:
            break
        last_id = new_last_id

    return order_ids


# ===== ШАГ 2: получить детали заявок через /v3/supply-order/get =====
async def fetch_supply_order_details(
    session: aiohttp.ClientSession,
    order_ids: list
) -> list[dict]:
    orders     = []
    chunk_size = 50

    for i in range(0, len(order_ids), chunk_size):
        chunk = order_ids[i:i + chunk_size]
        try:
            data = await ozon_post(
                session,
                f"{OZON_API_URL}/v3/supply-order/get",
                {"order_ids": chunk}
            )
            orders.extend(data.get("orders", []))
        except Exception as e:
            logging.warning(f"supply-order/get chunk {chunk}: {e}")

    return orders


# ===== ФИЛЬТР: заявки на ближайшие 5 дней =====
def filter_orders_by_date(orders: list[dict]) -> list[dict]:
    now      = datetime.now(MOSCOW_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    date_to  = now + timedelta(days=5)
    filtered = []

    for order in orders:
        supply_date_str = order.get("supply_date", "")
        if not supply_date_str:
            supplies = order.get("supplies", [])
            if supplies:
                supply_date_str = supplies[0].get("arrival_date", "")

        if supply_date_str:
            try:
                dt = datetime.strptime(supply_date_str[:10], "%Y-%m-%d")
                dt = MOSCOW_TZ.localize(dt)
                if now <= dt <= date_to:
                    filtered.append(order)
            except Exception:
                pass
        else:
            filtered.append(order)

    return filtered


# ===== ОСНОВНАЯ ФУНКЦИЯ ПОСТАВОК =====
async def fetch_supply_requests() -> list[dict]:
    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        logging.info(f"Всего order_id получено: {len(order_ids)}")

        if not order_ids:
            return []

        orders = await fetch_supply_order_details(session, order_ids)
        return filter_orders_by_date(orders)


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все SKU",            callback_data="show_skus")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку", callback_data="show_supplies")],
    ])


# ===== СТАРТ =====
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer("Привет! Выбери действие:", reply_markup=main_menu())


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

    lines = []
    for i, item in enumerate(items, 1):
        offer_id   = item.get("offer_id", "—")
        product_id = item.get("product_id", "—")
        lines.append(f"{i}. Артикул: {offer_id} | product_id: {product_id}")

    text_full = "\n".join(lines)
    chunks    = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]

    await call.message.edit_text(f"📦 Найдено артикулов: {len(items)}\n\n" + chunks[0])
    for chunk in chunks[1:]:
        await call.message.answer(chunk)

    await call.message.answer("Готово!", reply_markup=main_menu())


# ===== КНОПКА: ЗАЯВКИ НА ПОСТАВКУ =====
@dp.callback_query(F.data == "show_supplies")
async def handle_show_supplies(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Загружаю заявки на поставку...")

    try:
        orders = await fetch_supply_requests()
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
        return

    if not orders:
        await call.message.edit_text(
            "📭 Заявок на ближайшие 5 дней не найдено.",
            reply_markup=main_menu()
        )
        return

    # Собираем product_id для названий
    all_product_ids = []
    for order in orders:
        for supply in order.get("supplies", []):
            for item in supply.get("items", []):
                pid = item.get("product_id")
                if pid:
                    all_product_ids.append(pid)

    try:
        names = await fetch_product_names(list(set(all_product_ids)))
    except Exception:
        names = {}

    # Формируем текст
    lines = [f"🚚 Заявок на ближайшие 5 дней: {len(orders)}\n"]

    for order in orders:
        order_id = order.get("order_id", "—")
        status   = order.get("state", order.get("status", "—"))

        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"📋 Заявка #{order_id}")
        lines.append(f"🔖 Статус: {status}")

        for supply in order.get("supplies", []):
            arrival   = supply.get("arrival_date", "")[:10]
            warehouse = supply.get("warehouse_name", supply.get("warehouse_id", "—"))
            lines.append(f"📅 Дата: {arrival} | Склад: {warehouse}")
            lines.append("Товары:")

            for item in supply.get("items", []):
                sku        = item.get("sku", item.get("product_id", "—"))
                product_id = item.get("product_id")
                qty        = item.get("quantity", "—")
                name       = names.get(product_id, "—") if product_id else "—"
                lines.append(f"  • SKU: {sku} | {name} | кол-во: {qty}")

        lines.append("")

    text_full = "\n".join(lines)
    chunks    = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]

    await call.message.edit_text(chunks[0])
    for chunk in chunks[1:]:
        await call.message.answer(chunk)

    await call.message.answer("Готово!", reply_markup=main_menu())


# ===== ЗАПУСК =====
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

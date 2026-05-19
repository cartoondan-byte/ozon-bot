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


# ===== ПОЛУЧЕНИЕ SKU С OZON =====
async def fetch_all_skus() -> list[dict]:
    skus    = []
    last_id = ""
    limit   = 1000

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {
                "filter": {"visibility": "ALL"},
                "last_id": last_id,
                "limit":   limit,
            }
            async with session.post(
                f"{OZON_API_URL}/v3/product/list",
                headers=ozon_headers(),
                json=payload,
            ) as resp:
                raw = await resp.text()
                logging.info(f"Ozon SKU status: {resp.status} | body: {raw[:500]}")
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}: {raw[:300]}")
                data = json.loads(raw)

            items   = data.get("result", {}).get("items", [])
            last_id = data.get("result", {}).get("last_id", "")

            if not items:
                break
            skus.extend(items)
            if len(items) < limit:
                break

    return skus


# ===== ПОЛУЧЕНИЕ НАЗВАНИЙ ТОВАРОВ ПО product_id =====
async def fetch_product_names(product_ids: list[int]) -> dict[int, str]:
    """Возвращает словарь {product_id: name}"""
    result     = {}
    chunk_size = 1000

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            payload = {"product_id": chunk}
            async with session.post(
                f"{OZON_API_URL}/v2/product/info/list",
                headers=ozon_headers(),
                json=payload,
            ) as resp:
                raw = await resp.text()
                logging.info(f"Ozon product info status: {resp.status} | body: {raw[:300]}")
                if resp.status != 200:
                    continue
                data = json.loads(raw)

            for item in data.get("result", {}).get("items", []):
                pid  = item.get("id")
                name = item.get("name", "—")
                if pid:
                    result[pid] = name

    return result


# ===== ПОЛУЧЕНИЕ ЗАЯВОК НА ПОСТАВКУ =====
async def fetch_supply_requests() -> list[dict]:
    """
    Получает заявки на поставку на ближайшие 5 дней.
    Использует /v1/supply-order/list
    """
    now       = datetime.now(MOSCOW_TZ)
    date_from = now.strftime("%Y-%m-%dT00:00:00Z")
    date_to   = (now + timedelta(days=5)).strftime("%Y-%m-%dT23:59:59Z")

    payload = {
        "filter": {
            "supply_date_from": date_from,
            "supply_date_to":   date_to,
        },
        "paging": {
            "from_supply_order_id": 0,
            "limit": 100,
        },
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{OZON_API_URL}/v1/supply-order/list",
            headers=ozon_headers(),
            json=payload,
        ) as resp:
            raw = await resp.text()
            logging.info(f"Ozon supply status: {resp.status} | body: {raw[:500]}")
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}: {raw[:300]}")
            data = json.loads(raw)

    return data.get("supply_orders", [])


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все SKU",            callback_data="show_skus")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку", callback_data="show_supplies")],
    ])


# ===== СТАРТ =====
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "Привет! Выбери действие:",
        reply_markup=main_menu()
    )


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

    # Собираем все product_id для получения названий
    all_product_ids = []
    for order in orders:
        for item in order.get("items", []):
            pid = item.get("product_id")
            if pid:
                all_product_ids.append(pid)

    try:
        names = await fetch_product_names(list(set(all_product_ids)))
    except Exception:
        names = {}

    # Формируем текст
    lines = []
    lines.append(f"🚚 Заявок на ближайшие 5 дней: {len(orders)}\n")

    for order in orders:
        order_id    = order.get("supply_order_id", "—")
        status      = order.get("status", "—")
        supply_date = order.get("supply_date", "")[:10]

        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"📋 Заявка #{order_id}")
        lines.append(f"📅 Дата: {supply_date}")
        lines.append(f"🔖 Статус: {status}")
        lines.append("Товары:")

        for item in order.get("items", []):
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

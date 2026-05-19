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


# ===== ВСПОМОГАТЕЛЬНАЯ: POST-запрос =====
async def ozon_post(session: aiohttp.ClientSession, url: str, payload: dict) -> dict:
    async with session.post(url, headers=ozon_headers(), json=payload) as resp:
        raw = await resp.text()
        logging.info(f"POST {url} → {resp.status} | {raw[:600]}")
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
async def fetch_product_names(product_ids: list) -> dict:
    result     = {}
    chunk_size = 1000

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            try:
                data = await ozon_post(
                    session,
                    f"{OZON_API_URL}/v3/product/info/list",
                    {"product_id": chunk}
                )
                for item in data.get("items", []):
                    pid = item.get("id")
                    if pid:
                        result[pid] = item.get("name", "—")
            except Exception as e:
                logging.warning(f"product/info/list ошибка: {e}")

    return result


# ===== ПОЛУЧЕНИЕ order_id через /v3/supply-order/list =====
async def fetch_supply_order_ids(session: aiohttp.ClientSession) -> list:
    now       = datetime.now(MOSCOW_TZ)
    date_from = now.strftime("%Y-%m-%dT00:00:00Z")
    date_to   = (now + timedelta(days=5)).strftime("%Y-%m-%dT23:59:59Z")

    all_states = [
        "PLANNED", "ACCEPTED", "IN_PROGRESS", "COMPLETED",
        "CANCELLED", "DRAFT", "CONFIRMED", "REJECTED",
        "CREATED", "APPROVED",
    ]

    order_ids = []
    last_id   = ""
    limit     = 50

    while True:
        payload = {
            "filter": {
                "supply_date_from": date_from,
                "supply_date_to":   date_to,
                "states":           all_states,
            },
            "sort_by": 1,
            "limit":   limit,
        }
        if last_id:
            payload["last_id"] = last_id

        data  = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", payload)
        batch = data.get("order_ids", [])

        if not batch:
            break

        order_ids.extend(batch)

        new_last_id = str(data.get("last_id", ""))
        if not new_last_id or new_last_id == last_id or len(batch) < limit:
            break
        last_id = new_last_id

    return order_ids


# ===== ДЕТАЛИ ЗАЯВОК через /v3/supply-order/get =====
async def fetch_supply_order_details(session: aiohttp.ClientSession, order_ids: list) -> list[dict]:
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


# ===== ОСНОВНАЯ ФУНКЦИЯ ПОСТАВОК =====
async def fetch_supply_requests() -> list[dict]:
    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        logging.info(f"Получено order_id: {len(order_ids)}")

        if not order_ids:
            return []

        return await fetch_supply_order_details(session, order_ids)


# ===== ПАРСИНГ ОДНОЙ ЗАЯВКИ =====
def parse_order(order: dict, names: dict) -> list[str]:
    """
    Разбирает один объект заявки в список строк для вывода.
    Реальная структура из API:
    - order_id, order_number, state
    - drop_off_warehouse.name, drop_off_warehouse.address
    - supplies[]: arrival_date, warehouse.name, items[]: sku, product_id, quantity
    """
    order_id     = order.get("order_id", "—")
    order_number = order.get("order_number", "")
    status       = order.get("state", "—")
    created      = order.get("created_date", "")[:10]

    # Склад сдачи
    drop_wh      = order.get("drop_off_warehouse", {})
    drop_name    = drop_wh.get("name", drop_wh.get("address", "—"))

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"📋 Заявка #{order_id} ({order_number})")
    lines.append(f"🔖 Статус: {status}")
    lines.append(f"📅 Создана: {created}")
    lines.append(f"🏭 Склад: {drop_name}")

    supplies = order.get("supplies", [])
    if supplies:
        for supply in supplies:
            arrival  = supply.get("arrival_date", supply.get("planned_arrival_date", ""))[:10]
            wh       = supply.get("warehouse", supply.get("storage_warehouse", {}))
            wh_name  = wh.get("name", wh.get("warehouse_name", "")) if isinstance(wh, dict) else ""
            if arrival:
                lines.append(f"📦 Дата поставки: {arrival}" + (f" | Склад: {wh_name}" if wh_name else ""))

            items = supply.get("items", supply.get("bundles", []))
            if items:
                lines.append("Товары:")
                for item in items:
                    sku        = item.get("sku", item.get("product_id", "—"))
                    product_id = item.get("product_id")
                    qty        = item.get("quantity", item.get("quantity_in_supply", "—"))
                    name       = names.get(product_id, "—") if product_id else "—"
                    lines.append(f"  • SKU: {sku} | {name} | кол-во: {qty}")
    else:
        # Товары могут быть прямо в заявке
        items = order.get("items", order.get("bundles", []))
        if items:
            lines.append("Товары:")
            for item in items:
                sku        = item.get("sku", item.get("product_id", "—"))
                product_id = item.get("product_id")
                qty        = item.get("quantity", "—")
                name       = names.get(product_id, "—") if product_id else "—"
                lines.append(f"  • SKU: {sku} | {name} | кол-во: {qty}")
        else:
            lines.append("Товары: нет данных")

    lines.append("")
    return lines


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все SKU",            callback_data="show_skus")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку", callback_data="show_supplies")],
    ])


# ===== СТАРТ: удаляем старые сообщения =====
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

    await bot.send_message(
        chat_id=chat_id,
        text="Привет! Выбери действие:",
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
    all_product_ids = set()
    for order in orders:
        for supply in order.get("supplies", []):
            for item in supply.get("items", supply.get("bundles", [])):
                pid = item.get("product_id")
                if pid:
                    all_product_ids.add(pid)
        for item in order.get("items", order.get("bundles", [])):
            pid = item.get("product_id")
            if pid:
                all_product_ids.add(pid)

    try:
        names = await fetch_product_names(list(all_product_ids))
    except Exception:
        names = {}

    lines = [f"🚚 Заявок на ближайшие 5 дней: {len(orders)}\n"]
    for order in orders:
        lines.extend(parse_order(order, names))

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

import asyncio
import logging
import json
import os
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = os.environ["OZON_CLIENT_ID"]
OZON_API_KEY   = os.environ["OZON_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OZON_API_URL   = "https://api-seller.ozon.ru"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()


# ===== ПОЛУЧЕНИЕ SKU С OZON =====
async def fetch_all_skus() -> list[dict]:
    headers = {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }
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
                f"{OZON_API_URL}/v2/product/list",
                headers=headers,
                json=payload,
            ) as resp:
                raw = await resp.text()
                logging.info(f"Ozon status: {resp.status} | body: {raw[:500]}")

                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}: {raw[:300]}")

                try:
                    data = json.loads(raw)
                except Exception:
                    raise Exception(f"Не удалось распарсить JSON. Ответ: {raw[:300]}")

            items   = data.get("result", {}).get("items", [])
            last_id = data.get("result", {}).get("last_id", "")

            if not items:
                break

            skus.extend(items)

            if len(items) < limit:
                break

    return skus


# ===== СТАРТ =====
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Показать все SKU", callback_data="show_skus")]
    ])
    await message.answer(
        "Привет! Нажми кнопку, чтобы получить список артикулов из Ozon Seller.",
        reply_markup=kb
    )


# ===== ОБРАБОТКА КНОПКИ =====
@dp.callback_query(F.data == "show_skus")
async def handle_show_skus(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Загружаю список артикулов...")

    try:
        items = await fetch_all_skus()
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка при запросе к Ozon API:\n{e}")
        return

    if not items:
        await call.message.edit_text("📭 Артикулы не найдены.")
        return

    # Формируем строки
    lines = []
    for i, item in enumerate(items, 1):
        offer_id   = item.get("offer_id", "—")
        product_id = item.get("product_id", "—")
        lines.append(f"{i}. Артикул: {offer_id} | product_id: {product_id}")

    text_full = "\n".join(lines)

    # Разбиваем на чанки по 4000 символов (лимит Telegram — 4096)
    chunks = [text_full[i:i + 4000] for i in range(0, len(text_full), 4000)]

    header = f"📦 Найдено артикулов: {len(items)}\n\n"
    await call.message.edit_text(header + chunks[0])

    for chunk in chunks[1:]:
        await call.message.answer(chunk)

    # Кнопка «Обновить»
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="show_skus")]
    ])
    await call.message.answer("Готово! Можно обновить список:", reply_markup=kb)


# ===== ЗАПУСК =====
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

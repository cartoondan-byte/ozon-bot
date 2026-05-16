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

# ===== OZON API =====

async def ozon_post(session, url, payload, retry=3):
    """Запрос к Ozon API с автоматическим retry при rate limit"""
    for attempt in range(retry):
        await asyncio.sleep(2)  # пауза перед каждым запросом
        async with session.post(url, json=payload, headers=HEADERS) as resp:
            text = await resp.text()
            logger.info(f"POST {url} status={resp.status} body={text[:300]}")

            if resp.status == 429:
                wait = 5 * (attempt + 1)
                logger.warning(f"Rate limit, ждём {wait} сек (попытка {attempt+1})")
                await asyncio.sleep(wait)
                continue

            if resp.status == 403:
                raise Exception("Нет доступа (403). Проверь права API-ключа.")
            if resp.status == 401:
                raise Exception("Неверный API-ключ (401).")
            if resp.status == 404:
                raise Exception(f"Метод не найден (404): {url}")
            if resp.status != 200:
                raise Exception(f"Ошибка {resp.status}: {text[:200]}")

            return json.loads(text)

    raise Exception("Превышен лимит запросов, попробуй позже")


async def get_supply_orders(session):
    """Получить заявки со статусом DATA_FILLING"""
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

    # Фильтруем только DATA_FILLING
    result = [o for o in all_orders if o.get("state", "") == "DATA_FILLING"]
    logger.info(f"Всего заявок: {len(all_orders)}, DATA_FILLING: {len(result)}")
    return result


async def get_timeslots(session, supply_order_id, date_from, date_to):
    """Получить доступные таймслоты"""
    data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/get", {
        "supply_order_id": supply_order_id,
        "date_from": date_from,
        "date_to": date_to
    })
    return data.get("timeslots", [])


async def update_timeslot(session, supply_order_id, time_from, time_to):
    """Установить новый таймслот"""
    return await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/update", {
        "supply_order_id": supply_order_id,
        "timeslot": {
            "from": time_from,
            "to": time_to
        }
    })


# ===== ЛОГИКА =====

def find_best_slot(timeslots, tomorrow_date):
    """Найти слот 19:00-20:00 начиная с завтра, иначе первый доступный"""
    future = []
    for s in timeslots:
        try:
            dt = datetime.fromisoformat(s["from"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
            if dt.date() >= tomorrow_date:
                future.append((dt, s))
        except Exception:
            pass

    if not future:
        return None

    # Сначала ищем 19:00 завтра
    for dt, s in future:
        if dt.date() == tomorrow_date and dt.hour == 19:
            return s

    # Потом любое 19:00
    for dt, s in future:
        if dt.hour == 19:
            return s

    # Иначе первый доступный
    return future[0][1]


def format_slot(slot):
    try:
        dt_from = datetime.fromisoformat(slot["from"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
        dt_to = datetime.fromisoformat(slot["to"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
        return f"{dt_from.strftime('%d.%m.%Y %H:%M')}–{dt_to.strftime('%H:%M')}"
    except Exception:
        return f"{slot.get('from', '?')} – {slot.get('to', '?')}"


async def process_orders() -> str:
    now = datetime.now(MOSCOW_TZ)
    tomorrow = now + timedelta(days=1)
    date_from = tomorrow.strftime("%Y-%m-%dT00:00:00+03:00")
    date_to = (now + timedelta(days=8)).strftime("%Y-%m-%dT23:59:59+03:00")

    results, errors = [], []

    async with aiohttp.ClientSession() as session:
        orders = await get_supply_orders(session)

        if not orders:
            return "📭 Нет заявок со статусом «Заполнение данных»."

        for order in orders:
            oid = order.get("order_id")
            onum = order.get("order_number", str(oid))

            try:
                slots = await get_timeslots(session, oid, date_from, date_to)
                logger.info(f"Заявка {onum}: найдено слотов {len(slots)}")

                best = find_best_slot(slots, tomorrow.date())
                if not best:
                    errors.append(f"❌ {onum}: нет доступных слотов на 8 дней")
                    continue

                result = await update_timeslot(session, oid, best["from"], best["to"])
                logger.info(f"Update result: {result}")

                if not result.get("error"):
                    results.append(f"✅ {onum} → {format_slot(best)}")
                else:
                    errors.append(f"❌ {onum}: {result.get('message', str(result))}")

            except Exception as e:
                logger.exception(f"Ошибка для {onum}")
                errors.append(f"❌ {onum}: {str(e)}")

    lines = [f"📦 Заявок для переноса: {len(orders)}\n"]
    if results:
        lines.append("Успешно перенесено:")
        lines.extend(results)
    if errors:
        if results:
            lines.append("")
        lines.append("Проблемы:")
        lines.extend(errors)

    return "\n".join(lines)


# ===== TELEGRAM =====

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📅 Перенести заявки на день вперёд", callback_data="reschedule")
    ]])


def again_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Запустить снова", callback_data="reschedule")
    ]])


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Привет! Я бот для переноса заявок на поставку Ozon FBO.\n\n"
        "Переношу все заявки со статусом «Заполнение данных» на завтра (19:00–20:00).\n"
        "Если слот недоступен — нахожу ближайший доступный.\n\n"
        "Заявки со статусом «Готово» не трогаю.",
        reply_markup=main_keyboard()
    )


@dp.callback_query(F.data == "reschedule")
async def on_reschedule(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("⏳ Обрабатываю заявки, подождите (займёт ~30 сек)...")
    try:
        result = await process_orders()
    except Exception as e:
        logger.exception("process_orders error")
        result = f"❗ Ошибка: {str(e)}"
    await callback.message.edit_text(result, reply_markup=again_keyboard())


async def main():
    logger.info("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

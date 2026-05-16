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

async def ozon_post(session, url, payload):
    async with session.post(url, json=payload, headers=HEADERS) as resp:
        text = await resp.text()
        logger.info(f"POST {url} status={resp.status} body={text[:500]}")
        if resp.status == 429:
            logger.warning("Rate limit, ждём 2 сек...")
            await asyncio.sleep(2)
            raise Exception(f"Превышен лимит запросов, попробуй ещё раз через несколько секунд")
        if resp.status == 403:
            raise Exception("Нет доступа (403). Проверь права API-ключа в Ozon Seller → Настройки → API ключи.")
        if resp.status == 401:
            raise Exception("Неверный API-ключ (401). Проверь Client-Id и Api-Key.")
        if resp.status == 404:
            raise Exception(f"Метод не найден (404): {url}")
        if resp.status != 200:
            raise Exception(f"Ozon API вернул ошибку {resp.status}: {text[:300]}")
        try:
            return json.loads(text)
        except Exception:
            raise Exception(f"Ozon API вернул не JSON: {text[:300]}")

async def get_supply_orders(session):
    # Шаг 1: получаем список ID заявок
    data = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/list", {
        "limit": 50,
        "from_supply_order_id": 0,
        "sort_by": 1,
        "sort_direction": 1,
        "filter": {
            "states": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }
    })
    order_ids = data.get("order_ids", [])
    if not order_ids:
        return []

    # Шаг 2: получаем детали всех заявок одним запросом
    detail = await ozon_post(session, f"{OZON_API_URL}/v3/supply-order/get", {
        "order_ids": order_ids
    })
    logger.info(f"supply-order/get response: {str(detail)[:500]}")
    return detail.get("orders", [])

async def get_available_timeslots_via_draft(session, supply_order_id, date_from, date_to):
    """Пробуем получить таймслоты через draft endpoint"""
    data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/get", {
        "supply_order_id": supply_order_id,
        "date_from": date_from,
        "date_to": date_to
    })
    logger.info(f"timeslot/get response: {str(data)[:300]}")
    return data

async def get_timeslots(session, supply_order_id, date_from, date_to):
    data = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/get", {
        "supply_order_id": supply_order_id,
        "date_from": date_from,
        "date_to": date_to
    })
    logger.info(f"timeslot response: {str(data)[:300]}")
    return data.get("timeslots", [])

async def update_timeslot(session, supply_order_id, time_from, time_to):
    return await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/timeslot/update", {
        "supply_order_id": supply_order_id,
        "timeslot": {
            "from": time_from,
            "to": time_to
        }
    })

# ===== ЛОГИКА =====

def is_target_status(status: str) -> bool:
    logger.info(f"Checking status: {status}")
    # Из логов: нужный статус DATA_FILLING, пропускаем CANCELLED, COMPLETED, READY и т.д.
    return status.upper() == "DATA_FILLING"

def find_best_timeslot(timeslots):
    for slot in timeslots:
        try:
            dt = datetime.fromisoformat(slot["from"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
            if dt.hour == 19:
                return slot
        except Exception:
            pass
    return timeslots[0] if timeslots else None

def format_slot(slot) -> str:
    try:
        dt_from = datetime.fromisoformat(slot["from"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
        dt_to = datetime.fromisoformat(slot["to"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
        return f"{dt_from.strftime('%d.%m.%Y %H:%M')}–{dt_to.strftime('%H:%M')}"
    except Exception:
        return str(slot.get("timeslot_id", "?"))

async def process_orders() -> str:
    now = datetime.now(MOSCOW_TZ)
    tomorrow = now + timedelta(days=1)
    date_from = tomorrow.strftime("%Y-%m-%dT00:00:00+03:00")
    date_to = (now + timedelta(days=8)).strftime("%Y-%m-%dT23:59:59+03:00")

    results, errors = [], []

    async with aiohttp.ClientSession() as session:
        orders = await get_supply_orders(session)

        if not orders:
            return "📭 Заявок на поставку не найдено."

        targets = [o for o in orders if is_target_status(o.get("state", ""))]

        if not targets:
            return f"✅ Нет заявок для переноса (все в статусе «Готово»).\nВсего заявок: {len(orders)}"

        for order in targets:
            oid = order.get("supply_order_id") or order.get("order_id")
            onum = order.get("supply_order_number") or order.get("order_number") or str(oid)

            try:
                slots = await get_timeslots(session, oid, date_from, date_to)

                future_slots = []
                for s in slots:
                    try:
                        dt = datetime.fromisoformat(s["from"].replace("Z", "+00:00")).astimezone(MOSCOW_TZ)
                        if dt.date() >= tomorrow.date():
                            future_slots.append(s)
                    except Exception:
                        future_slots.append(s)

                if not future_slots:
                    errors.append(f"❌ {onum}: нет слотов на 7 дней вперёд")
                    continue

                best = find_best_timeslot(future_slots)
                if not best:
                    errors.append(f"❌ {onum}: не найден слот")
                    continue

                logger.info(f"Best slot keys: {list(best.keys())} values: {best}")
                slot_id = (best.get("timeslot_id") or best.get("id") or 
                          best.get("slot_id") or best.get("timeslot") or
                          best.get("time_slot_id"))
                if not slot_id:
                    errors.append(f"❌ {onum}: не найден ID слота. Ключи: {list(best.keys())}")
                    continue
                result = await update_timeslot(session, oid, slot_id)

                if result.get("operation_id") or not result.get("error"):
                    results.append(f"✅ {onum} → {format_slot(best)}")
                else:
                    errors.append(f"❌ {onum}: {result.get('message') or result.get('error') or str(result)}")

            except Exception as e:
                logger.exception(f"Error: {onum}")
                errors.append(f"❌ {onum}: {str(e)}")

    lines = [f"📦 Заявок для переноса: {len(targets)}\n"]
    if results:
        lines.append("Успешно:")
        lines.extend(results)
    if errors:
        if results: lines.append("")
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
        "Если слот недоступен — нахожу ближайший.\n\n"
        "Заявки со статусом «Готово» не трогаю.",
        reply_markup=main_keyboard()
    )

@dp.callback_query(F.data == "reschedule")
async def on_reschedule(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("⏳ Обрабатываю заявки, подождите...")
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

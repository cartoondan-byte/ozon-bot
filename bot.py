import asyncio
import logging
from datetime import datetime, timedelta
import pytz
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===== НАСТРОЙКИ =====
OZON_CLIENT_ID = "1360213"
OZON_API_KEY = "dd0e57bc-1497-4e70-a642-63266dbddcc7"
TELEGRAM_TOKEN = "8801888159:AAFJIece-JoNfGvg9PP5brVygU46_XdRbU0"
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

async def get_supply_orders(session):
    url = f"{OZON_API_URL}/v1/supply-order/list"
    payload = {"paging": {"from_supply_order_id": 0, "limit": 50}, "filter": {}}
    async with session.post(url, json=payload, headers=HEADERS) as resp:
        data = await resp.json()
        logger.info(f"supply-order/list: {data}")
        return data.get("supply_orders", [])

async def get_timeslots(session, supply_order_id, date_from, date_to):
    url = f"{OZON_API_URL}/v1/supply-order/timeslot/list"
    payload = {"supply_order_id": supply_order_id, "date_from": date_from, "date_to": date_to}
    async with session.post(url, json=payload, headers=HEADERS) as resp:
        data = await resp.json()
        logger.info(f"timeslot/list for {supply_order_id}: {data}")
        return data.get("timeslots", [])

async def update_timeslot(session, supply_order_id, timeslot_id):
    url = f"{OZON_API_URL}/v1/supply-order/timeslot/update"
    payload = {"supply_order_id": supply_order_id, "timeslot_id": timeslot_id}
    async with session.post(url, json=payload, headers=HEADERS) as resp:
        data = await resp.json()
        logger.info(f"timeslot/update for {supply_order_id}: {data}")
        return data

# ===== ЛОГИКА =====

def is_target_status(status: str) -> bool:
    skip = ["ready", "completed", "готово", "ready_to_supply"]
    s = status.lower()
    return not any(x in s for x in skip)

def find_best_timeslot(timeslots: list):
    # Сначала ищем 19:00
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

        targets = [o for o in orders if is_target_status(o.get("status", ""))]

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

                slot_id = best.get("timeslot_id") or best.get("id")
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
        lines.append("Ошибки:")
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

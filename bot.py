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

# ===== МАППИНГ КЛАСТЕРОВ =====
CLUSTER_NAMES: dict = {}

def cluster_name(cluster_id: str) -> str:
    name = CLUSTER_NAMES.get(str(cluster_id), "")
    if name:
        return f"Кластер {name}"
    return f"Кластер {cluster_id}"


async def load_cluster_names() -> None:
    """Перебирает варианты cluster_type пока не найдёт рабочий."""
    global CLUSTER_NAMES

    for cluster_type in [
        "CLUSTER_TYPE_SUPPLY_DELIVERY",
        "CLUSTER_TYPE_SUPPLY",
        "CLUSTER_TYPE_DELIVERY",
        "SUPPLY_DELIVERY",
        "SUPPLY",
        "DELIVERY",
    ]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{OZON_API_URL}/v1/cluster/list",
                    headers=ozon_headers(),
                    json={"cluster_type": cluster_type}
                ) as resp:
                    raw  = await resp.text()
                    logging.info(f"cluster/list type={cluster_type} → {resp.status} | {raw[:300]}")
                    if resp.status != 200:
                        continue
                    data = json.loads(raw)

            clusters = (
                data.get("clusters") or
                data.get("result", {}).get("clusters") or
                data.get("result") or []
            )
            if not isinstance(clusters, list):
                clusters = []

            for c in clusters:
                cid  = str(c.get("cluster_id") or c.get("id") or c.get("macrolocal_cluster_id") or "")
                name = c.get("name") or c.get("cluster_name") or c.get("title") or ""
                if cid and name:
                    CLUSTER_NAMES[cid] = name

            if CLUSTER_NAMES:
                logging.info(f"Загружено кластеров ({cluster_type}): {len(CLUSTER_NAMES)}")
                return

        except Exception as e:
            logging.warning(f"cluster/list type={cluster_type}: {e}")

    logging.warning("Кластеры не загружены — будут показаны числовые коды")


# ===== КЭШ =====
_cache: dict = {}


# ===== ЗАГОЛОВКИ =====
def ozon_headers() -> dict:
    return {
        "Client-Id":    OZON_CLIENT_ID,
        "Api-Key":      OZON_API_KEY,
        "Content-Type": "application/json",
    }


# ===== POST с retry на 429 =====
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


# ===== ТОВАРЫ ИЗ БАНДЛОВ =====
async def fetch_bundle_items(session: aiohttp.ClientSession, bundle_ids: list) -> dict:
    result = {}
    for bid in bundle_ids:
        try:
            data  = await ozon_post(session, f"{OZON_API_URL}/v1/supply-order/bundle",
                                    {"bundle_ids": [bid], "last_id": "", "limit": 100})
            result[bid] = data.get("items", [])
        except Exception as e:
            logging.warning(f"bundle {bid}: {e}")
    return result


# ===== КЛЮЧ И МЕТКА НАЗНАЧЕНИЯ =====
def get_dest_key(order: dict) -> str:
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
    if dest_key.startswith("wh::"):
        return dest_key[4:]
    if dest_key.startswith("cl::"):
        return cluster_name(dest_key[4:])
    return dest_key


# ===== ЗАГРУЗКА И ГРУППИРОВКА ВСЕХ ЗАЯВОК =====
async def load_all_orders() -> dict:
    async with aiohttp.ClientSession() as session:
        order_ids = await fetch_supply_order_ids(session)
        logging.info(f"DATA_FILLING: {len(order_ids)} заявок")
        if not order_ids:
            return {}

        orders = await fetch_supply_order_details(session, order_ids)

        bundle_ids = []
        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                if bid:
                    bundle_ids.append(bid)

        bundle_items = await fetch_bundle_items(session, bundle_ids) if bundle_ids else {}

        for order in orders:
            for supply in order.get("supplies", []):
                bid = supply.get("bundle_id")
                supply["_items"] = bundle_items.get(bid, []) if bid else []

    grouped: dict = {}
    for order in orders:
        key = get_dest_key(order)
        grouped.setdefault(key, []).append(order)

    return grouped


# ===== ГЛАВНОЕ МЕНЮ =====
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Ближайшие заявки на перенос", callback_data="show_skus")],
        [InlineKeyboardButton(text="🚚 Заявки на поставку",          callback_data="show_supplies")],
    ])


# ===== МЕНЮ СКЛАДОВ/КЛАСТЕРОВ =====
def dest_menu(grouped: dict) -> InlineKeyboardMarkup:
    buttons = []
    for dest_key, orders in sorted(grouped.items(), key=lambda x: get_dest_label(x[0])):
        label = f"{get_dest_label(dest_key)} ({len(orders)})"
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"dest::{dest_key[:50]}"
        )])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить",      callback_data="show_supplies")])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню",  callback_data="main_menu")])
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


# ===== ГЛАВНОЕ МЕНЮ (callback) =====
@dp.callback_query(F.data == "main_menu")
async def handle_main_menu(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("Выбери действие:", reply_markup=main_menu())


# ===== БЛИЖАЙШИЕ ЗАЯВКИ НА ПЕРЕНОС =====
@dp.callback_query(F.data == "show_skus")
async def handle_show_skus(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("⏳ Ищу заявки с датой поставки в ближайшие 5 дней...")

    grouped = _cache.get("grouped")
    if not grouped:
        try:
            grouped = await load_all_orders()
            _cache["grouped"] = grouped
        except Exception as e:
            await call.message.edit_text(f"❌ Ошибка:\n{e}", reply_markup=main_menu())
            return

    all_orders = [order for orders in grouped.values() for order in orders]

    now     = datetime.now(MOSCOW_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = now + timedelta(days=5)

    near_orders = []
    for order in all_orders:
        ts      = order.get("timeslot", {}).get("timeslot", {})
        ts_from = ts.get("from", "")
        if not ts_from:
            continue
        try:
            dt = datetime.strptime(ts_from[:19], "%Y-%m-%dT%H:%M:%S")
            dt = MOSCOW_TZ.localize(dt)
            if now <= dt <= date_to:
                near_orders.append((dt, order))
        except Exception:
            continue

    near_orders.sort(key=lambda x: x[0])

    if not near_orders:
        await call.message.edit_text(
            "📭 Заявок с датой поставки в ближайшие 5 дней не найдено.",
            reply_markup=main_menu()
        )
        return

    all_pids = set()
    for _, order in near_orders:
        for supply in order.get("supplies", []):
            for item in supply.get("_items", []):
                pid = item.get("product_id")
                if pid:
                    all_pids.add(pid)

    names = await fetch_product_names(list(all_pids)) if all_pids else {}

    lines = [f"📅 Заявок в ближайшие 5 дней: {len(near_orders)}\n"]

    for dt, order in near_orders:
        order_id  = order.get("order_id", "—")
        order_num = order.get("order_number", "")
        created   = order.get("created_date", "")[:10]
        deadline  = (order.get("data_filling_deadline") or "")[:10]
        dest      = get_dest_label(get_dest_key(order))

        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"📋 #{order_id} ({order_num})")
        lines.append(f"🏭 Назначение: {dest}")
        lines.append(f"🗓 Дата поставки: {dt.strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"📅 Создана: {created}")
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
    back_kb   = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])

    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        kb = back_kb if i == len(chunks) - 1 else None
        await call.message.answer(chunk, reply_markup=kb)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Список выше", reply_markup=back_kb)


# ===== ЗАЯВКИ — ПОКАЗАТЬ СКЛАДЫ/КЛАСТЕРЫ =====
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


# ===== ВЫБРАН СКЛАД/КЛАСТЕР =====
@dp.callback_query(F.data.startswith("dest::"))
async def handle_dest_select(call: CallbackQuery):
    await call.answer()
    dest_key = call.data[6:]
    grouped  = _cache.get("grouped", {})

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
    back_kb   = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад к складам", callback_data="back_to_dests")],
        [InlineKeyboardButton(text="🏠 Главное меню",    callback_data="main_menu")],
    ])

    await call.message.edit_text(chunks[0], reply_markup=back_kb if len(chunks) == 1 else None)
    for i, chunk in enumerate(chunks[1:], 1):
        kb = back_kb if i == len(chunks) - 1 else None
        await call.message.answer(chunk, reply_markup=kb)
    if len(chunks) > 1:
        await call.message.answer("⬆️ Список выше", reply_markup=back_kb)


# ===== НАЗАД К СКЛАДАМ =====
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
    await load_cluster_names()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

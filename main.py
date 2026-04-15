import os
import json
import re
import html
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from oauth2client.service_account import ServiceAccountCredentials


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm_bot")


# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_RAW = os.getenv("GOOGLE_CREDENTIALS", "").strip()
ADMINS = [
    int(x.strip())
    for x in os.getenv("ADMINS", "").split(",")
    if x.strip().isdigit()
]

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")
if not BASE_WEBHOOK_URL:
    raise ValueError("BASE_WEBHOOK_URL topilmadi")
if not WEBHOOK_SECRET:
    raise ValueError("WEBHOOK_SECRET topilmadi")
if not GOOGLE_SHEET_ID:
    raise ValueError("GOOGLE_SHEET_ID topilmadi")
if not GOOGLE_CREDENTIALS_RAW:
    raise ValueError("GOOGLE_CREDENTIALS topilmadi")

WEBHOOK_PATH = f"/{WEBHOOK_SECRET}"
WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"


# =========================================================
# BOT / DP / LOCKS
# =========================================================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())

LEAD_LOCK = asyncio.Lock()
AGENT_LOCK = asyncio.Lock()


# =========================================================
# SHEETS
# =========================================================
def get_gspread_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_RAW)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


gc = get_gspread_client()
spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
leads_ws = spreadsheet.worksheet("Leads")
agents_ws = spreadsheet.worksheet("Agents")


# =========================================================
# CONSTANTS
# =========================================================
PURPOSE_MAP = {
    "🏠 Уйимни сотмоқчиман": "sell",
    "🔎 Уй сотиб олмоқчиман": "buy",
    "🏘 Ижарага уй бермоқчиман": "rent_out",
    "🔑 Ижарага уй олмоқчиман": "rent_in",
    "🏦 Ипотека хизматидан фойдаланиш": "mortgage_service",
    "🏢 Янги домлардан ипотекага уй сотиб олиш": "new_building_mortgage",
}

PURPOSE_LABELS = {
    "sell": "Уй сотиш",
    "buy": "Уй сотиб олиш",
    "rent_out": "Ижарага бериш",
    "rent_in": "Ижарага олиш",
    "mortgage_service": "Ипотека хизмати",
    "new_building_mortgage": "Янги домдан ипотека",
}

LEAD_STATUS_NEW = "new"
LEAD_STATUS_TAKEN = "taken"
LEAD_STATUS_DONE = "done"


# =========================================================
# STATES
# =========================================================
class LeadForm(StatesGroup):
    waiting_phone = State()
    waiting_property_id = State()
    waiting_description = State()


class AddAgentForm(StatesGroup):
    waiting_tg_id = State()
    waiting_full_name = State()
    waiting_phone = State()


# =========================================================
# HELPERS
# =========================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def clean_text(value: Optional[str]) -> str:
    return (value or "").strip()


def safe_int(value) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def escape_html_text(value: str) -> str:
    return html.escape(value or "")


def user_full_name(user) -> str:
    name = " ".join(x for x in [user.first_name, user.last_name] if x).strip()
    return name or "No name"


def normalize_phone(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if raw.startswith("+"):
        digits = "+" + re.sub(r"\D", "", raw)
    else:
        digits = re.sub(r"\D", "", raw)
        if digits.startswith("998"):
            digits = "+" + digits
    return digits or raw


def is_valid_phone(value: str) -> bool:
    phone = normalize_phone(value)
    digits = re.sub(r"\D", "", phone)
    return len(digits) >= 9


def username_text(user) -> str:
    return f"@{user.username}" if user.username else ""


def purpose_label(purpose_code: str) -> str:
    return PURPOSE_LABELS.get(purpose_code, purpose_code or "—")


def build_lead_note(old_note: str, new_line: str) -> str:
    old_note = clean_text(old_note)
    new_line = clean_text(new_line)
    if not old_note:
        return new_line
    return f"{old_note}\n{new_line}"


def headers_map(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {header: i + 1 for i, header in enumerate(headers)}


# =========================================================
# KEYBOARDS
# =========================================================
def client_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Уйимни сотмоқчиман")],
            [KeyboardButton(text="🔎 Уй сотиб олмоқчиман")],
            [KeyboardButton(text="🏘 Ижарага уй бермоқчиман")],
            [KeyboardButton(text="🔑 Ижарага уй олмоқчиман")],
            [KeyboardButton(text="🏦 Ипотека хизматидан фойдаланиш")],
            [KeyboardButton(text="🏢 Янги домлардан ипотекага уй сотиб олиш")],
        ],
        resize_keyboard=True,
    )


def ask_phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📞 Телефон рақамни юбориш", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="👤 Агент қўшиш")],
            [KeyboardButton(text="📋 Очиқ лидлар")],
        ],
        resize_keyboard=True,
    )


def lead_action_kb(lead_id: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Олдим", callback_data=f"lead_take:{lead_id}"),
                InlineKeyboardButton(text="❌ Рад этдим", callback_data=f"lead_reject:{lead_id}"),
            ],
            [
                InlineKeyboardButton(text="🏁 Бажарилди", callback_data=f"lead_done:{lead_id}")
            ],
        ]
    )


# =========================================================
# AGENTS SHEET
# =========================================================
def get_agents_records() -> List[Dict]:
    return agents_ws.get_all_records()


def get_agent_by_tg_id(tg_id: int) -> Optional[Dict]:
    for row in get_agents_records():
        if safe_int(row.get("tg_id")) == tg_id:
            return row
    return None


def is_admin(tg_id: int) -> bool:
    if tg_id in ADMINS:
        return True
    row = get_agent_by_tg_id(tg_id)
    if not row:
        return False
    return clean_text(row.get("role")).lower() == "admin"


def is_agent(tg_id: int) -> bool:
    row = get_agent_by_tg_id(tg_id)
    if not row:
        return False
    return (
        clean_text(row.get("role")).lower() == "agent"
        and clean_text(row.get("is_active")).lower() == "yes"
    )


def get_role(tg_id: int) -> str:
    if is_admin(tg_id):
        return "admin"
    if is_agent(tg_id):
        return "agent"
    return "client"


def get_active_agents() -> List[Dict]:
    result = []
    for row in get_agents_records():
        if (
            clean_text(row.get("role")).lower() == "agent"
            and clean_text(row.get("is_active")).lower() == "yes"
            and clean_text(row.get("can_take_leads")).lower() == "yes"
        ):
            result.append(row)
    return result


def add_or_update_agent(tg_id: int, full_name: str, phone: str):
    records = get_agents_records()
    headers = headers_map(agents_ws)

    for idx, row in enumerate(records, start=2):
        if safe_int(row.get("tg_id")) == tg_id:
            updates = {
                "tg_id": str(tg_id),
                "full_name": full_name,
                "phone": phone,
                "role": "agent",
                "is_active": "yes",
                "can_take_leads": "yes",
                "registered_at": clean_text(row.get("registered_at")) or now_str(),
            }
            for key, value in updates.items():
                col = headers.get(key)
                if col:
                    agents_ws.update_cell(idx, col, value)
            return

    new_row = [
        str(tg_id),          # tg_id
        full_name,           # full_name
        phone,               # phone
        "",                  # username
        "agent",             # role
        "yes",               # is_active
        "yes",               # can_take_leads
        now_str(),           # registered_at
        "",                  # notes
    ]
    agents_ws.append_row(new_row, value_input_option="USER_ENTERED")


# =========================================================
# LEADS SHEET
# =========================================================
def get_leads_records() -> List[Dict]:
    return leads_ws.get_all_records()


def get_lead_by_id(lead_id: str) -> Optional[Dict]:
    for row in get_leads_records():
        if clean_text(row.get("lead_id")) == lead_id:
            return row
    return None


def get_lead_row_index_by_id(lead_id: str) -> Optional[int]:
    for idx, row in enumerate(get_leads_records(), start=2):
        if clean_text(row.get("lead_id")) == lead_id:
            return idx
    return None


def generate_lead_id() -> str:
    max_num = 0
    for row in get_leads_records():
        lead_id = clean_text(row.get("lead_id"))
        if lead_id.startswith("LD-"):
            try:
                num = int(lead_id.split("-")[1])
                max_num = max(max_num, num)
            except Exception:
                pass
    return f"LD-{max_num + 1:04d}"


def create_lead(data: Dict) -> str:
    lead_id = generate_lead_id()
    row = [
        lead_id,                                # lead_id
        now_str(),                              # created_at
        data.get("purpose", ""),                # purpose
        data.get("property_id", ""),            # property_id
        str(data.get("client_tg_id", "")),      # client_tg_id
        data.get("client_name", ""),            # client_name
        data.get("client_phone", ""),           # client_phone
        data.get("client_username", ""),        # client_username
        data.get("lead_text", ""),              # lead_text
        LEAD_STATUS_NEW,                        # lead_status
        "",                                     # assigned_to_tg_id
        "",                                     # assigned_to_name
        "",                                     # taken_at
        "",                                     # finished_at
        "",                                     # result
        "bot",                                  # source
        "",                                     # group_message_id
        "",                                     # notes
    ]
    leads_ws.append_row(row, value_input_option="USER_ENTERED")
    return lead_id


def update_lead_fields(lead_id: str, updates: Dict[str, str]) -> bool:
    row_index = get_lead_row_index_by_id(lead_id)
    if not row_index:
        return False

    headers = headers_map(leads_ws)
    for key, value in updates.items():
        col = headers.get(key)
        if col:
            leads_ws.update_cell(row_index, col, value)
    return True


def assign_lead_to_agent(lead_id: str, agent_tg_id: int, agent_name: str) -> Tuple[bool, str]:
    lead = get_lead_by_id(lead_id)
    if not lead:
        return False, "Лид топилмади"

    if clean_text(lead.get("lead_status")) != LEAD_STATUS_NEW:
        return False, "Бу лид аллақачон олинган ёки якунланган"

    ok = update_lead_fields(
        lead_id,
        {
            "lead_status": LEAD_STATUS_TAKEN,
            "assigned_to_tg_id": str(agent_tg_id),
            "assigned_to_name": agent_name,
            "taken_at": now_str(),
            "result": "in_progress",
            "notes": build_lead_note(
                clean_text(lead.get("notes")),
                f"{now_str()} | taken by {agent_name} ({agent_tg_id})",
            ),
        },
    )
    return (ok, "ok" if ok else "Лидни сақлашда хато")


def reopen_lead(lead_id: str, actor_name: str, actor_tg_id: int) -> Tuple[bool, str]:
    lead = get_lead_by_id(lead_id)
    if not lead:
        return False, "Лид топилмади"

    ok = update_lead_fields(
        lead_id,
        {
            "lead_status": LEAD_STATUS_NEW,
            "assigned_to_tg_id": "",
            "assigned_to_name": "",
            "taken_at": "",
            "result": "rejected_by_agent",
            "notes": build_lead_note(
                clean_text(lead.get("notes")),
                f"{now_str()} | reopened by {actor_name} ({actor_tg_id})",
            ),
        },
    )
    return (ok, "ok" if ok else "Лидни қайта очишда хато")


def finish_lead(lead_id: str, actor_name: str, actor_tg_id: int) -> Tuple[bool, str]:
    lead = get_lead_by_id(lead_id)
    if not lead:
        return False, "Лид топилмади"

    if clean_text(lead.get("lead_status")) == LEAD_STATUS_DONE:
        return False, "Бу лид аллақачон якунланган"

    ok = update_lead_fields(
        lead_id,
        {
            "lead_status": LEAD_STATUS_DONE,
            "finished_at": now_str(),
            "result": "completed",
            "notes": build_lead_note(
                clean_text(lead.get("notes")),
                f"{now_str()} | done by {actor_name} ({actor_tg_id})",
            ),
        },
    )
    return (ok, "ok" if ok else "Лидни якунлашда хато")


# =========================================================
# FORMATTERS
# =========================================================
def format_lead_for_agents(lead: Dict) -> str:
    lead_id = escape_html_text(clean_text(lead.get("lead_id")))
    purpose = escape_html_text(purpose_label(clean_text(lead.get("purpose"))))
    client_name = escape_html_text(clean_text(lead.get("client_name")))
    client_phone = escape_html_text(clean_text(lead.get("client_phone")))
    client_username = escape_html_text(clean_text(lead.get("client_username")))
    property_id = escape_html_text(clean_text(lead.get("property_id")))
    lead_text = escape_html_text(clean_text(lead.get("lead_text")))
    status = escape_html_text(clean_text(lead.get("lead_status")).upper())

    parts = [
        "🆕 <b>Янги лид</b>",
        "",
        f"<b>ID:</b> {lead_id}",
        f"<b>Мақсад:</b> {purpose}",
        f"<b>Мижоз:</b> {client_name}",
        f"<b>Телефон:</b> {client_phone}",
    ]

    if client_username:
        parts.append(f"<b>Username:</b> {client_username}")
    if property_id:
        parts.append(f"<b>Property ID:</b> {property_id}")
    if lead_text:
        parts.append(f"<b>Изоҳ:</b> {lead_text}")

    parts.append(f"<b>Ҳолат:</b> {status}")
    return "\n".join(parts)


def format_lead_short(lead: Dict) -> str:
    return (
        f"{clean_text(lead.get('lead_id'))} | "
        f"{purpose_label(clean_text(lead.get('purpose')))} | "
        f"{clean_text(lead.get('lead_status'))} | "
        f"{clean_text(lead.get('client_name'))}"
    )


# =========================================================
# NOTIFICATIONS
# =========================================================
async def safe_send(chat_id: int, text: str, reply_markup=None):
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        return True
    except Exception as e:
        logger.exception(f"Yuborishda xato chat_id={chat_id}: {e}")
        return False


async def notify_agents_about_lead(lead_id: str):
    lead = get_lead_by_id(lead_id)
    if not lead:
        return

    agents = get_active_agents()
    text = format_lead_for_agents(lead)

    for agent in agents:
        tg_id = safe_int(agent.get("tg_id"))
        if not tg_id:
            continue
        await safe_send(tg_id, text, reply_markup=lead_action_kb(lead_id))


async def notify_admins(text: str):
    admin_ids = set(ADMINS)
    for row in get_agents_records():
        if clean_text(row.get("role")).lower() == "admin":
            tg = safe_int(row.get("tg_id"))
            if tg:
                admin_ids.add(tg)

    for admin_id in admin_ids:
        await safe_send(admin_id, text)


# =========================================================
# STATS
# =========================================================
def build_stats_text() -> str:
    leads = get_leads_records()
    now = datetime.now()
    month_key = now.strftime("%Y-%m")

    total = len(leads)
    today_total = 0
    today_done = 0
    month_total = 0
    month_done = 0
    new_count = 0
    taken_count = 0
    done_count = 0

    agent_taken = {}
    agent_done = {}

    for row in leads:
        status = clean_text(row.get("lead_status"))
        assigned_name = clean_text(row.get("assigned_to_name"))
        created_at = parse_dt(clean_text(row.get("created_at")))
        finished_at = parse_dt(clean_text(row.get("finished_at")))

        if status == LEAD_STATUS_NEW:
            new_count += 1
        elif status == LEAD_STATUS_TAKEN:
            taken_count += 1
        elif status == LEAD_STATUS_DONE:
            done_count += 1

        if created_at and created_at.date() == now.date():
            today_total += 1
        if finished_at and finished_at.date() == now.date():
            today_done += 1
        if created_at and created_at.strftime("%Y-%m") == month_key:
            month_total += 1
        if finished_at and finished_at.strftime("%Y-%m") == month_key:
            month_done += 1

        if assigned_name:
            agent_taken[assigned_name] = agent_taken.get(assigned_name, 0) + 1

        if status == LEAD_STATUS_DONE and assigned_name:
            agent_done[assigned_name] = agent_done.get(assigned_name, 0) + 1

    lines = [
        "📊 <b>Статистика</b>",
        "",
        f"<b>Жами лид:</b> {total}",
        f"<b>Янги:</b> {new_count}",
        f"<b>Олинган:</b> {taken_count}",
        f"<b>Бажарилган:</b> {done_count}",
        "",
        f"<b>Бугун тушган:</b> {today_total}",
        f"<b>Бугун якунланган:</b> {today_done}",
        f"<b>Ойлик тушган:</b> {month_total}",
        f"<b>Ойлик якунланган:</b> {month_done}",
        "",
        "<b>Агентлар кесими:</b>",
    ]

    agent_names = sorted(set(list(agent_taken.keys()) + list(agent_done.keys())))
    if not agent_names:
        lines.append("Ҳозирча маълумот йўқ")
    else:
        for name in agent_names:
            lines.append(
                f"• {escape_html_text(name)} — олган: {agent_taken.get(name, 0)}, бажарган: {agent_done.get(name, 0)}"
            )

    return "\n".join(lines)


def build_open_leads_text() -> str:
    leads = get_leads_records()
    open_leads = [
        row for row in leads
        if clean_text(row.get("lead_status")) in (LEAD_STATUS_NEW, LEAD_STATUS_TAKEN)
    ]

    if not open_leads:
        return "📋 Очиқ лидлар йўқ"

    lines = ["📋 <b>Очиқ лидлар</b>", ""]
    for row in open_leads[-30:]:
        lines.append(escape_html_text(format_lead_short(row)))
    return "\n".join(lines)


# =========================================================
# FLOW HELPERS
# =========================================================
async def ask_next_step_after_phone(message: Message, state: FSMContext):
    data = await state.get_data()
    purpose = data.get("purpose")

    if purpose == "buy":
        await message.answer(
            "Каналда кўрган уй ID рақамини юборинг:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(LeadForm.waiting_property_id)
    else:
        await message.answer(
            "Маълумотни батафсил ёзиб юборинг:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(LeadForm.waiting_description)


async def process_phone_input(message: Message, state: FSMContext, phone: str):
    phone = normalize_phone(phone)
    if not is_valid_phone(phone):
        await message.answer("Телефон рақам нотўғри. Қайта юборинг:")
        return

    await state.update_data(client_phone=phone)
    await ask_next_step_after_phone(message, state)


# =========================================================
# START / MENUS
# =========================================================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    role = get_role(message.from_user.id)

    if role == "admin":
        await message.answer("Сиз админсиз.", reply_markup=admin_menu())
        return

    if role == "agent":
        await message.answer(
            "Сиз агентсиз. Янги лидлар шу ерга тушади.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await message.answer("Хизмат турини танланг:", reply_markup=client_menu())


@dp.message(Command("admin"))
async def admin_command(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("Админ меню:", reply_markup=admin_menu())


# =========================================================
# CLIENT FLOW
# =========================================================
@dp.message(F.text.in_(list(PURPOSE_MAP.keys())))
async def client_choose_purpose(message: Message, state: FSMContext):
    if get_role(message.from_user.id) != "client":
        return

    purpose = PURPOSE_MAP[message.text]
    await state.clear()
    await state.update_data(purpose=purpose)

    await message.answer(
        "Телефон рақамингизни юборинг ёки қўлда ёзинг:",
        reply_markup=ask_phone_kb(),
    )
    await state.set_state(LeadForm.waiting_phone)


@dp.message(LeadForm.waiting_phone, F.contact)
async def lead_phone_contact(message: Message, state: FSMContext):
    phone = clean_text(message.contact.phone_number)
    await process_phone_input(message, state, phone)


@dp.message(LeadForm.waiting_phone)
async def lead_phone_text(message: Message, state: FSMContext):
    await process_phone_input(message, state, message.text or "")


@dp.message(LeadForm.waiting_property_id)
async def lead_property_id(message: Message, state: FSMContext):
    property_id = clean_text(message.text)
    if not property_id:
        await message.answer("ID рақамни қайта юборинг:")
        return

    await state.update_data(property_id=property_id)
    await message.answer("Изоҳингизни ёзинг:")
    await state.set_state(LeadForm.waiting_description)


@dp.message(LeadForm.waiting_description)
async def lead_description(message: Message, state: FSMContext):
    data = await state.get_data()

    lead_payload = {
        "purpose": data.get("purpose", ""),
        "property_id": data.get("property_id", ""),
        "client_tg_id": message.from_user.id,
        "client_name": user_full_name(message.from_user),
        "client_phone": data.get("client_phone", ""),
        "client_username": username_text(message.from_user),
        "lead_text": clean_text(message.text),
    }

    async with LEAD_LOCK:
        lead_id = create_lead(lead_payload)

    await state.clear()

    await message.answer(
        f"✅ Аризангиз қабул қилинди.\nЛид ID: <b>{escape_html_text(lead_id)}</b>\nТез орада сиз билан боғланишади.",
        reply_markup=client_menu(),
    )

    await notify_agents_about_lead(lead_id)
    await notify_admins(f"🆕 Янги лид тушди: {escape_html_text(lead_id)}")


# =========================================================
# AGENT CALLBACKS
# =========================================================
@dp.callback_query(F.data.startswith("lead_take:"))
async def callback_take_lead(callback: CallbackQuery):
    tg_id = callback.from_user.id
    role = get_role(tg_id)

    if role not in ("agent", "admin"):
        await callback.answer("Сизда рухсат йўқ", show_alert=True)
        return

    lead_id = callback.data.split(":", 1)[1]
    actor_name = user_full_name(callback.from_user)

    async with LEAD_LOCK:
        ok, msg = assign_lead_to_agent(lead_id, tg_id, actor_name)

    if not ok:
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer("Лид сизга бириктирилди")
    await safe_send(tg_id, f"✅ Лид <b>{escape_html_text(lead_id)}</b> сизга бириктирилди")
    await notify_admins(f"✅ Лид олинди: {escape_html_text(lead_id)} | {escape_html_text(actor_name)}")


@dp.callback_query(F.data.startswith("lead_reject:"))
async def callback_reject_lead(callback: CallbackQuery):
    tg_id = callback.from_user.id
    role = get_role(tg_id)

    if role not in ("agent", "admin"):
        await callback.answer("Сизда рухсат йўқ", show_alert=True)
        return

    lead_id = callback.data.split(":", 1)[1]
    actor_name = user_full_name(callback.from_user)

    lead = get_lead_by_id(lead_id)
    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = safe_int(lead.get("assigned_to_tg_id"))
    if role != "admin" and assigned_to != tg_id:
        await callback.answer("Фақат бириктирилган агент рад этиши мумкин", show_alert=True)
        return

    async with LEAD_LOCK:
        ok, msg = reopen_lead(lead_id, actor_name, tg_id)

    if not ok:
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer("Лид қайта очилди")
    await safe_send(tg_id, f"❌ Лид <b>{escape_html_text(lead_id)}</b> қайта очилди")
    await notify_admins(f"❌ Лид қайта очилди: {escape_html_text(lead_id)} | {escape_html_text(actor_name)}")
    await notify_agents_about_lead(lead_id)


@dp.callback_query(F.data.startswith("lead_done:"))
async def callback_done_lead(callback: CallbackQuery):
    tg_id = callback.from_user.id
    role = get_role(tg_id)

    if role not in ("agent", "admin"):
        await callback.answer("Сизда рухсат йўқ", show_alert=True)
        return

    lead_id = callback.data.split(":", 1)[1]
    actor_name = user_full_name(callback.from_user)

    lead = get_lead_by_id(lead_id)
    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = safe_int(lead.get("assigned_to_tg_id"))
    if role != "admin" and assigned_to != tg_id:
        await callback.answer("Фақат бириктирилган агент якунлай олади", show_alert=True)
        return

    async with LEAD_LOCK:
        ok, msg = finish_lead(lead_id, actor_name, tg_id)

    if not ok:
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer("Лид якунланди")
    await safe_send(tg_id, f"🏁 Лид <b>{escape_html_text(lead_id)}</b> якунланди")
    await notify_admins(f"🏁 Лид якунланди: {escape_html_text(lead_id)} | {escape_html_text(actor_name)}")


# =========================================================
# ADMIN FLOW
# =========================================================
@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(build_stats_text())


@dp.message(F.text == "📋 Очиқ лидлар")
async def admin_open_leads(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(build_open_leads_text())


@dp.message(F.text == "👤 Агент қўшиш")
async def admin_add_agent_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(AddAgentForm.waiting_tg_id)
    await message.answer("Янги агентнинг Telegram ID рақамини юборинг:")


@dp.message(AddAgentForm.waiting_tg_id)
async def admin_add_agent_tg_id(message: Message, state: FSMContext):
    tg_id = safe_int(message.text)
    if not tg_id:
        await message.answer("TG ID рақам бўлиши керак. Қайта юборинг:")
        return

    await state.update_data(agent_tg_id=tg_id)
    await state.set_state(AddAgentForm.waiting_full_name)
    await message.answer("Агент ФИШ ни юборинг:")


@dp.message(AddAgentForm.waiting_full_name)
async def admin_add_agent_full_name(message: Message, state: FSMContext):
    full_name = clean_text(message.text)
    if not full_name:
        await message.answer("ФИШ бўш бўлмаслиги керак. Қайта юборинг:")
        return

    await state.update_data(agent_full_name=full_name)
    await state.set_state(AddAgentForm.waiting_phone)
    await message.answer("Агент телефон рақамини юборинг:")


@dp.message(AddAgentForm.waiting_phone)
async def admin_add_agent_phone(message: Message, state: FSMContext):
    phone = normalize_phone(message.text or "")
    if not is_valid_phone(phone):
        await message.answer("Телефон рақам нотўғри. Қайта юборинг:")
        return

    data = await state.get_data()

    async with AGENT_LOCK:
        add_or_update_agent(
            tg_id=data["agent_tg_id"],
            full_name=data["agent_full_name"],
            phone=phone,
        )

    await state.clear()
    await message.answer("✅ Агент сақланди", reply_markup=admin_menu())


# =========================================================
# FALLBACKS
# =========================================================
@dp.message()
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        return

    role = get_role(message.from_user.id)

    if role == "admin":
        await message.answer("Админ менюдан керакли бўлимни танланг.", reply_markup=admin_menu())
        return

    if role == "agent":
        await message.answer("Сиз агентсиз. Янги лидлар шу ерга тушади.")
        return

    await message.answer("Хизмат турини танланг:", reply_markup=client_menu())


# =========================================================
# WEBHOOK
# =========================================================
async def on_startup():
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"Webhook set: {WEBHOOK_URL}")


async def on_shutdown():
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    finally:
        await bot.session.close()
    logger.info("Bot stopped")


async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.exception(f"Webhook processing error: {e}")
    return web.Response(text="ok")


async def healthcheck(request: web.Request):
    return web.Response(text="ok")


def create_app():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    async def startup_handler(app):
        await on_startup()

    async def shutdown_handler(app):
        await on_shutdown()

    app.on_startup.append(startup_handler)
    app.on_shutdown.append(shutdown_handler)
    return app


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=port)
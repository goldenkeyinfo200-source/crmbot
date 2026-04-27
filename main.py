import os
import json
import re
import html
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import gspread
from aiohttp import web
from aiogram import Bot, Dispatcher, F
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
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

LEAD_LOCK = asyncio.Lock()
AGENT_LOCK = asyncio.Lock()

BOT_USERNAME_CACHE = None


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
    "🏢 Янги дом ипотека": "new_building_mortgage",
    "📑 Нотариус хизмати": "notary_service",
    "🗂 Кадастр хизмати": "cadastre_service",
}

PURPOSE_LABELS = {
    "sell": "Уй сотиш",
    "buy": "Уй сотиб олиш",
    "rent_out": "Ижарага бериш",
    "rent_in": "Ижарага олиш",
    "mortgage_service": "Ипотека хизмати",
    "new_building_mortgage": "Янги дом ипотека",
    "notary_service": "Нотариус хизмати",
    "cadastre_service": "Кадастр хизмати",
}

ADMIN_PURPOSE_BUTTONS = {
    "🏠 Сотиш": "sell",
    "🔎 Сотиб олиш": "buy",
    "🏘 Ижарага бериш": "rent_out",
    "🔑 Ижарага олиш": "rent_in",
    "🏦 Ипотека хизмати": "mortgage_service",
    "🏢 Янги дом ипотека": "new_building_mortgage",
    "📑 Нотариус хизмати": "notary_service",
    "🗂 Кадастр хизмати": "cadastre_service",
}

LEAD_STATUS_NEW = "new"
LEAD_STATUS_TAKEN = "taken"
LEAD_STATUS_IN_PROGRESS = "in_progress"
LEAD_STATUS_DONE = "done"

BACK_TEXT = "🔙 Орқага"
# LEAD CONTROL
AGENT_REMINDER_2H = 2
AGENT_REMINDER_6H = 6
ADMIN_ALERT_24H = 24
AUTO_REOPEN_30H = 30

MARK_2H = "agent_reminder_2h_sent"
MARK_6H = "agent_reminder_6h_sent"
MARK_24H = "admin_alert_24h_sent"
MARK_AUTO = "auto_reopened_30h"

CONTROL_INTERVAL = 1800


# =========================================================
# STATES
# =========================================================
class LeadForm(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_property_id = State()
    waiting_description = State()


class AddAgentForm(StatesGroup):
    waiting_tg_id = State()
    waiting_full_name = State()
    waiting_phone = State()


class AdminManualLeadForm(StatesGroup):
    waiting_client_name = State()
    waiting_client_phone = State()
    waiting_purpose = State()
    waiting_property_id = State()
    waiting_description = State()


# =========================================================
# HELPERS
# =========================================================
UZ_TZ = ZoneInfo("Asia/Tashkent")

def now_str() -> str:
    return datetime.now(UZ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=UZ_TZ)
    except Exception:
        return None


def clean_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


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

    digits = re.sub(r"\D", "", raw)

    if not digits.startswith("998"):
        digits = "998" + digits

    return "+" + digits


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


LEADS_HEADERS_CACHE = None
AGENTS_HEADERS_CACHE = None

def headers_map(ws) -> Dict[str, int]:
    global LEADS_HEADERS_CACHE, AGENTS_HEADERS_CACHE

    if ws == leads_ws:
        if LEADS_HEADERS_CACHE is None:
            LEADS_HEADERS_CACHE = {
                h: i + 1 for i, h in enumerate(ws.row_values(1))
            }
        return LEADS_HEADERS_CACHE

    if ws == agents_ws:
        if AGENTS_HEADERS_CACHE is None:
            AGENTS_HEADERS_CACHE = {
                h: i + 1 for i, h in enumerate(ws.row_values(1))
            }
        return AGENTS_HEADERS_CACHE

    return {h: i + 1 for i, h in enumerate(ws.row_values(1))}


def is_cancel_text(text: str) -> bool:
    text = clean_text(text).lower()
    return text in {"cancel", "/cancel", "бекор", "❌ бекор"}


def is_back_text(text: str) -> bool:
    return clean_text(text) == BACK_TEXT


async def ensure_admin_state(message: Message, state: FSMContext) -> bool:
    if not is_admin(message.from_user.id):
        await state.clear()
        return False
    return True


async def get_bot_username() -> str:
    global BOT_USERNAME_CACHE
    if BOT_USERNAME_CACHE:
        return BOT_USERNAME_CACHE
    me = await bot.get_me()
    BOT_USERNAME_CACHE = me.username or ""
    return BOT_USERNAME_CACHE


def build_special_start_token(agent_tg_id: int) -> str:
    return f"sa{agent_tg_id}"


def parse_special_start_token(token: str) -> Optional[int]:
    token = clean_text(token)
    if not token.startswith("sa"):
        return None
    digits = token[2:]
    if not digits.isdigit():
        return None
    return int(digits)


def make_special_source(agent_tg_id: int, agent_name: str) -> str:
    safe_name = clean_text(agent_name).replace("|", "/")
    return f"special_agent:{agent_tg_id}:{safe_name}"


def extract_special_agent_meta(lead: Dict) -> Tuple[Optional[int], str]:
    source = clean_text(lead.get("source"))
    if source.startswith("special_agent:"):
        parts = source.split(":", 2)
        if len(parts) >= 3:
            return safe_int(parts[1]), clean_text(parts[2])
        if len(parts) == 2:
            return safe_int(parts[1]), ""
    return None, ""


async def clear_preserve_special_context(state: FSMContext):
    data = await state.get_data()
    special_referrer_tg_id = data.get("special_referrer_tg_id")
    special_referrer_name = data.get("special_referrer_name")
    await state.clear()
    if special_referrer_tg_id:
        await state.update_data(
            special_referrer_tg_id=special_referrer_tg_id,
            special_referrer_name=special_referrer_name or "",
        )


def agent_can_receive_purpose(agent_row: Dict, purpose_code: str) -> bool:
    allowed = clean_text(agent_row.get("allowed_purposes")).lower()

    # 🔥 ЭНГ МУҲИМ ФИКС
    if not allowed:
        return True

    if allowed == "all":
        return True

    allowed_list = [x.strip().lower() for x in allowed.split(",") if x.strip()]
    return purpose_code.strip().lower() in allowed_list

def hours_passed(dt: datetime) -> float:
    return (datetime.now(UZ_TZ) - dt).total_seconds() / 3600


def note_has(notes: str, mark: str) -> bool:
    return mark in clean_text(notes)


# =========================================================
# KEYBOARDS
# =========================================================
def client_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🏠 Уйимни сотмоқчиман"),
                KeyboardButton(text="🔎 Уй сотиб олмоқчиман"),
            ],
            [
                KeyboardButton(text="🏘 Ижарага уй бермоқчиман"),
                KeyboardButton(text="🔑 Ижарага уй олмоқчиман"),
            ],
            [
                KeyboardButton(text="🏦 Ипотека хизматидан фойдаланиш"),
                KeyboardButton(text="🏢 Янги дом ипотека"),
            ],
            [
                KeyboardButton(text="📑 Нотариус хизмати"),
                KeyboardButton(text="🗂 Кадастр хизмати"),
            ],
        ],
        resize_keyboard=True,
        input_field_placeholder="Хизматни танланг...",
    )


def ask_phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📞 Телефон рақамни юбориш", request_contact=True)],
            [KeyboardButton(text=BACK_TEXT)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Телефон рақамни юборинг...",
    )


def admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📋 Очиқ лидлар")],
            [KeyboardButton(text="👤 Агент қўшиш"), KeyboardButton(text="➕ Клиент номидан лид")],
            [KeyboardButton(text="🔗 Махсус агент линк")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Админ меню",
    )


def agent_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Махсус агент линк")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Агент меню",
    )


def admin_manual_purpose_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Сотиш"), KeyboardButton(text="🔎 Сотиб олиш")],
            [KeyboardButton(text="🏘 Ижарага бериш"), KeyboardButton(text="🔑 Ижарага олиш")],
            [KeyboardButton(text="🏦 Ипотека хизмати"), KeyboardButton(text="🏢 Янги дом ипотека")],
            [KeyboardButton(text="📑 Нотариус хизмати"), KeyboardButton(text="🗂 Кадастр хизмати")],
            [KeyboardButton(text=BACK_TEXT)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Лид турини танланг...",
    )


def only_back_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BACK_TEXT)]],
        resize_keyboard=True,
        input_field_placeholder="Орқага қайтиш мумкин",
    )


def lead_action_kb(lead_id: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Олдим", callback_data=f"lead_take:{lead_id}"),
                InlineKeyboardButton(text="❌ Рад этдим", callback_data=f"lead_reject:{lead_id}"),
            ],
            [
                InlineKeyboardButton(text="🟡 Жараёнда", callback_data=f"lead_progress:{lead_id}")
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
        str(tg_id),
        full_name,
        phone,
        "",
        "agent",
        "yes",
        "yes",
        "no",
        now_str(),
        "",
        "all",
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
        lead_id,
        now_str(),
        data.get("purpose", ""),
        data.get("property_id", ""),
        str(data.get("client_tg_id", "")),
        data.get("client_name", ""),
        data.get("client_phone", ""),
        data.get("client_username", ""),
        data.get("lead_text", ""),
        LEAD_STATUS_NEW,
        "",
        "",
        "",
        "",
        "",
        data.get("source", "bot"),
        "",
        data.get("notes", ""),
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
# MESSAGE MAP HELPERS
# =========================================================
def load_message_map(lead: Dict) -> Dict[str, Dict]:
    raw = clean_text(lead.get("group_message_id"))
    if not raw:
        return {}

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def save_message_map(lead_id: str, message_map: Dict[str, Dict]) -> bool:
    try:
        return update_lead_fields(
            lead_id,
            {
                "group_message_id": json.dumps(message_map, ensure_ascii=False),
            },
        )
    except Exception as e:
        logger.exception(f"save_message_map error for {lead_id}: {e}")
        return False


def remember_sent_message(lead_id: str, chat_id: int, message_id: int, kind: str):
    lead = get_lead_by_id(lead_id)
    if not lead:
        return

    message_map = load_message_map(lead)
    message_map[str(chat_id)] = {
        "message_id": message_id,
        "kind": kind,
    }
    save_message_map(lead_id, message_map)


async def edit_saved_lead_messages(lead_id: str, remove_buttons: bool = False):
    lead = get_lead_by_id(lead_id)
    if not lead:
        return

    message_map = load_message_map(lead)
    if not message_map:
        return

    for chat_id_str, meta in message_map.items():
        try:
            chat_id = int(chat_id_str)
            message_id = int(meta.get("message_id"))
            kind = clean_text(meta.get("kind"))

            latest_lead = get_lead_by_id(lead_id)
            if not latest_lead:
                continue

            if kind == "admin":
                new_text = format_lead_for_admins(latest_lead)
            else:
                new_text = format_lead_for_agents(latest_lead)

            reply_markup = None if remove_buttons else lead_action_kb(lead_id)

            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=new_text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        except Exception as e:
            logger.info(f"edit_saved_lead_messages skip chat={chat_id_str}: {e}")


async def remove_buttons_from_other_agents(lead_id: str, except_chat_id: int):
    lead = get_lead_by_id(lead_id)
    if not lead:
        return

    message_map = load_message_map(lead)
    if not message_map:
        return

    for chat_id_str, meta in message_map.items():
        try:
            chat_id = int(chat_id_str)
            if chat_id == except_chat_id:
                continue

            message_id = int(meta.get("message_id"))

            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=None,
            )
        except Exception as e:
            logger.info(f"remove_buttons_from_other_agents skip chat={chat_id_str}: {e}")


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
    assigned_to = escape_html_text(clean_text(lead.get("assigned_to_name")))
    result = escape_html_text(clean_text(lead.get("result")))
    special_agent_tg_id, special_agent_name = extract_special_agent_meta(lead)

    parts = [
        "🆕 <b>Янги лид агент учун</b>",
        "",
        f"<b>Лид ID:</b> {lead_id}",
        f"<b>Мақсад:</b> {purpose}",
        f"<b>Мижоз:</b> {client_name}",
        f"<b>Телефон:</b> {client_phone}",
    ]

    if client_username:
        parts.append(f"<b>Username:</b> {client_username}")
    if property_id:
        parts.append(f"<b>Property ID:</b> {property_id}")
    if lead_text:
        parts.append(f"<b>Мижоз изоҳи:</b> {lead_text}")
    if special_agent_tg_id:
        parts.append(
            f"<b>Махсус агент:</b> {escape_html_text(special_agent_name or str(special_agent_tg_id))}"
        )

    parts.append(f"<b>Ҳолат:</b> {status}")

    if assigned_to:
        parts.append(f"<b>Бириктирилган:</b> {assigned_to}")
    if result:
        parts.append(f"<b>Натижа:</b> {result}")

    if clean_text(lead.get("lead_status")) == LEAD_STATUS_DONE:
        parts.append("")
        parts.append("🏁 <b>Лид якунланган</b>")
    elif clean_text(lead.get("lead_status")) == LEAD_STATUS_TAKEN:
        parts.append("")
        parts.append("📌 <b>Бу лид олинган</b>")
    else:
        parts.append("")
        if special_agent_tg_id:
            parts.append("🔒 <b>Бу лид махсус агент канали орқали келган.</b>")
        else:
            parts.append("Белгиланган агентлардан бири ушбу лидни олади.")

    return "\n".join(parts)


def format_lead_for_admins(lead: Dict) -> str:
    lead_id = escape_html_text(clean_text(lead.get("lead_id")))
    created_at = escape_html_text(clean_text(lead.get("created_at")))
    purpose = escape_html_text(purpose_label(clean_text(lead.get("purpose"))))
    purpose_code = escape_html_text(clean_text(lead.get("purpose")))
    client_name = escape_html_text(clean_text(lead.get("client_name")))
    client_phone = escape_html_text(clean_text(lead.get("client_phone")))
    client_username = escape_html_text(clean_text(lead.get("client_username")))
    property_id = escape_html_text(clean_text(lead.get("property_id")))
    lead_text = escape_html_text(clean_text(lead.get("lead_text")))
    status = escape_html_text(clean_text(lead.get("lead_status")).upper())
    client_tg_id = escape_html_text(clean_text(str(lead.get("client_tg_id", ""))))
    source = escape_html_text(clean_text(lead.get("source")))
    assigned_to = escape_html_text(clean_text(lead.get("assigned_to_name")))
    result = escape_html_text(clean_text(lead.get("result")))
    special_agent_tg_id, special_agent_name = extract_special_agent_meta(lead)

    parts = [
        "🛎 <b>Админга янги лид</b>",
        "",
        f"<b>Лид ID:</b> {lead_id}",
        f"<b>Яратилган вақт:</b> {created_at}",
        f"<b>Мақсад:</b> {purpose}",
        f"<b>Код:</b> {purpose_code}",
        f"<b>Мижоз:</b> {client_name}",
        f"<b>Телефон:</b> {client_phone}",
        f"<b>Client TG ID:</b> {client_tg_id or 'manual'}",
    ]

    if client_username:
        parts.append(f"<b>Username:</b> {client_username}")
    if property_id:
        parts.append(f"<b>Property ID:</b> {property_id}")
    if lead_text:
        parts.append(f"<b>Тўлиқ изоҳ:</b> {lead_text}")
    if special_agent_tg_id:
        parts.append(
            f"<b>Махсус агент:</b> {escape_html_text(special_agent_name or str(special_agent_tg_id))} ({special_agent_tg_id})"
        )

    parts.append(f"<b>Манба:</b> {source or 'bot'}")
    parts.append(f"<b>Ҳолат:</b> {status}")

    if assigned_to:
        parts.append(f"<b>Бириктирилган:</b> {assigned_to}")
    if result:
        parts.append(f"<b>Натижа:</b> {result}")

    if clean_text(lead.get("lead_status")) == LEAD_STATUS_DONE:
        parts.append("")
        parts.append("🏁 <b>Лид якунланган</b>")
    elif clean_text(lead.get("lead_status")) == LEAD_STATUS_TAKEN:
        parts.append("")
        parts.append("📌 <b>Лид агентга бириктирилган</b>")
    else:
        parts.append("")
        parts.append("Админ ҳам ушбу лидни бошқариши мумкин.")

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
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
        logger.info(f"Message sent to {chat_id}: {msg.message_id}")
        return msg
    except Exception as e:
        logger.exception(f"Send error chat_id={chat_id}: {e}")
        return None


async def notify_agents_about_lead(lead_id: str):
    lead = get_lead_by_id(lead_id)
    if not lead:
        logger.info(f"Lead not found for agents: {lead_id}")
        return

    text = format_lead_for_agents(lead)
    sent_ids = set()

    special_agent_tg_id, _ = extract_special_agent_meta(lead)
    purpose_code = clean_text(lead.get("purpose"))

    if special_agent_tg_id and is_agent(special_agent_tg_id):
        special_agent_row = get_agent_by_tg_id(special_agent_tg_id)

        if (
            special_agent_row
            and clean_text(special_agent_row.get("is_active")).lower() == "yes"
            and clean_text(special_agent_row.get("can_take_leads")).lower() == "yes"
        ):
            msg = await safe_send(
                special_agent_tg_id,
                text,
                reply_markup=lead_action_kb(lead_id),
            )
            if msg:
                remember_sent_message(lead_id, special_agent_tg_id, msg.message_id, "agent")
                sent_ids.add(special_agent_tg_id)

        logger.info(f"Special agent notification done for {lead_id}, sent={len(sent_ids)}")
        return

    for agent in get_agents_records():
        tg_id = safe_int(agent.get("tg_id"))
        role = clean_text(agent.get("role")).lower()
        is_active = clean_text(agent.get("is_active")).lower()
        can_take = clean_text(agent.get("can_take_leads")).lower()

        if not tg_id:
            continue
        if tg_id in sent_ids:
            continue
        if role != "agent":
            continue
        if is_active != "yes":
            continue
        if can_take != "yes":
            continue
        if not agent_can_receive_purpose(agent, purpose_code):
            continue

        sent_ids.add(tg_id)
        msg = await safe_send(
            tg_id,
            text,
            reply_markup=lead_action_kb(lead_id),
        )
        if msg:
            remember_sent_message(lead_id, tg_id, msg.message_id, "agent")

    logger.info(
        f"Filtered agent notifications done for {lead_id}, purpose={purpose_code}, sent={len(sent_ids)}"
    )


async def notify_admins_about_lead(lead_id: str):
    lead = get_lead_by_id(lead_id)
    if not lead:
        logger.info(f"Lead not found for admins: {lead_id}")
        return

    admin_ids = set()

    for admin_id in ADMINS:
        if admin_id:
            admin_ids.add(int(admin_id))

    for row in get_agents_records():
        tg_id = safe_int(row.get("tg_id"))
        role = clean_text(row.get("role")).lower()
        is_active = clean_text(row.get("is_active")).lower()

        if not tg_id:
            continue
        if role == "admin" and is_active == "yes":
            admin_ids.add(tg_id)

    text = format_lead_for_admins(lead)

    for admin_id in admin_ids:
        msg = await safe_send(
            admin_id,
            text,
            reply_markup=lead_action_kb(lead_id),
        )
        if msg:
            remember_sent_message(lead_id, admin_id, msg.message_id, "admin")

    logger.info(f"Admin notifications done for {lead_id}, sent={len(admin_ids)}")


async def notify_admins_simple(text: str):
    admin_ids = set()

    for admin_id in ADMINS:
        if admin_id:
            admin_ids.add(int(admin_id))

    for row in get_agents_records():
        tg_id = safe_int(row.get("tg_id"))
        role = clean_text(row.get("role")).lower()
        is_active = clean_text(row.get("is_active")).lower()

        if not tg_id:
            continue
        if role == "admin" and is_active == "yes":
            admin_ids.add(tg_id)

    for admin_id in admin_ids:
        await safe_send(admin_id, text)

    logger.info(f"Simple admin notification sent={len(admin_ids)}")


async def notify_special_agent_bonus_if_needed(lead_id: str):
    lead = get_lead_by_id(lead_id)
    if not lead:
        return

    special_agent_tg_id, _ = extract_special_agent_meta(lead)
    if not special_agent_tg_id:
        return

    assigned_to_name = clean_text(lead.get("assigned_to_name"))
    client_name = clean_text(lead.get("client_name"))
    purpose = purpose_label(clean_text(lead.get("purpose")))

    text = (
        "🎉 <b>Махсус агент бонуси</b>\n\n"
        f"<b>Лид ID:</b> {escape_html_text(clean_text(lead.get('lead_id')))}\n"
        f"<b>Мижоз:</b> {escape_html_text(client_name)}\n"
        f"<b>Мақсад:</b> {escape_html_text(purpose)}\n"
        f"<b>Ишни якунлаган:</b> {escape_html_text(assigned_to_name or '—')}\n\n"
        "✅ Мижозингизнинг иши якунланди.\n"
        "Бонусингизни офисдан олиб кетишингиз мумкин."
    )

    await safe_send(special_agent_tg_id, text)

async def process_lead_control_once():
    leads = get_leads_records()

    for lead in leads:
        try:
            lead_id = clean_text(lead.get("lead_id"))
            status = clean_text(lead.get("lead_status"))
            taken_at = parse_dt(clean_text(lead.get("taken_at")))
            notes = clean_text(lead.get("notes"))
            agent_tg_id = safe_int(lead.get("assigned_to_tg_id"))

            if status not in (LEAD_STATUS_TAKEN, LEAD_STATUS_IN_PROGRESS):
                continue

            if not taken_at or not agent_tg_id:
                continue

            passed = hours_passed(taken_at)

            if passed >= AUTO_REOPEN_30H and not note_has(notes, MARK_AUTO):
                update_lead_fields(lead_id, {
                    "lead_status": LEAD_STATUS_NEW,
                    "assigned_to_tg_id": "",
                    "assigned_to_name": "",
                    "taken_at": "",
                    "result": "auto_reopened",
                    "notes": build_lead_note(notes, f"{now_str()} | {MARK_AUTO}")
                })

                await safe_send(agent_tg_id, f"🔄 Лид {lead_id} сиздан олинди")
                await notify_agents_about_lead(lead_id)
                await notify_admins_about_lead(lead_id)
                await edit_saved_lead_messages(lead_id, remove_buttons=False)

            elif passed >= ADMIN_ALERT_24H and not note_has(notes, MARK_24H):
                await notify_admins_simple(f"🚨 Лид {lead_id} 24 соат бўлди")
                update_lead_fields(lead_id, {
                    "notes": build_lead_note(notes, f"{now_str()} | {MARK_24H}")
                })

            elif passed >= AGENT_REMINDER_6H and not note_has(notes, MARK_6H):
                await safe_send(agent_tg_id, f"⚠️ Лид {lead_id} 6 соат бўлди")
                update_lead_fields(lead_id, {
                    "notes": build_lead_note(notes, f"{now_str()} | {MARK_6H}")
                })

            elif passed >= AGENT_REMINDER_2H and not note_has(notes, MARK_2H):
                await safe_send(agent_tg_id, f"⏰ Лид {lead_id} 2 соат бўлди")
                update_lead_fields(lead_id, {
                    "notes": build_lead_note(notes, f"{now_str()} | {MARK_2H}")
                })

        except Exception as e:
            logger.exception(f"Lead control error: {e}")

async def lead_control_worker():
    logger.info("Lead control worker started")

    while True:
        try:
            await process_lead_control_once()
        except Exception as e:
            logger.exception(f"Lead control worker error: {e}")

        await asyncio.sleep(CONTROL_INTERVAL)


# =========================================================
# STATS
# =========================================================
def build_stats_text() -> str:
    leads = get_leads_records()
    now = datetime.now(UZ_TZ)
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
# NAVIGATION HELPERS
# =========================================================
async def reset_to_role_menu(message: Message, state: FSMContext):
    await clear_preserve_special_context(state)
    role = get_role(message.from_user.id)

    if role == "admin":
        await message.answer("✅ Бекор қилинди.", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
    elif role == "agent":
        await message.answer("✅ Бекор қилинди.", reply_markup=agent_menu(), parse_mode=ParseMode.HTML)
    else:
        await message.answer("✅ Бекор қилинди.", reply_markup=client_menu(), parse_mode=ParseMode.HTML)


async def ask_next_step_after_phone(message: Message, state: FSMContext):
    data = await state.get_data()
    purpose = data.get("purpose")

    if purpose == "buy":
        await message.answer(
            "Каналда кўрган уй ID рақамини юборинг:",
            reply_markup=only_back_kb(),
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(LeadForm.waiting_property_id)
    else:
        await message.answer(
            "Маълумотни батафсил ёзиб юборинг:",
            reply_markup=only_back_kb(),
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(LeadForm.waiting_description)


async def process_phone_input(message: Message, state: FSMContext, phone: str):
    phone = normalize_phone(phone)
    if not is_valid_phone(phone):
        await message.answer(
            "❌ Телефон нотўғри. Масалан: +998901234567",
            parse_mode=ParseMode.HTML,
        )
        return

    await state.update_data(client_phone=phone)
    await ask_next_step_after_phone(message, state)


# =========================================================
# START / MENUS
# =========================================================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    text = clean_text(message.text)
    args = ""
    if " " in text:
        args = text.split(" ", 1)[1].strip()

    special_agent_tg_id = parse_special_start_token(args)
    if special_agent_tg_id and get_role(message.from_user.id) == "client":
        ref_agent = get_agent_by_tg_id(special_agent_tg_id)
        if ref_agent and clean_text(ref_agent.get("is_active")).lower() == "yes":
            await state.update_data(
                special_referrer_tg_id=special_agent_tg_id,
                special_referrer_name=clean_text(ref_agent.get("full_name")),
            )
            await message.answer(
                f"✅ Сиз махсус агент орқали кирдингиз.\n"
                f"<b>Агент:</b> {escape_html_text(clean_text(ref_agent.get('full_name')))}\n\n"
                f"Энди хизмат турини танланг:",
                reply_markup=client_menu(),
                parse_mode=ParseMode.HTML,
            )
            return

    await clear_preserve_special_context(state)

    role = get_role(message.from_user.id)

    if role == "admin":
        await message.answer("Сиз админсиз.", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
        return

    if role == "agent":
        await message.answer(
            "Сиз агентсиз. Янги лидлар шу ерга тушади.",
            reply_markup=agent_menu(),
            parse_mode=ParseMode.HTML,
        )
        return

    await message.answer("Хизмат турини танланг:", reply_markup=client_menu(), parse_mode=ParseMode.HTML)


@dp.message(Command("admin"))
async def admin_command(message: Message, state: FSMContext):
    await clear_preserve_special_context(state)
    if not is_admin(message.from_user.id):
        return
    await message.answer("Админ меню:", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)


@dp.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    await reset_to_role_menu(message, state)


# =========================================================
# SPECIAL AGENT LINK
# =========================================================
@dp.message(F.text == "🔗 Махсус агент линк")
async def special_agent_link_handler(message: Message):
    role = get_role(message.from_user.id)
    if role not in ("agent", "admin"):
        return

    bot_username = await get_bot_username()
    agent_row = get_agent_by_tg_id(message.from_user.id)
    agent_name = clean_text(agent_row.get("full_name")) if agent_row else user_full_name(message.from_user)
    token = build_special_start_token(message.from_user.id)
    link = f"https://t.me/{bot_username}?start={token}"

    text = (
        "🔗 <b>Сизнинг махсус агент линкингиз</b>\n\n"
        f"<b>Агент:</b> {escape_html_text(agent_name)}\n"
        f"<b>Линк:</b>\n{escape_html_text(link)}\n\n"
        "Бу линкни мижозга юборинг.\n"
        "Мижоз шу линк орқали кирса, лид тўғридан-тўғри сизга боғланади."
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


# =========================================================
# CLIENT FLOW
# =========================================================
@dp.message(F.text.in_(list(PURPOSE_MAP.keys())))
async def client_choose_purpose(message: Message, state: FSMContext):
    if get_role(message.from_user.id) != "client":
        return

    current_data = await state.get_data()
    special_referrer_tg_id = current_data.get("special_referrer_tg_id")
    special_referrer_name = current_data.get("special_referrer_name")

    purpose = PURPOSE_MAP[message.text]
    await state.clear()

    if special_referrer_tg_id:
        await state.update_data(
            special_referrer_tg_id=special_referrer_tg_id,
            special_referrer_name=special_referrer_name,
        )

    await state.update_data(purpose=purpose)

    if purpose in ("notary_service", "cadastre_service"):
        await message.answer(
            "Исм-фамилиянгизни юборинг:",
            reply_markup=only_back_kb(),
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(LeadForm.waiting_name)
        return

    await message.answer(
        "Телефон рақамингизни юборинг ёки қўлда ёзинг:",
        reply_markup=ask_phone_kb(),
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(LeadForm.waiting_phone)


@dp.message(LeadForm.waiting_name)
async def lead_name_input(message: Message, state: FSMContext):
    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await clear_preserve_special_context(state)
        await message.answer(
            "Хизмат турини танланг:",
            reply_markup=client_menu(),
            parse_mode=ParseMode.HTML,
        )
        return

    if len(text) < 3:
        await message.answer("❌ Исм жуда қисқа. Қайта киритинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(custom_client_name=text)

    await message.answer(
        "Телефон рақамингизни юборинг ёки қўлда ёзинг:",
        reply_markup=ask_phone_kb(),
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(LeadForm.waiting_phone)


@dp.message(LeadForm.waiting_phone, F.contact)
async def lead_phone_contact(message: Message, state: FSMContext):
    await process_phone_input(message, state, clean_text(message.contact.phone_number))


@dp.message(LeadForm.waiting_phone)
async def lead_phone_text(message: Message, state: FSMContext):
    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        data = await state.get_data()
        purpose = data.get("purpose")

        if purpose in ("notary_service", "cadastre_service"):
            await message.answer(
                "Исм-фамилиянгизни юборинг:",
                reply_markup=only_back_kb(),
                parse_mode=ParseMode.HTML,
            )
            await state.set_state(LeadForm.waiting_name)
            return

        await clear_preserve_special_context(state)
        await message.answer("Хизмат турини танланг:", reply_markup=client_menu(), parse_mode=ParseMode.HTML)
        return

    await process_phone_input(message, state, text)


@dp.message(LeadForm.waiting_property_id)
async def lead_property_id(message: Message, state: FSMContext):
    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        data = await state.get_data()
        purpose = data.get("purpose")
        await message.answer(
            "Телефон рақамингизни юборинг ёки қўлда ёзинг:",
            reply_markup=ask_phone_kb(),
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(LeadForm.waiting_phone)
        await state.update_data(purpose=purpose, property_id="")
        return

    if not text:
        await message.answer("ID рақамни қайта юборинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(property_id=text)
    await message.answer("Изоҳингизни ёзинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)
    await state.set_state(LeadForm.waiting_description)


@dp.message(LeadForm.waiting_description)
async def lead_description(message: Message, state: FSMContext):
    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        data = await state.get_data()
        purpose = data.get("purpose")

        if purpose == "buy":
            await message.answer(
                "Каналда кўрган уй ID рақамини юборинг:",
                reply_markup=only_back_kb(),
                parse_mode=ParseMode.HTML,
            )
            await state.set_state(LeadForm.waiting_property_id)
        else:
            await message.answer(
                "Телефон рақамингизни юборинг ёки қўлда ёзинг:",
                reply_markup=ask_phone_kb(),
                parse_mode=ParseMode.HTML,
            )
            await state.set_state(LeadForm.waiting_phone)
        return

    if len(text) < 3:
        await message.answer("❌ Изоҳ жуда қисқа. Қайта ёзинг:", parse_mode=ParseMode.HTML)
        return

    data = await state.get_data()
    special_referrer_tg_id = data.get("special_referrer_tg_id")
    special_referrer_name = clean_text(data.get("special_referrer_name"))

    lead_source = "bot"
    lead_notes = ""

    if special_referrer_tg_id:
        lead_source = make_special_source(special_referrer_tg_id, special_referrer_name)
        lead_notes = f"{now_str()} | special_agent_referral by {special_referrer_name} ({special_referrer_tg_id})"

    lead_payload = {
        "purpose": data.get("purpose", ""),
        "property_id": data.get("property_id", ""),
        "client_tg_id": message.from_user.id,
        "client_name": data.get("custom_client_name") or user_full_name(message.from_user),
        "client_phone": data.get("client_phone", ""),
        "client_username": username_text(message.from_user),
        "lead_text": text,
        "source": lead_source,
        "notes": lead_notes,
    }

    async with LEAD_LOCK:
        lead_id = create_lead(lead_payload)

    await clear_preserve_special_context(state)

    if special_referrer_tg_id:
        answer_text = (
            f"✅ Аризангиз қабул қилинди.\n"
            f"Лид ID: <b>{escape_html_text(lead_id)}</b>\n"
            f"Сиз билан махсус агент тез орада боғланади."
        )
    else:
        answer_text = (
            f"✅ Аризангиз қабул қилинди.\n"
            f"Лид ID: <b>{escape_html_text(lead_id)}</b>\n"
            f"Тез орада сиз билан боғланишади."
        )

    await message.answer(
        answer_text,
        reply_markup=client_menu(),
        parse_mode=ParseMode.HTML,
    )

    await notify_agents_about_lead(lead_id)
    await notify_admins_about_lead(lead_id)


# =========================================================
# ADMIN MANUAL LEAD
# =========================================================
@dp.message(F.text == "➕ Клиент номидан лид")
async def admin_manual_lead_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await clear_preserve_special_context(state)
    await state.set_state(AdminManualLeadForm.waiting_client_name)
    await message.answer(
        "Клиент исм-фамилиясини юборинг:",
        reply_markup=only_back_kb(),
        parse_mode=ParseMode.HTML,
    )


@dp.message(AdminManualLeadForm.waiting_client_name)
async def admin_manual_lead_name(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await clear_preserve_special_context(state)
        await message.answer("Админ меню:", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
        return

    if len(text) < 3:
        await message.answer("❌ Клиент исми жуда қисқа. Қайта ёзинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(client_name=text)
    await state.set_state(AdminManualLeadForm.waiting_client_phone)
    await message.answer("Клиент телефон рақамини юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)


@dp.message(AdminManualLeadForm.waiting_client_phone)
async def admin_manual_lead_phone(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await state.set_state(AdminManualLeadForm.waiting_client_name)
        await message.answer("Клиент исм-фамилиясини юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)
        return

    phone = normalize_phone(text)
    if not is_valid_phone(phone):
        await message.answer("❌ Телефон нотўғри. Масалан: +998901234567", parse_mode=ParseMode.HTML)
        return

    await state.update_data(client_phone=phone)
    await state.set_state(AdminManualLeadForm.waiting_purpose)
    await message.answer(
        "Лид мақсадини танланг:",
        reply_markup=admin_manual_purpose_kb(),
        parse_mode=ParseMode.HTML,
    )


@dp.message(AdminManualLeadForm.waiting_purpose)
async def admin_manual_lead_purpose(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_back_text(text):
        await state.set_state(AdminManualLeadForm.waiting_client_phone)
        await message.answer("Клиент телефон рақамини юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)
        return

    purpose = ADMIN_PURPOSE_BUTTONS.get(text)

    if not purpose:
        await message.answer("❌ Тугмалардан бирини танланг.", reply_markup=admin_manual_purpose_kb(), parse_mode=ParseMode.HTML)
        return

    await state.update_data(purpose=purpose)

    await state.set_state(AdminManualLeadForm.waiting_description)
    await message.answer("Клиент изоҳини ёзинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)


@dp.message(AdminManualLeadForm.waiting_property_id)
async def admin_manual_lead_property_id(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await state.set_state(AdminManualLeadForm.waiting_purpose)
        await message.answer("Лид мақсадини танланг:", reply_markup=admin_manual_purpose_kb(), parse_mode=ParseMode.HTML)
        return

    if not text:
        await message.answer("❌ Property ID бўш бўлмаслиги керак. Қайта юборинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(property_id=text)
    await state.set_state(AdminManualLeadForm.waiting_description)
    await message.answer("Клиент изоҳини ёзинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)


@dp.message(AdminManualLeadForm.waiting_description)
async def admin_manual_lead_description(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await state.set_state(AdminManualLeadForm.waiting_purpose)
        await message.answer(
            "Лид мақсадини танланг:",
            reply_markup=admin_manual_purpose_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    if len(text) < 3:
        await message.answer("❌ Изоҳ жуда қисқа. Қайта ёзинг:", parse_mode=ParseMode.HTML)
        return

    data = await state.get_data()
    admin_name = user_full_name(message.from_user)
    admin_tg_id = message.from_user.id

    lead_payload = {
        "purpose": data.get("purpose", ""),
        "property_id": data.get("property_id", ""),
        "client_tg_id": "",
        "client_name": data.get("client_name", ""),
        "client_phone": data.get("client_phone", ""),
        "client_username": "",
        "lead_text": text,
        "source": "admin_manual",
        "notes": f"{now_str()} | manually created by {admin_name} ({admin_tg_id})",
    }

    async with LEAD_LOCK:
        lead_id = create_lead(lead_payload)

    await clear_preserve_special_context(state)

    await message.answer(
        f"✅ Клиент номидан лид сақланди.\nЛид ID: <b>{escape_html_text(lead_id)}</b>",
        reply_markup=admin_menu(),
        parse_mode=ParseMode.HTML,
    )

    await notify_agents_about_lead(lead_id)
    await notify_admins_about_lead(lead_id)


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

    lead = get_lead_by_id(lead_id)
    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    special_agent_tg_id, _ = extract_special_agent_meta(lead)

    if special_agent_tg_id and role != "admin" and special_agent_tg_id != tg_id:
        await callback.answer("Бу лид махсус агентга тегишли", show_alert=True)
        return

    async with LEAD_LOCK:
        lead = get_lead_by_id(lead_id)

        if not lead:
            await callback.answer("Лид топилмади", show_alert=True)
            return

        if clean_text(lead.get("lead_status")) != LEAD_STATUS_NEW:
            await callback.answer("Бу лид аллақачон олинган", show_alert=True)
            return

        ok, msg = assign_lead_to_agent(lead_id, tg_id, actor_name)

    if not ok:
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer("Лид сизга бириктирилди")
    await safe_send(tg_id, f"✅ Лид <b>{escape_html_text(lead_id)}</b> сизга бириктирилди")

    await notify_admins_simple(
        f"✅ Лид олинди: <b>{escape_html_text(lead_id)}</b>\n"
        f"<b>Олган:</b> {escape_html_text(actor_name)}"
    )

    await remove_buttons_from_other_agents(lead_id, except_chat_id=tg_id)
    await edit_saved_lead_messages(lead_id, remove_buttons=False)


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

    await notify_admins_simple(
        f"❌ Лид қайта очилди: <b>{escape_html_text(lead_id)}</b>\n"
        f"<b>Амалга оширган:</b> {escape_html_text(actor_name)}"
    )

    await notify_agents_about_lead(lead_id)
    await notify_admins_about_lead(lead_id)


@dp.callback_query(F.data.startswith("lead_progress:"))
async def callback_progress_lead(callback: CallbackQuery):
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
        await callback.answer("Фақат бириктирилган агент босиши мумкин", show_alert=True)
        return

    async with LEAD_LOCK:
        ok = update_lead_fields(
            lead_id,
            {
                "lead_status": LEAD_STATUS_IN_PROGRESS,
                "result": LEAD_STATUS_IN_PROGRESS,
                "notes": build_lead_note(
                    clean_text(lead.get("notes")),
                    f"{now_str()} | in_progress by {actor_name} ({tg_id})"
                ),
            },
        )

    if not ok:
        await callback.answer("Хато юз берди", show_alert=True)
        return

    await callback.answer("Жараёнда деб белгиланди")
    await safe_send(tg_id, f"🟡 Лид <b>{escape_html_text(lead_id)}</b> жараёнда")

    await notify_admins_simple(
        f"🟡 Лид жараёнда: <b>{escape_html_text(lead_id)}</b>\n"
        f"<b>Агент:</b> {escape_html_text(actor_name)}"
    )

    await edit_saved_lead_messages(lead_id, remove_buttons=False)


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
        await callback.answer("Фақат бириктирилган агент якунлаши мумкин", show_alert=True)
        return

    async with LEAD_LOCK:
        ok, msg = finish_lead(lead_id, actor_name, tg_id)

    if not ok:
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer("Лид якунланди")
    await safe_send(tg_id, f"🏁 Лид <b>{escape_html_text(lead_id)}</b> якунланди")

    await notify_admins_simple(
        f"🏁 Лид якунланди: <b>{escape_html_text(lead_id)}</b>\n"
        f"<b>Якунлаган:</b> {escape_html_text(actor_name)}"
    )

    await notify_special_agent_bonus_if_needed(lead_id)
    await edit_saved_lead_messages(lead_id, remove_buttons=True)


# =========================================================
# ADMIN FLOW
# =========================================================
@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(build_stats_text(), parse_mode=ParseMode.HTML)


@dp.message(F.text == "📋 Очиқ лидлар")
async def admin_open_leads(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(build_open_leads_text(), parse_mode=ParseMode.HTML)


@dp.message(F.text == "👤 Агент қўшиш")
async def admin_add_agent_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await clear_preserve_special_context(state)
    await state.set_state(AddAgentForm.waiting_tg_id)
    await message.answer(
        "Янги агентнинг Telegram ID рақамини юборинг:",
        reply_markup=only_back_kb(),
        parse_mode=ParseMode.HTML,
    )


@dp.message(AddAgentForm.waiting_tg_id)
async def admin_add_agent_tg_id(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await clear_preserve_special_context(state)
        await message.answer("Админ меню:", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
        return

    tg_id = safe_int(text)
    if not tg_id:
        await message.answer("❌ TG ID рақам бўлиши керак. Қайта юборинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(agent_tg_id=tg_id)
    await state.set_state(AddAgentForm.waiting_full_name)
    await message.answer("Агент ФИШ ни юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)


@dp.message(AddAgentForm.waiting_full_name)
async def admin_add_agent_full_name(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await state.set_state(AddAgentForm.waiting_tg_id)
        await message.answer(
            "Янги агентнинг Telegram ID рақамини юборинг:",
            reply_markup=only_back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    if len(text) < 3:
        await message.answer("❌ ФИШ жуда қисқа. Қайта юборинг:", parse_mode=ParseMode.HTML)
        return

    await state.update_data(agent_full_name=text)
    await state.set_state(AddAgentForm.waiting_phone)
    await message.answer("Агент телефон рақамини юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)


@dp.message(AddAgentForm.waiting_phone)
async def admin_add_agent_phone(message: Message, state: FSMContext):
    if not await ensure_admin_state(message, state):
        return

    text = clean_text(message.text)

    if is_cancel_text(text):
        await reset_to_role_menu(message, state)
        return

    if is_back_text(text):
        await state.set_state(AddAgentForm.waiting_full_name)
        await message.answer("Агент ФИШ ни юборинг:", reply_markup=only_back_kb(), parse_mode=ParseMode.HTML)
        return

    phone = normalize_phone(text)
    if not is_valid_phone(phone):
        await message.answer("❌ Телефон нотўғри. Масалан: +998901234567", parse_mode=ParseMode.HTML)
        return

    data = await state.get_data()

    async with AGENT_LOCK:
        add_or_update_agent(
            tg_id=data["agent_tg_id"],
            full_name=data["agent_full_name"],
            phone=phone,
        )

    await clear_preserve_special_context(state)
    await message.answer("✅ Агент сақланди", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)


# =========================================================
# UNIVERSAL STATE GUARD
# =========================================================
@dp.message()
async def universal_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()

    if current_state:
        text = clean_text(message.text)

        if is_cancel_text(text):
            await reset_to_role_menu(message, state)
            return

        await message.answer(
            "⚠️ Сиз жараён ичидасиз.\n"
            "Тўғри маълумот киритинг, ёки /cancel юборинг.",
            parse_mode=ParseMode.HTML,
        )
        return

    role = get_role(message.from_user.id)

    if role == "admin":
        await message.answer("Админ меню:", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
        return

    if role == "agent":
        await message.answer(
            "Сиз агентсиз. Янги лидлар шу ерга тушади.",
            reply_markup=agent_menu(),
            parse_mode=ParseMode.HTML,
        )
        return

    await message.answer("Хизмат турини танланг:", reply_markup=client_menu(), parse_mode=ParseMode.HTML)


# =========================================================
# WEBHOOK
# =========================================================
async def on_startup():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Old webhook deleted")
    except Exception as e:
        logger.info(f"Old webhook delete error: {e}")

    await bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )

    info = await bot.get_webhook_info()
    logger.info(f"Webhook set result: url={info.url} pending={info.pending_update_count}")

    asyncio.create_task(lead_control_worker())
    logger.info("Lead control started")


async def on_shutdown():
    await bot.session.close()
    logger.info("Bot stopped")


async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
        logger.info(f"Incoming update: {data}")
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        logger.info("Update processed successfully")
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
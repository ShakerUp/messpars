import asyncio
import os
import sys
import json
import io
import re
import sqlite3
import logging
import zipfile
import random
from datetime import datetime, timezone, timedelta, time as dt_time
from html import escape
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.extensions import html as telethon_html
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import (
    User, Chat, Channel, MessageActionTopicCreate,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters

# ====== НАСТРОЙКА ЛОГИРОВАНИЯ ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    handlers=[
        logging.FileHandler("bot_messages.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

user_edit_state = {}

logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("telegram").setLevel(logging.CRITICAL)
logging.getLogger("telethon").setLevel(logging.CRITICAL)

# ====== КОНФИГУРАЦИЯ ======
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DEFAULT_TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID'))
ADMIN_ID = 684460638

TOPICS_DB_FILE = 'topics_mapping.json'
BOT_SETTINGS_FILE = 'bot_settings.json'
DB_FILE = 'bot_data.db'
MAX_FILE_SIZE = 50 * 1024 * 1024

LOG_FILE = "bot_messages.log"
LOG_RETENTION_DAYS = 2
LOG_EXPORT_HOURS = 24
KYIV_OFFSET = 3

# ====== BACKUP CONFIG ======
BACKUP_TIME_HOUR_KYIV = 4  # час низкой активности для ежедневного backup

# ====== PRESENCE EMULATION CONFIG ======
# Держит user-аккаунт Telethon "online" днем и переводит в "offline" ночью.
PRESENCE_ENABLED = os.getenv("PRESENCE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
PRESENCE_ONLINE_FROM_KYIV = os.getenv("PRESENCE_ONLINE_FROM_KYIV", "08:00")
PRESENCE_ONLINE_UNTIL_KYIV = os.getenv("PRESENCE_ONLINE_UNTIL_KYIV", "23:30")
PRESENCE_REFRESH_SECONDS = int(os.getenv("PRESENCE_REFRESH_SECONDS", "75"))
PRESENCE_NIGHT_CHECK_SECONDS = int(os.getenv("PRESENCE_NIGHT_CHECK_SECONDS", "600"))
PRESENCE_JITTER_SECONDS = int(os.getenv("PRESENCE_JITTER_SECONDS", "15"))

DEFAULT_BOT_SETTINGS = {
    "presence_enabled": PRESENCE_ENABLED,
    "presence_online_from": PRESENCE_ONLINE_FROM_KYIV,
    "presence_online_until": PRESENCE_ONLINE_UNTIL_KYIV,
}

# ====== БЛОКИРОВКА ДЛЯ ЗАПИСИ В КОНФИГ/БД ПРИ RESTORE ======
# Защищает от гонок при чтении-изменении-записи topics_mapping.json,
# а также используется как "стоп-кран" при восстановлении из backup.
data_lock = asyncio.Lock()

# ====== USER COLOR SYSTEM ======

USER_MARKERS = [
    "🔴", "🟠", "🟡", "🟢", "🔵", "🟣", "🟤",
    "🔹", "🔸", "🔺", "🔻", "🔷", "🔶", "💠"
]

def get_user_marker(user_id: int):
    if not user_id:
        return "🔹"
    return USER_MARKERS[user_id % len(USER_MARKERS)]

DISPLAY_MODE = "compact"

client = None
bot_app = None

SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), DEFAULT_TARGET_CHAT_ID] + SYSTEM_IDS

# ====== STYLE + LOG HELPERS ======

def get_now_kyiv():
    return datetime.now(timezone.utc) + timedelta(hours=KYIV_OFFSET)

def load_bot_settings() -> dict:
    settings = DEFAULT_BOT_SETTINGS.copy()
    if not os.path.exists(BOT_SETTINGS_FILE):
        return settings
    try:
        with open(BOT_SETTINGS_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            settings.update({k: saved[k] for k in settings.keys() if k in saved})
    except Exception as e:
        logger.warning(f"[BOT SETTINGS] Не удалось прочитать {BOT_SETTINGS_FILE}: {e}")
    settings["presence_enabled"] = bool(settings.get("presence_enabled"))
    settings["presence_online_from"] = str(settings.get("presence_online_from") or "08:00")
    settings["presence_online_until"] = str(settings.get("presence_online_until") or "23:30")
    return settings

def save_bot_settings(settings: dict):
    current = load_bot_settings()
    current.update({k: settings[k] for k in DEFAULT_BOT_SETTINGS.keys() if k in settings})
    tmp_path = f"{BOT_SETTINGS_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, BOT_SETTINGS_FILE)

def parse_hhmm(value: str, default: str) -> dt_time:
    try:
        hour_str, minute_str = value.strip().split(":", 1)
        return dt_time(hour=int(hour_str), minute=int(minute_str))
    except Exception:
        logger.warning(f"[PRESENCE] Некорректное время '{value}', использую {default}")
        hour_str, minute_str = default.split(":", 1)
        return dt_time(hour=int(hour_str), minute=int(minute_str))

def parse_presence_time_range(text: str) -> tuple[str, str] | None:
    matches = re.findall(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if len(matches) < 2:
        return None
    start = f"{int(matches[0][0]):02d}:{int(matches[0][1]):02d}"
    end = f"{int(matches[1][0]):02d}:{int(matches[1][1]):02d}"
    return start, end

def parse_extra_route_input(text: str) -> tuple[int, int, list[str]] | None:
    match = re.match(r"^\s*(-?\d+)\s+(\d+)(?:\s+([\s\S]*))?$", text)
    if not match:
        return None
    try:
        extra_chat_id = int(match.group(1))
        target_topic_id = int(match.group(2))
    except ValueError:
        return None
    if target_topic_id <= 0:
        return None

    keywords = []
    seen = set()
    for value in re.split(r"[\n,;]+", match.group(3) or ""):
        keyword = value.strip()
        normalized = keyword.casefold()
        if keyword and normalized not in seen:
            seen.add(normalized)
            keywords.append(keyword)

    return extra_chat_id, target_topic_id, keywords

def is_presence_daytime(now_kyiv: datetime | None = None, settings: dict | None = None) -> bool:
    settings = settings or load_bot_settings()
    now_kyiv = now_kyiv or get_now_kyiv()
    now_time = now_kyiv.time()
    start = parse_hhmm(settings.get("presence_online_from", "08:00"), "08:00")
    end = parse_hhmm(settings.get("presence_online_until", "23:30"), "23:30")

    if start <= end:
        return start <= now_time < end
    return now_time >= start or now_time < end

async def set_account_presence(online: bool):
    if not client:
        return
    await client(UpdateStatusRequest(offline=not online))

async def presence_emulation_loop():
    last_online_state = None
    logger.info("[PRESENCE] Фоновая задача эмуляции online запущена")

    while True:
        try:
            settings = load_bot_settings()

            if not settings.get("presence_enabled", True):
                if last_online_state != "disabled":
                    await set_account_presence(False)
                    logger.info("[PRESENCE] Эмуляция выключена в настройках, аккаунт переведен в offline")
                last_online_state = "disabled"
                await asyncio.sleep(60)
                continue

            should_be_online = is_presence_daytime(settings=settings)
            await set_account_presence(should_be_online)

            if should_be_online:
                if last_online_state is not True:
                    logger.info(
                        "[PRESENCE] Аккаунт переведен в online "
                        f"({settings['presence_online_from']}-{settings['presence_online_until']} Киев)"
                    )
                base_sleep = PRESENCE_REFRESH_SECONDS
                jitter = random.randint(-PRESENCE_JITTER_SECONDS, PRESENCE_JITTER_SECONDS)
                sleep_for = max(30, base_sleep + jitter)
            else:
                if last_online_state is not False:
                    logger.info("[PRESENCE] Ночное окно: аккаунт переведен в offline")
                sleep_for = PRESENCE_NIGHT_CHECK_SECONDS

            last_online_state = should_be_online
            await asyncio.sleep(sleep_for)

        except FloodWaitError as e:
            wait_for = int(getattr(e, "seconds", 60)) + 5
            logger.warning(f"[PRESENCE] FloodWait, пауза {wait_for} сек")
            await asyncio.sleep(wait_for)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[PRESENCE ERROR] {e}")
            await asyncio.sleep(300)

def render_message_html(msg) -> str:
    """
    Конвертирует текст + entities Telethon в HTML для Bot API.
    Фиксы:
    - убираем артефакт '{}' который Telethon генерирует для неизвестных entity-типов
    - убираем пустые <pre></pre> и <code></code>
    """
    try:
        text = msg.message or ""
        entities = getattr(msg, "entities", None) or []
        if not text:
            return ""

        # Фильтруем entity-типы которые порождают '{}'
        from telethon.tl.types import MessageEntityCustomEmoji
        SKIP_ENTITY_TYPES = (MessageEntityCustomEmoji,)
        try:
            filtered_entities = [e for e in entities if not isinstance(e, SKIP_ENTITY_TYPES)]
        except Exception:
            filtered_entities = entities

        result = telethon_html.unparse(text, filtered_entities)

        # Убираем оставшиеся артефакты '{}'
        result = re.sub(r'\{\}', '', result)

        # Убираем пустые теги
        result = re.sub(r'<pre>\s*</pre>', '', result)
        result = re.sub(r'<code>\s*</code>', '', result)

        # Bot API не поддерживает <pre><code class='language-X'>...</code></pre>
        # Telethon генерирует именно такую структуру для MessageEntityPre.
        # Разворачиваем: оставляем только <pre>содержимое</pre>, убирая
        # вложенный <code class=...> и лишние отступы.
        def flatten_pre(m):
            inner = m.group(1)
            # убираем обёртку <code class='language-...'>...</code> внутри pre
            inner = re.sub(r'<code[^>]*>(.*?)</code>', lambda c: c.group(1), inner, flags=re.DOTALL)
            # strip только внешние переносы
            inner = inner.strip('\n')
            lines = inner.split('\n')
            # минимальный отступ среди непустых строк
            min_indent = min(
                (len(l) - len(l.lstrip(' ')) for l in lines if l.strip()),
                default=0
            )
            cleaned = '\n'.join(l[min_indent:] for l in lines)
            # убираем trailing пробелы в каждой строке
            cleaned = '\n'.join(l.rstrip() for l in cleaned.split('\n'))
            cleaned = cleaned.strip('\n')
            return f'<pre>{cleaned}</pre>'
        result = re.sub(r'<pre>(.*?)</pre>', flatten_pre, result, flags=re.DOTALL)

        return result

    except Exception as e:
        logger.warning(f"[STYLE ERROR] Не удалось распарсить entities: {e}")
        return escape(msg.message or "")

def has_pre_block(msg) -> bool:
    """Проверяет, содержит ли сообщение блок <pre> (моноширинная таблица/код)."""
    from telethon.tl.types import MessageEntityPre
    entities = getattr(msg, "entities", None) or []
    return any(isinstance(e, MessageEntityPre) for e in entities)

def escape_md(text: str) -> str:
    if text is None:
        return ""
    chars = r'_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, f'\\{ch}')
    return text

def build_prefixed_html(sender_name: str, user_marker: str, msg, edited=False) -> str:
    original_html = render_message_html(msg)

    safe_sender = escape(sender_name or "Unknown")
    safe_marker = escape(user_marker or "🔹")

    if DISPLAY_MODE == "compact":
        header = f"{safe_marker} <b>{safe_sender}:</b>"
    else:
        header = f"{safe_marker} <b>{safe_sender}</b>"

    if original_html:
        if has_pre_block(msg):
            # Для сообщений с <pre> (таблицы, код):
            # Telethon при unparse оборачивает весь pre-контент так:
            # <pre>строка1\nстрока2...</pre>
            result = f"{header}\n{original_html}"
        else:
            result = f"{header}\n{original_html}"
    else:
        result = header

    if edited:
        edit_time = get_now_kyiv().strftime('%H:%M')
        result += f"\n\n<i>(ред. {edit_time})</i>"

    return result

def parse_log_timestamp(line: str):
    m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}\s\|", line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def prune_old_logs():
    if not os.path.exists(LOG_FILE):
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOG_RETENTION_DAYS)
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        kept = []
        keep_current_block = False
        for line in lines:
            ts = parse_log_timestamp(line)
            if ts is not None:
                keep_current_block = ts >= cutoff
            if keep_current_block:
                kept.append(line)
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.writelines(kept)
    except Exception as e:
        logger.error(f"[LOG PRUNE ERROR] {e}")

def collect_recent_logs(hours=LOG_EXPORT_HOURS) -> str:
    if not os.path.exists(LOG_FILE):
        return None
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    output_path = f"logs_last_{hours}h.txt"
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        selected = []
        keep_current_block = False
        for line in lines:
            ts = parse_log_timestamp(line)
            if ts is not None:
                keep_current_block = ts >= cutoff
            if keep_current_block:
                selected.append(line)
        if not selected:
            selected = ["За последние 24 часа записей нет.\n"]
        with open(output_path, "w", encoding="utf-8") as f:
            f.writelines(selected)
        return output_path
    except Exception as e:
        logger.error(f"[LOG EXPORT ERROR] {e}")
        return None

# ====== DATABASE ======
# Схема расширена: добавлена таблица msg_map_extra для хранения
# маппингов сообщений в дополнительные каналы-назначения.
# Основная таблица msg_map не изменилась — обратная совместимость сохранена.

class DB:
    @staticmethod
    def init():
        with sqlite3.connect(DB_FILE) as conn:
            # WAL снижает вероятность "database is locked" при параллельном
            # чтении (например, во время создания backup) и записи.
            try:
                conn.execute('PRAGMA journal_mode=WAL')
            except Exception as e:
                logger.warning(f"[DB INIT] Не удалось включить WAL: {e}")
            conn.execute(
                'CREATE TABLE IF NOT EXISTS msg_map '
                '(src_id INTEGER PRIMARY KEY, tgt_id INTEGER, tid INTEGER, custom_target_id INTEGER)'
            )
            # Новая таблица: маппинг src_id -> (tgt_chat_id, tgt_id, tid) для каждого доп. канала.
            # Одному src_id может соответствовать несколько строк (по одной на доп. канал).
            conn.execute(
                'CREATE TABLE IF NOT EXISTS msg_map_extra '
                '(src_id INTEGER, tgt_chat_id INTEGER, tgt_id INTEGER, tid INTEGER, '
                'PRIMARY KEY (src_id, tgt_chat_id))'
            )

    @staticmethod
    def save(src_id, tgt_chat_id, tgt_id, tid):
        """Сохраняет маппинг для основного канала."""
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO msg_map (src_id, tgt_id, tid, custom_target_id) VALUES (?, ?, ?, ?)',
                (src_id, tgt_id, tid, tgt_chat_id)
            )

    @staticmethod
    def save_extra(src_id, tgt_chat_id, tgt_id, tid):
        """Сохраняет маппинг для дополнительного канала."""
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO msg_map_extra (src_id, tgt_chat_id, tgt_id, tid) VALUES (?, ?, ?, ?)',
                (src_id, tgt_chat_id, tgt_id, tid)
            )

    @staticmethod
    def get(src_id):
        """Возвращает маппинг основного канала."""
        with sqlite3.connect(DB_FILE) as conn:
            r = conn.execute(
                'SELECT tgt_id, tid, custom_target_id FROM msg_map WHERE src_id = ?',
                (src_id,)
            ).fetchone()
            if r:
                return {"tgt_id": r[0], "tid": r[1], "tgt_chat_id": r[2]}
            return None

    @staticmethod
    def get_extra(src_id):
        """
        Возвращает список маппингов для всех дополнительных каналов.
        Формат: [{"tgt_chat_id": ..., "tgt_id": ..., "tid": ...}, ...]
        """
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                'SELECT tgt_chat_id, tgt_id, tid FROM msg_map_extra WHERE src_id = ?',
                (src_id,)
            ).fetchall()
            return [{"tgt_chat_id": r[0], "tgt_id": r[1], "tid": r[2]} for r in rows]

# ====== TOPIC MANAGER ======
# Новое поле в JSON-конфиге чата: "extra_targets" — список доп. каналов.
# Старый формат {"chat_id": int, "topics": {"<source_tid>": <target_tid>}}
# продолжает означать "дублировать весь чат".
# Новый selective-формат {"chat_id": int, "mode": "selected", "topics": {...}}
# дублирует только явно заданные source-ветки.
#
# ВАЖНО про блокировку: все операции "прочитать JSON -> изменить -> сохранить"
# либо обёрнуты в data_lock (в методах ниже), либо в местах вызова
# (callback_handler / handle_admin_text / cmd_bindtopic), чтобы исключить
# потерю обновлений при параллельной обработке нескольких апдейтов и
# чтобы файл не мог быть прочитан "наполовину записанным" во время backup/restore.

class TopicManager:
    @staticmethod
    def load_db():
        if not os.path.exists(TOPICS_DB_FILE):
            return {}
        try:
            with open(TOPICS_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}

    @staticmethod
    def save_db(db):
        # Пишем во временный файл и атомарно переименовываем — так исключаем
        # шанс получить битый/пустой JSON, если процесс упадёт посреди записи.
        tmp_path = f"{TOPICS_DB_FILE}.tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, TOPICS_DB_FILE)

    @staticmethod
    def get_status(chat_id, s_tid=0):
        db = TopicManager.load_db()
        chat_data = db.get(str(chat_id))

        logger.info(f"[GET_STATUS] chat_id={chat_id}, s_tid={s_tid}")

        if not chat_data:
            logger.info("[GET_STATUS] -> new (chat not found)")
            return "new"

        if not chat_data.get('enabled', True):
            logger.info("[GET_STATUS] -> paused (chat disabled)")
            return "paused"

        t_key = str(s_tid or 0)
        topic_data = chat_data.get('topics', {}).get(t_key)

        logger.info(f"[GET_STATUS] t_key={t_key}, topic_data={topic_data}")

        if topic_data and not topic_data.get('enabled', True):
            logger.info("[GET_STATUS] -> paused (topic disabled)")
            return "paused"

        result = "active" if (topic_data and topic_data.get('topic_id')) else "active_need_topic"
        logger.info(f"[GET_STATUS] -> {result}")
        return result

    @staticmethod
    async def register_source(chat_id, title, chat_type, s_tid=0, s_tname=None, target_tid=None):
        async with data_lock:
            db = TopicManager.load_db()
            c_key, t_key = str(chat_id), str(s_tid or 0)

            if c_key not in db:
                default_enabled = False if chat_type == "private" else True
                db[c_key] = {
                    "title": title,
                    "type": chat_type,
                    "enabled": default_enabled,
                    "custom_target_id": None,
                    "auto_create_topics": True,
                    "extra_targets": [],   # <- список доп. каналов
                    "topics": {}
                }

            if "auto_create_topics" not in db[c_key]:
                db[c_key]["auto_create_topics"] = True

            # Миграция: добавляем поле если его нет в старых записях
            if "extra_targets" not in db[c_key]:
                db[c_key]["extra_targets"] = []

            existing_topic = db[c_key]["topics"].get(t_key, {})
            db[c_key]["topics"][t_key] = {
                "topic_id": target_tid or existing_topic.get('topic_id'),
                "title": s_tname or existing_topic.get('title') or (
                    "Личка" if chat_type == "private" else (f"Thread {t_key}" if t_key != "0" else "Main")
                ),
                "enabled": existing_topic.get('enabled', True)
            }
            TopicManager.save_db(db)

    @staticmethod
    async def set_topic_title(chat_id, s_tid, title: str):
        clean_title = (title or "").strip()
        if not clean_title:
            return
        async with data_lock:
            db = TopicManager.load_db()
            c_key, t_key = str(chat_id), str(s_tid or 0)
            if c_key not in db or t_key not in db[c_key].get("topics", {}):
                return
            current_title = db[c_key]["topics"][t_key].get("title")
            if current_title == clean_title:
                return
            db[c_key]["topics"][t_key]["title"] = clean_title
            TopicManager.save_db(db)

    # ------------------------------------------------------------------ #
    # Новые методы для управления дополнительными каналами
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_extra_targets(chat_id: str) -> list:
        """
        Возвращает список доп. каналов для источника.
        Формат: [{"chat_id": int, "topics": {"<s_tid>": <t_tid>}}, ...]
        """
        db = TopicManager.load_db()
        return db.get(str(chat_id), {}).get("extra_targets", [])

    @staticmethod
    def is_selected_extra_target(extra_target: dict) -> bool:
        return extra_target.get("mode") == "selected"

    @staticmethod
    async def add_extra_target(chat_id: str, extra_chat_id: int) -> bool:
        """Добавляет доп. канал к источнику. Возвращает False если уже есть."""
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(chat_id)
            if c_key not in db:
                return False
            if "extra_targets" not in db[c_key]:
                db[c_key]["extra_targets"] = []

            # Проверяем дубликат
            for et in db[c_key]["extra_targets"]:
                if et["chat_id"] == extra_chat_id:
                    return False

            db[c_key]["extra_targets"].append({"chat_id": extra_chat_id, "topics": {}})
            TopicManager.save_db(db)
            return True

    @staticmethod
    async def add_extra_topic_route(
        chat_id: str,
        source_tid: str,
        extra_chat_id: int,
        target_tid: int,
        keywords: list[str] | None = None
    ) -> bool:
        """Добавляет дубль конкретной source-ветки в конкретный топик доп. канала."""
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(chat_id)
            t_key = str(source_tid or 0)
            if c_key not in db:
                return False
            if "extra_targets" not in db[c_key]:
                db[c_key]["extra_targets"] = []

            for et in db[c_key]["extra_targets"]:
                if et.get("chat_id") == extra_chat_id:
                    if "topics" not in et:
                        et["topics"] = {}
                    et["topics"][t_key] = target_tid
                    filters = et.setdefault("keyword_filters", {})
                    if keywords:
                        filters[t_key] = keywords
                    else:
                        filters.pop(t_key, None)
                    if not filters:
                        et.pop("keyword_filters", None)
                    TopicManager.save_db(db)
                    return True

            new_target = {
                "chat_id": extra_chat_id,
                "mode": "selected",
                "topics": {t_key: target_tid}
            }
            if keywords:
                new_target["keyword_filters"] = {t_key: keywords}
            db[c_key]["extra_targets"].append(new_target)
            TopicManager.save_db(db)
            return True

    @staticmethod
    async def remove_extra_target(chat_id: str, extra_chat_id: int):
        """Удаляет доп. канал из источника."""
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(chat_id)
            if c_key not in db:
                return
            db[c_key]["extra_targets"] = [
                et for et in db[c_key].get("extra_targets", [])
                if et["chat_id"] != extra_chat_id
            ]
            TopicManager.save_db(db)

    @staticmethod
    async def remove_extra_topic_route(chat_id: str, source_tid: str, extra_chat_id: int):
        """Удаляет дубль конкретной source-ветки в доп. канал."""
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(chat_id)
            t_key = str(source_tid or 0)
            if c_key not in db:
                return

            kept_targets = []
            changed = False
            for et in db[c_key].get("extra_targets", []):
                if et.get("chat_id") == extra_chat_id and t_key in et.get("topics", {}):
                    del et["topics"][t_key]
                    filters = et.get("keyword_filters", {})
                    filters.pop(t_key, None)
                    if not filters:
                        et.pop("keyword_filters", None)
                    changed = True
                    if TopicManager.is_selected_extra_target(et) and not et.get("topics"):
                        continue
                kept_targets.append(et)

            if changed:
                db[c_key]["extra_targets"] = kept_targets
                TopicManager.save_db(db)

    @staticmethod
    async def set_extra_topic(chat_id: str, extra_chat_id: int, s_tid: str, t_tid: int):
        """Сохраняет маппинг топика для конкретного доп. канала."""
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(chat_id)
            for et in db.get(c_key, {}).get("extra_targets", []):
                if et["chat_id"] == extra_chat_id:
                    et["topics"][str(s_tid)] = t_tid
                    TopicManager.save_db(db)
                    return

    @staticmethod
    def get_extra_topic(chat_id: str, extra_chat_id: int, s_tid) -> int | None:
        """Возвращает target_tid для конкретного доп. канала и source topic."""
        for et in TopicManager.get_extra_targets(chat_id):
            if et["chat_id"] == extra_chat_id:
                return et["topics"].get(str(s_tid))
        return None

    @staticmethod
    def get_extra_topic_routes(chat_id: str, s_tid) -> list:
        """Возвращает доп. маршруты, явно настроенные для source-ветки."""
        routes = []
        t_key = str(s_tid or 0)
        for et in TopicManager.get_extra_targets(chat_id):
            topics = et.get("topics", {})
            if t_key in topics:
                routes.append({
                    "chat_id": et["chat_id"],
                    "topic_id": topics[t_key],
                    "selected": TopicManager.is_selected_extra_target(et),
                    "keywords": et.get("keyword_filters", {}).get(t_key, [])
                })
        return routes

    @staticmethod
    def get_extra_topic_keywords(extra_target: dict, s_tid) -> list[str]:
        return extra_target.get("keyword_filters", {}).get(str(s_tid or 0), [])

# ====== FORUM MANAGER ======
class ForumManager:
    @staticmethod
    async def create_topic(target_chat, chat_title, s_tname=None):
        try:
            name = (s_tname if s_tname else f"💬 {chat_title}")[:120]
            res = await bot_app.bot.create_forum_topic(chat_id=target_chat, name=name)
            tid = res.message_thread_id
            logger.info(f"[FORUM] Создан новый топик '{name}' ID: {tid} в чате {target_chat}")
            return tid
        except Exception as e:
            logger.error(f"[FORUM ERROR] Ошибка создания топика: {e}")
            return None

def extract_topic_create_title(msg) -> str | None:
    action = getattr(msg, "action", None)
    if isinstance(action, MessageActionTopicCreate):
        title = getattr(action, "title", None)
        if title:
            return str(title).strip()
    return None

def is_generic_topic_title(title: str | None, topic_id) -> bool:
    if not title:
        return True
    normalized = str(title).strip()
    return normalized in {"Main", "Личка", f"Thread {topic_id}"}

async def get_source_topic_title(chat, source_top_id, chat_conf=None, msg=None) -> str | None:
    if not source_top_id or int(source_top_id) <= 0:
        return None

    t_key = str(source_top_id)
    stored_title = (chat_conf or {}).get("topics", {}).get(t_key, {}).get("title")
    if not is_generic_topic_title(stored_title, source_top_id):
        return stored_title

    title = extract_topic_create_title(msg)
    if title:
        return title

    try:
        try:
            from telethon.tl.functions.messages import GetForumTopicsByIDRequest
            request = GetForumTopicsByIDRequest(peer=chat, topics=[int(source_top_id)])
        except ImportError:
            from telethon.tl.functions.channels import GetForumTopicsByIDRequest
            request = GetForumTopicsByIDRequest(channel=chat, topics=[int(source_top_id)])
        res = await client(request)
        if res and getattr(res, "topics", None):
            title = getattr(res.topics[0], "title", None)
            if title:
                return str(title).strip()
    except Exception as e:
        logger.warning(f"[TOPIC TITLE API ERROR] {e}")

    try:
        topic_start_msg = await client.get_messages(chat, ids=int(source_top_id))
        title = extract_topic_create_title(topic_start_msg)
        if title:
            return title
    except Exception as e:
        logger.warning(f"[TOPIC TITLE MESSAGE ERROR] {e}")

    return None

def resolve_source_topic_id(msg, chat=None, chat_conf=None) -> int:
    if getattr(msg, 'message_thread_id', None):
        return int(msg.message_thread_id)

    reply_to = getattr(msg, 'reply_to', None)
    if not reply_to:
        return 0

    if getattr(reply_to, 'reply_to_top_id', None):
        return int(reply_to.reply_to_top_id)

    if getattr(reply_to, 'reply_to_msg_id', None):
        candidate = int(reply_to.reply_to_msg_id)
        known_topics = (chat_conf or {}).get('topics', {})
        is_forum = isinstance(chat, Channel) and getattr(chat, 'forum', False)
        forum_topic = getattr(reply_to, 'forum_topic', None)
        # В forum-чате только forum_topic=True подтверждает, что candidate — корень ветки.
        if str(candidate) in known_topics and (not is_forum or forum_topic is True):
            return candidate
        if is_forum and forum_topic is True:
            return candidate

    return 0

def get_real_reply_source_msg_id(msg, source_top_id: int) -> int | None:
    """
    В forum-топиках Telegram reply_to_msg_id часто указывает на корневое
    сообщение ветки даже для обычных сообщений. Такой служебный reply нельзя
    переносить в target, иначе вся ветка начинает отвечать на один старый msg.
    """
    reply_to = getattr(msg, "reply_to", None)
    if not reply_to:
        return None

    reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None)
    if not reply_to_msg_id:
        return None

    reply_to_msg_id = int(reply_to_msg_id)
    reply_to_top_id = getattr(reply_to, "reply_to_top_id", None)
    if reply_to_top_id is not None and reply_to_msg_id == int(reply_to_top_id):
        return None
    if source_top_id and reply_to_msg_id == int(source_top_id):
        return None

    return reply_to_msg_id

def same_optional_int(left, right) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return int(left) == int(right)
    except (TypeError, ValueError):
        return left == right

def message_matches_keywords(msg, keywords: list[str]) -> bool:
    if not keywords:
        return True

    searchable_parts = [getattr(msg, "message", "") or ""]
    for entity in getattr(msg, "entities", None) or []:
        url = getattr(entity, "url", None)
        if url:
            searchable_parts.append(str(url))

    webpage = getattr(getattr(msg, "media", None), "webpage", None)
    webpage_url = getattr(webpage, "url", None)
    if webpage_url:
        searchable_parts.append(str(webpage_url))

    haystack = "\n".join(searchable_parts).casefold()
    return any(
        str(keyword).casefold() in haystack
        for keyword in keywords
        if str(keyword).strip()
    )

# ====== BACKUP / RESTORE ======

async def create_backup_archive() -> str | None:
    """
    Создаёт zip с bot_data.db и topics_mapping.json.
    Использует sqlite backup API, чтобы не словить corruption при
    параллельной записи в БД во время бэкапа.
    """
    timestamp = get_now_kyiv().strftime('%Y%m%d_%H%M%S')
    archive_path = f"backup_{timestamp}.zip"
    tmp_db_copy = f"_backup_tmp_{timestamp}.db"

    try:
        src_conn = sqlite3.connect(DB_FILE)
        dst_conn = sqlite3.connect(tmp_db_copy)
        with dst_conn:
            src_conn.backup(dst_conn)
        src_conn.close()
        dst_conn.close()

        # topics_mapping.json читаем под тем же локом, что и запись,
        # чтобы точно не захватить файл в процессе перезаписи.
        async with data_lock:
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if os.path.exists(tmp_db_copy):
                    zf.write(tmp_db_copy, arcname='bot_data.db')
                if os.path.exists(TOPICS_DB_FILE):
                    zf.write(TOPICS_DB_FILE, arcname=TOPICS_DB_FILE)
                if os.path.exists(BOT_SETTINGS_FILE):
                    zf.write(BOT_SETTINGS_FILE, arcname=BOT_SETTINGS_FILE)

        return archive_path
    except Exception as e:
        logger.error(f"[BACKUP ERROR] {e}")
        return None
    finally:
        if os.path.exists(tmp_db_copy):
            try:
                os.remove(tmp_db_copy)
            except Exception:
                pass

async def send_backup(bot, chat_id: int):
    archive_path = await create_backup_archive()
    if not archive_path:
        await bot.send_message(chat_id=chat_id, text="❌ Не удалось создать backup.")
        return
    try:
        with open(archive_path, "rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=os.path.basename(archive_path),
                caption=f"💾 Backup от {get_now_kyiv().strftime('%d.%m.%Y %H:%M')} (Киев)"
            )
    except Exception as e:
        logger.error(f"[BACKUP SEND ERROR] {e}")
        await bot.send_message(chat_id=chat_id, text=f"❌ Ошибка отправки backup: {e}")
    finally:
        if os.path.exists(archive_path):
            try:
                os.remove(archive_path)
            except Exception:
                pass

async def cmd_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("⏳ Собираю backup...")
    await send_backup(context.bot, ADMIN_ID)

async def scheduled_backup_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("[BACKUP] Запуск ежедневного backup по расписанию")
    await send_backup(context.bot, ADMIN_ID)

async def cmd_restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    user_edit_state[ADMIN_ID] = {"mode": "restore_backup"}
    await update.message.reply_text(
        "⚠️ Отправьте .zip файл backup для восстановления.\n\n"
        "Перед перезаписью я автоматически сделаю резервную копию "
        "текущих данных — если что-то пойдёт не так, можно будет откатиться."
    )

async def handle_admin_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_edit_state.get(user_id, {}).get("mode") != "restore_backup":
        return

    user_edit_state.pop(user_id, None)
    doc = update.message.document

    if not doc or not (doc.file_name or "").lower().endswith(".zip"):
        await update.message.reply_text("❌ Нужен .zip файл backup (созданный через /backup).")
        return

    tmp_zip = f"_restore_tmp_{get_now_kyiv().strftime('%Y%m%d_%H%M%S')}.zip"
    try:
        await update.message.reply_text("⏳ Скачиваю и проверяю архив...")
        tg_file = await doc.get_file()
        await tg_file.download_to_drive(tmp_zip)

        with zipfile.ZipFile(tmp_zip, 'r') as zf:
            names = zf.namelist()
            if 'bot_data.db' not in names or TOPICS_DB_FILE not in names:
                await update.message.reply_text(
                    "❌ Архив не содержит нужных файлов (bot_data.db и/или topics_mapping.json)."
                )
                return

            # safety backup перед перезаписью текущих данных
            safety_path = await create_backup_archive()
            if safety_path:
                logger.info(f"[RESTORE] Safety backup перед восстановлением сохранён: {safety_path}")
                try:
                    os.remove(safety_path)
                except Exception:
                    pass

            async with data_lock:
                zf.extract('bot_data.db', path='.')
                zf.extract(TOPICS_DB_FILE, path='.')
                if BOT_SETTINGS_FILE in names:
                    zf.extract(BOT_SETTINGS_FILE, path='.')

        DB.init()
        await update.message.reply_text(
            "✅ Восстановление завершено. Данные подхватятся автоматически "
            "(перезапуск бота не обязателен, но рекомендуется на всякий случай)."
        )
        logger.info("[RESTORE] Восстановление из backup выполнено успешно")

    except zipfile.BadZipFile:
        await update.message.reply_text("❌ Файл повреждён или это не zip-архив.")
    except Exception as e:
        logger.error(f"[RESTORE ERROR] {e}")
        await update.message.reply_text(f"❌ Ошибка восстановления: {e}")
    finally:
        if os.path.exists(tmp_zip):
            try:
                os.remove(tmp_zip)
            except Exception:
                pass

# ====== ИНТЕРФЕЙС УПРАВЛЕНИЯ ======

async def show_manage_menu(query, cid, db):
    cdata = db.get(str(cid))
    if not cdata:
        alt_cid = str(cid).replace("-100", "")
        cdata = db.get(alt_cid)
        if cdata:
            cid = alt_cid
    if not cdata:
        alt_cid = f"-100{cid}" if not str(cid).startswith("-100") else cid
        cdata = db.get(alt_cid)
        if cdata:
            cid = alt_cid
    if not cdata:
        logger.warning(f"ID {cid} не найден в базе при попытке открыть меню")
        return

    is_private = cdata.get('type') == 'private'
    custom_target = cdata.get('custom_target_id') or "По умолчанию (из .env)"
    auto_create_topics = cdata.get('auto_create_topics', True)
    extra_targets = cdata.get('extra_targets', [])

    safe_title = escape_md(cdata['title'])
    text = f"⚙️ **Управление:** {safe_title} (`{cid}`)\n\n"
    text += f"Статус: {'✅ ВКЛ' if cdata['enabled'] else '⏸ ПАУЗА'}\n"
    text += f"🎯 Основной канал: `{custom_target}`\n"

    # Показываем список доп. каналов
    if extra_targets:
        ids = ', '.join(f'`{et["chat_id"]}`' for et in extra_targets)
        text += f"➕ Доп. каналы: {ids}\n"
    else:
        text += "➕ Доп. каналы: нет\n"

    text += f"🆕 Автосоздание топиков: {'✅ ВКЛ' if auto_create_topics else '⛔ ВЫКЛ'}\n\n"
    text += "🔍 `[Статус] Имя (ID источника) ➡️ ID топика`"

    keyboard = [
        [InlineKeyboardButton(
            f"{'🔴 ВЫКЛЮЧИТЬ ЧАТ' if cdata['enabled'] else '🟢 ВКЛЮЧИТЬ ЧАТ'}",
            callback_data=f"tgc_{cid}"
        )],
        [InlineKeyboardButton("🎯 ИЗМЕНИТЬ ОСНОВНОЙ КАНАЛ", callback_data=f"editchat_{cid}")],
        [InlineKeyboardButton("➕ ДОБАВИТЬ ДОП. КАНАЛ", callback_data=f"addextra_{cid}")],
        [InlineKeyboardButton(
            "⛔ НЕ СОЗДАВАТЬ НОВЫЕ ТОПИКИ" if auto_create_topics else "✅ РАЗРЕШИТЬ СОЗДАНИЕ ТОПИКОВ",
            callback_data=f"tat_{cid}"
        )]
    ]

    # Кнопки удаления доп. каналов
    for et in extra_targets:
        ec_id = et["chat_id"]
        keyboard.append([
            InlineKeyboardButton(
                f"🗑 Удалить доп. канал {ec_id}",
                callback_data=f"delextra_{cid}_{ec_id}"
            )
        ])

    if not is_private:
        keyboard.append([InlineKeyboardButton("--- Настройка веток ---", callback_data="none")])
        for tid, tdata in cdata.get('topics', {}).items():
            t_enabled = tdata.get('enabled', True)
            t_status = "🟢" if t_enabled else "🔴"
            target_id = tdata.get('topic_id', '???')
            extra_routes = TopicManager.get_extra_topic_routes(str(cid), tid)

            btn_display = f"{t_status} {tdata.get('title', 'Без названия')} ({tid}) ➡️ {target_id}"
            keyboard.append([InlineKeyboardButton(btn_display, callback_data=f"editid_{cid}_{tid}")])
            keyboard.append([
                InlineKeyboardButton(
                    "⏸ ОТКЛЮЧИТЬ ВЕТКУ" if t_enabled else "🟢 ВКЛЮЧИТЬ ВЕТКУ",
                    callback_data=f"tgt_{cid}_{tid}"
                ),
                InlineKeyboardButton("❌ УДАЛИТЬ", callback_data=f"del_{cid}_{tid}")
            ])
            keyboard.append([
                InlineKeyboardButton("➕ ДУБЛЬ / ФИЛЬТР", callback_data=f"addroute_{cid}_{tid}")
            ])
            for route in extra_routes:
                mode_label = "точечно" if route["selected"] else "весь чат"
                keywords = route.get("keywords", [])
                keyword_label = ""
                if keywords:
                    keyword_label = f" | 🔎 {keywords[0][:28]}"
                    if len(keywords) > 1:
                        keyword_label += f" +{len(keywords) - 1}"
                route_buttons = [
                    InlineKeyboardButton(
                        f"↪ {route['chat_id']} / {route['topic_id']} ({mode_label}){keyword_label}",
                        callback_data="none"
                    )
                ]
                if route["selected"]:
                    route_buttons.append(InlineKeyboardButton(
                        "🗑",
                        callback_data=f"delroute_{cid}_{tid}_{route['chat_id']}"
                    ))
                keyboard.append(route_buttons)

    back_target = "list_privates" if is_private else "list_groups"
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_target)])

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_bot_menu(query):
    settings = load_bot_settings()
    presence_enabled = settings.get("presence_enabled", True)
    schedule_from = settings.get("presence_online_from", "08:00")
    schedule_until = settings.get("presence_online_until", "23:30")
    should_be_online = presence_enabled and is_presence_daytime(settings=settings)

    text = "🤖 **Бот**\n\n"
    text += f"Online-эмуляция: {'✅ ВКЛ' if presence_enabled else '⛔ ВЫКЛ'}\n"
    text += f"Расписание online: `{schedule_from}` — `{schedule_until}` (Киев)\n"
    text += f"Сейчас по настройкам: {'🟢 online' if should_be_online else '⚫ offline'}\n\n"
    text += "Чтобы изменить время, нажмите кнопку ниже и отправьте диапазон, например `08:00 23:30`."

    keyboard = [
        [InlineKeyboardButton(
            "⛔ ВЫКЛЮЧИТЬ ONLINE" if presence_enabled else "🟢 ВКЛЮЧИТЬ ONLINE",
            callback_data="bot_presence_toggle"
        )],
        [InlineKeyboardButton("🕘 ИЗМЕНИТЬ ВРЕМЯ ONLINE", callback_data="bot_presence_time")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")]
    ]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    keyboard = [
        [InlineKeyboardButton("👥 ГРУППЫ И КАНАЛЫ", callback_data="list_groups")],
        [InlineKeyboardButton("👤 ЛИЧНЫЕ СООБЩЕНИЯ", callback_data="list_privates")],
        [InlineKeyboardButton("🤖 БОТ", callback_data="bot_settings")]
    ]
    text = "📂 **Главное меню:**"
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    prune_old_logs()
    log_path = collect_recent_logs(LOG_EXPORT_HOURS)
    if not log_path or not os.path.exists(log_path):
        await update.message.reply_text("❌ Не удалось собрать лог за последние 24 часа.")
        return
    try:
        with open(log_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=os.path.basename(log_path),
                caption="🧾 Логи за последние 24 часа"
            )
    except Exception as e:
        logger.error(f"[CMD /log ERROR] {e}")
        await update.message.reply_text(f"❌ Ошибка отправки логов: {e}")
    finally:
        try:
            if os.path.exists(log_path):
                os.remove(log_path)
        except Exception:
            pass

async def cmd_bindtopic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование:\n"
            "/bindtopic <source_chat_id> <source_topic_id> <target_topic_id>\n\n"
            "Пример:\n"
            "/bindtopic -1001234567890 17 2456"
        )
        return
    try:
        source_chat_id = int(context.args[0])
        source_topic_id = int(context.args[1])
        target_topic_id = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Все аргументы должны быть числами.")
        return
    if source_topic_id <= 0:
        await update.message.reply_text("❌ source_topic_id должен быть больше 0.")
        return
    if target_topic_id <= 0:
        await update.message.reply_text("❌ target_topic_id должен быть больше 0.")
        return
    try:
        async with data_lock:
            db = TopicManager.load_db()
            c_key = str(source_chat_id)
            t_key = str(source_topic_id)
            if c_key not in db:
                db[c_key] = {
                    "title": f"ManualBind {source_chat_id}",
                    "type": "channel",
                    "enabled": True,
                    "custom_target_id": None,
                    "auto_create_topics": True,
                    "extra_targets": [],
                    "topics": {}
                }
            if "topics" not in db[c_key]:
                db[c_key]["topics"] = {}
            if "auto_create_topics" not in db[c_key]:
                db[c_key]["auto_create_topics"] = True
            if "extra_targets" not in db[c_key]:
                db[c_key]["extra_targets"] = []
            existing_topic = db[c_key]["topics"].get(t_key, {})
            db[c_key]["topics"][t_key] = {
                "topic_id": target_topic_id,
                "title": existing_topic.get("title") or f"Thread {source_topic_id}",
                "enabled": existing_topic.get("enabled", True)
            }
            TopicManager.save_db(db)
        logger.info(
            f"[MANUAL BIND] source_chat_id={source_chat_id}, "
            f"source_topic_id={source_topic_id}, target_topic_id={target_topic_id}"
        )
        await update.message.reply_text(
            "✅ Маппинг ветки сохранён.\n"
            f"source_chat_id = `{source_chat_id}`\n"
            f"source_topic_id = `{source_topic_id}`\n"
            f"target_topic_id = `{target_topic_id}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"[BINDTOPIC ERROR] {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID or query.data == "none":
        await query.answer()
        return

    await query.answer()
    data = query.data

    if data in ["list_groups", "list_privates"]:
        db = TopicManager.load_db()
        target_priv = (data == "list_privates")
        kb = [
            [InlineKeyboardButton(
                f"{'✅' if d['enabled'] else '⏸'} {d['title']}",
                callback_data=f"manage_{cid}"
            )]
            for cid, d in db.items()
            if (d.get('type') == 'private') == target_priv
        ]
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")])
        await query.edit_message_text(
            f"📂 **Список: {'Лички' if target_priv else 'Группы'}**",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    elif data == "bot_settings":
        await show_bot_menu(query)

    elif data == "bot_presence_toggle":
        settings = load_bot_settings()
        settings["presence_enabled"] = not settings.get("presence_enabled", True)
        save_bot_settings(settings)

        try:
            if settings["presence_enabled"]:
                await set_account_presence(is_presence_daytime(settings=settings))
            else:
                await set_account_presence(False)
        except Exception as e:
            logger.warning(f"[PRESENCE] Не удалось сразу применить переключатель: {e}")

        await show_bot_menu(query)

    elif data == "bot_presence_time":
        user_edit_state[query.from_user.id] = {"mode": "presence_time"}
        await query.message.reply_text(
            "📝 Введите время, когда аккаунт должен быть online.\n\n"
            "Формат: `08:00 23:30`\n"
            "Первое время — включать online, второе — уходить offline.",
            parse_mode="Markdown"
        )

    elif data.startswith("manage_"):
        db = TopicManager.load_db()
        await show_manage_menu(query, data.split("_", 1)[1], db)

    elif data.startswith("tgc_"):
        cid = data.split("_", 1)[1]
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db:
                db[cid]['enabled'] = not db[cid]['enabled']
                TopicManager.save_db(db)
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data.startswith("tat_"):
        cid = data.split("_", 1)[1]
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db:
                current = db[cid].get("auto_create_topics", True)
                db[cid]["auto_create_topics"] = not current
                TopicManager.save_db(db)
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data.startswith("editchat_"):
        cid = data.split("_", 1)[1]
        user_edit_state[query.from_user.id] = {"mode": "target_chat", "cid": cid}
        await query.message.reply_text(
            "📝 Введите **ID основного канала**, куда пересылать сообщения.\n"
            "Чтобы вернуть стандартный канал, введите `0`."
        )

    elif data.startswith("addextra_"):
        # Запрашиваем ID нового доп. канала
        cid = data.split("_", 1)[1]
        user_edit_state[query.from_user.id] = {"mode": "add_extra_target", "cid": cid}
        await query.message.reply_text(
            "📝 Введите **ID дополнительного канала**, в который нужно дублировать сообщения:"
        )

    elif data.startswith("addroute_"):
        _, cid, tid = data.split("_", 2)
        user_edit_state[query.from_user.id] = {"mode": "add_extra_route", "cid": cid, "tid": tid}
        await query.message.reply_text(
            "📝 Введите **ID дополнительного канала**, **ID топика**, а затем при необходимости "
            "ключевые слова. Несколько ключей разделяйте запятыми или переносами строк.\n\n"
            "Вся ветка:\n"
            "`-1001234567890 777`\n\n"
            "Только сообщения со ссылкой Fundoor:\n"
            "`-1001234567890 777 fundoor.pro/spreadchart`\n\n"
            f"Фильтр применяется только к source-ветке `{tid}`. Достаточно совпадения одного ключа.",
            parse_mode="Markdown"
        )

    elif data.startswith("delextra_"):
        # delextra_<cid>_<extra_chat_id>
        parts = data.split("_", 2)
        cid = parts[1]
        extra_chat_id = int(parts[2])
        await TopicManager.remove_extra_target(cid, extra_chat_id)
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data.startswith("delroute_"):
        _, cid, rest = data.split("_", 2)
        tid, extra_chat_id = rest.rsplit("_", 1)
        await TopicManager.remove_extra_topic_route(cid, tid, int(extra_chat_id))
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data.startswith("editid_"):
        _, cid, tid = data.split("_", 2)
        user_edit_state[query.from_user.id] = {"mode": "topic_id", "cid": cid, "tid": tid}
        await query.message.reply_text(
            f"📝 Введите новый **Target ID** (ID топика) для ветки `{tid}`:"
        )

    elif data.startswith("del_"):
        _, cid, tid = data.split("_", 2)
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db and tid in db[cid].get('topics', {}):
                del db[cid]['topics'][tid]
                TopicManager.save_db(db)
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data.startswith("tgt_"):
        _, cid, tid = data.split("_", 2)
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db and tid in db[cid].get('topics', {}):
                current = db[cid]['topics'][tid].get('enabled', True)
                db[cid]['topics'][tid]['enabled'] = not current
                TopicManager.save_db(db)
        db = TopicManager.load_db()
        await show_manage_menu(query, cid, db)

    elif data == "main_menu":
        await cmd_list(update, context)

async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_edit_state:
        return

    state = user_edit_state.pop(user_id)
    new_input = update.message.text.strip()
    cid = state.get("cid")
    text = None

    if state["mode"] == "presence_time":
        parsed = parse_presence_time_range(new_input)
        if not parsed:
            await update.message.reply_text(
                "❌ Не понял время. Введите два значения в формате `08:00 23:30`.",
                parse_mode="Markdown"
            )
            return

        start, end = parsed
        settings = load_bot_settings()
        settings["presence_online_from"] = start
        settings["presence_online_until"] = end
        save_bot_settings(settings)

        try:
            if settings.get("presence_enabled", True):
                await set_account_presence(is_presence_daytime(settings=settings))
        except Exception as e:
            logger.warning(f"[PRESENCE] Не удалось сразу применить новое расписание: {e}")

        text = f"✅ Время online обновлено: `{start}` — `{end}` (Киев)"

    elif state["mode"] == "target_chat":
        if new_input == "0":
            new_val = None
            text = "✅ Теперь используются настройки по умолчанию."
        else:
            try:
                new_val = int(new_input)
                text = f"✅ Основной канал назначения изменён на `{new_input}`"
            except ValueError:
                await update.message.reply_text("❌ Ошибка: Введите корректный ID (число).")
                return
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db:
                db[cid]['custom_target_id'] = new_val
                TopicManager.save_db(db)

    elif state["mode"] == "add_extra_target":
        try:
            extra_chat_id = int(new_input)
        except ValueError:
            await update.message.reply_text("❌ Ошибка: Введите корректный ID (число).")
            return
        added = await TopicManager.add_extra_target(cid, extra_chat_id)
        if added:
            text = (
                f"✅ Дополнительный канал `{extra_chat_id}` добавлен.\n"
                "Топики для него будут создаваться автоматически при первом сообщении."
            )
        else:
            text = f"⚠️ Канал `{extra_chat_id}` уже добавлен или источник не найден."

    elif state["mode"] == "add_extra_route":
        tid = state["tid"]
        parsed = parse_extra_route_input(new_input)
        if not parsed:
            await update.message.reply_text(
                "❌ Не понял маршрут. Введите ID канала и ID топика через пробел.\n\n"
                "Пример: `-1001234567890 777`",
                parse_mode="Markdown"
            )
            return

        extra_chat_id, target_topic_id, keywords = parsed
        added = await TopicManager.add_extra_topic_route(
            cid, tid, extra_chat_id, target_topic_id, keywords=keywords
        )
        if added:
            filter_text = (
                "\nФильтр: " + ", ".join(f"`{keyword}`" for keyword in keywords)
                if keywords else
                "\nФильтр: нет, пересылаются все сообщения ветки."
            )
            text = (
                "✅ Дубль ветки добавлен.\n"
                f"Source topic `{tid}` будет дублироваться в канал `{extra_chat_id}`, "
                f"топик `{target_topic_id}`."
                f"{filter_text}"
            )
        else:
            text = "⚠️ Не удалось добавить дубль: источник не найден."

    elif state["mode"] == "topic_id":
        tid = state["tid"]
        if not new_input.isdigit():
            await update.message.reply_text("❌ Ошибка: Введите число.")
            return
        async with data_lock:
            db = TopicManager.load_db()
            if cid in db and tid in db[cid].get('topics', {}):
                db[cid]['topics'][tid]['topic_id'] = int(new_input)
                TopicManager.save_db(db)
        text = f"✅ Новый Target ID для ветки `{tid}` установлен: `{new_input}`"

    if text is None:
        return

    await update.message.reply_text(text + "\nИспользуйте /list для управления.")

# ====== CORE SEND LOGIC ======
# Выделена отдельная функция отправки в один канал — используется и для основного,
# и для каждого доп. канала.

async def send_to_target(
    msg,
    prefixed_text: str,
    target_chat: int,
    target_tid: int,
    reply_to_target_id: int | None,
    chat,
    chat_id_str: str,
    source_top_id: int,
    chat_title: str,
    chat_type: str,
    source_topic_title: str | None,
    auto_create_topics: bool,
    is_extra: bool = False
) -> int | None:
    """
    Отправляет сообщение в указанный канал/топик.
    Возвращает message_id отправленного сообщения или None при ошибке.

    is_extra=True — отправка в доп. канал (маппинг топиков берётся из extra_targets).
    """

    current_target_tid = target_tid
    # MessageMediaWebPage — это линк-превью (обычная ссылка в тексте), а не файл.
    # msg.media для таких сообщений truthy, но download_media ничего не вернёт,
    # и попытка отправить это как document/photo даёт ошибку "File must be non-empty".
    has_downloadable_media = bool(msg.media) and not isinstance(msg.media, MessageMediaWebPage)

    for attempt in range(2):
        if not current_target_tid:
            if not auto_create_topics:
                logger.info(
                    f"[SKIP AUTO CREATE {'EXTRA' if is_extra else 'MAIN'}] "
                    f"chat={chat_id_str}, title={chat_title}, "
                    f"source_topic={source_top_id} — автосоздание выключено"
                )
                return None

            logger.info(
                f"[AUTO {'EXTRA' if is_extra else 'MAIN'}] "
                f"Создаю топик для {chat_title} (source_topic={source_top_id}) "
                f"в канале {target_chat}..."
            )
            new_tid = await ForumManager.create_topic(target_chat, chat_title, s_tname=source_topic_title)
            if not new_tid:
                return None

            current_target_tid = new_tid

            if is_extra:
                await TopicManager.set_extra_topic(chat_id_str, target_chat, str(source_top_id), new_tid)
            else:
                await TopicManager.register_source(
                    int(chat_id_str), chat_title, chat_type,
                    source_top_id, s_tname=source_topic_title, target_tid=new_tid
                )

        try:
            current_reply_id = reply_to_target_id if attempt == 0 else None
            # Базовые kwargs — общие для всех типов отправки
            base_kwargs = {
                "chat_id": target_chat,
                "message_thread_id": int(current_target_tid),
                "reply_to_message_id": current_reply_id,
            }

            if has_downloadable_media:
                send_kwargs = {**base_kwargs, "parse_mode": "HTML", "caption": prefixed_text}
                buf = io.BytesIO()
                await msg.download_media(file=buf)
                buf.seek(0)
                buf.name = getattr(msg.file, 'name', 'file') or 'file'

                if isinstance(msg.media, MessageMediaPhoto):
                    sent = await bot_app.bot.send_photo(photo=buf, **send_kwargs)
                elif (
                    hasattr(msg.media, 'document')
                    and any(hasattr(a, 'voice') and a.voice for a in msg.media.document.attributes)
                ):
                    sent = await bot_app.bot.send_voice(voice=buf, **send_kwargs)
                else:
                    sent = await bot_app.bot.send_document(document=buf, **send_kwargs)
            else:
                # link_preview_options поддерживается только в send_message
                send_kwargs = {
                    **base_kwargs,
                    "link_preview_options": LinkPreviewOptions(is_disabled=True),
                }
                sent = await bot_app.bot.send_message(
                    text=prefixed_text,
                    parse_mode="HTML",
                    **send_kwargs
                )

            logger.info(
                f"[SUCCESS {'EXTRA' if is_extra else 'MAIN'}] "
                f"Msg {msg.id} (Source Topic:{source_top_id}) ➡️ "
                f"Target Msg {sent.message_id} (Target Topic:{current_target_tid}) "
                f"in chat {target_chat}"
            )
            return sent.message_id

        except Exception as e:
            err_str = str(e)
            if "message thread not found" in err_str.lower():
                logger.warning(
                    f"[RE-CREATE {'EXTRA' if is_extra else 'MAIN'}] "
                    f"Ветка {current_target_tid} невалидна. Пересоздаю..."
                )
                if is_extra:
                    await TopicManager.set_extra_topic(chat_id_str, target_chat, str(source_top_id), None)
                else:
                    async with data_lock:
                        db_data = TopicManager.load_db()
                        if chat_id_str in db_data and str(source_top_id) in db_data[chat_id_str]['topics']:
                            db_data[chat_id_str]['topics'][str(source_top_id)]['topic_id'] = None
                            TopicManager.save_db(db_data)
                current_target_tid = None
                continue
            elif "reply" in err_str.lower() or "Message to be replied not found" in err_str:
                continue
            else:
                logger.error(f"[ERROR {'EXTRA' if is_extra else 'MAIN'}] {e}")
                break

    return None

# ====== TELETHON HANDLERS ======

async def telethon_handler(event):
    msg = event.message
    if msg.sender_id in EXCLUDED_SENDERS:
        return

    chat = await event.get_chat()
    sender = await event.get_sender()

    log_full_message(event, tag="NEW")

    chat_title = getattr(chat, 'title', getattr(chat, 'first_name', 'Unknown'))
    is_private = isinstance(chat, User)
    chat_type = "private" if is_private else ("channel" if getattr(chat, 'broadcast', False) else "group")

    db_data = TopicManager.load_db()
    chat_id_str = str(chat.id)
    chat_conf = db_data.get(chat_id_str, {})
    auto_create_topics = chat_conf.get("auto_create_topics", True)

    final_target_chat = chat_conf.get('custom_target_id') or DEFAULT_TARGET_CHAT_ID

    # ===== Имя + маркер =====
    sender_id = getattr(sender, "id", None)
    if isinstance(chat, Channel) and getattr(chat, 'broadcast', False):
        sender_name = chat_title
    elif isinstance(sender, User):
        first = sender.first_name or ""
        last = sender.last_name or ""
        sender_name = (first + " " + last).strip() or sender.username or "Unknown"
    else:
        sender_name = chat_title
    user_marker = get_user_marker(sender_id)

    # ===== Source topic =====
    source_top_id = resolve_source_topic_id(msg, chat, chat_conf)

    # ===== Target topic (основной канал) =====
    target_tid = chat_conf.get('topics', {}).get(str(source_top_id), {}).get('topic_id')
    if target_tid is not None and int(target_tid) <= 1:
        target_tid = None

    # ===== Reply mapping =====
    reply_to_target_id = None
    reply_mapping = None
    real_reply_source_msg_id = get_real_reply_source_msg_id(msg, source_top_id)
    if real_reply_source_msg_id is not None:
        reply_mapping = DB.get(real_reply_source_msg_id)
        if reply_mapping and same_optional_int(reply_mapping.get("tgt_chat_id"), final_target_chat):
            if not target_tid:
                target_tid = reply_mapping.get('tid')
            if same_optional_int(reply_mapping.get("tid"), target_tid):
                reply_to_target_id = reply_mapping['tgt_id']
            else:
                logger.info(
                    f"[REPLY SKIP] reply mapping topic mismatch: "
                    f"src_reply={real_reply_source_msg_id}, mapped_tid={reply_mapping.get('tid')}, "
                    f"current_tid={target_tid}"
                )
        elif reply_mapping:
            logger.info(
                f"[REPLY SKIP] reply mapping chat mismatch: "
                f"src_reply={real_reply_source_msg_id}, mapped_chat={reply_mapping.get('tgt_chat_id')}, "
                f"current_chat={final_target_chat}"
            )

    logger.info(
        f"[THREAD CHECK] chat.id={chat.id}, msg.id={msg.id}, "
        f"source_top_id={source_top_id}, "
        f"message_thread_id={getattr(msg, 'message_thread_id', None)}, "
        f"reply_to_top_id={getattr(getattr(msg, 'reply_to', None), 'reply_to_top_id', None)}, "
        f"reply_to_msg_id={getattr(getattr(msg, 'reply_to', None), 'reply_to_msg_id', None)}, "
        f"forum_topic={getattr(getattr(msg, 'reply_to', None), 'forum_topic', None)}"
    )

    status = TopicManager.get_status(chat.id, source_top_id)
    if status == "paused":
        logger.info(f"[SKIP] Message {msg.id} skipped because topic {source_top_id} is disabled")
        return

    # ===== Название ветки (для автосоздания и обновления старых generic-имен) =====
    source_topic_title = None
    if not is_private and source_top_id and int(source_top_id) > 0:
        source_topic_title = await get_source_topic_title(chat, source_top_id, chat_conf, msg=msg)
        if source_topic_title:
            stored_title = chat_conf.get('topics', {}).get(str(source_top_id), {}).get('title')
            if is_generic_topic_title(stored_title, source_top_id):
                await TopicManager.set_topic_title(chat.id, source_top_id, source_topic_title)
                chat_conf = TopicManager.load_db().get(chat_id_str, chat_conf)
            logger.info(f"[TOPIC TITLE] source_topic={source_top_id}, title={source_topic_title}")

    # ===== Текст =====
    prefixed_text = build_prefixed_html(sender_name, user_marker, msg, edited=False)
    logger.info(f"[HTML DEBUG] entities={getattr(msg, 'entities', [])}")
    logger.info(f"[HTML DEBUG] raw_text={repr(msg.message)}")
    logger.info(f"[HTML DEBUG] final_html={repr(prefixed_text)}")

    # ===== Ранний выход для новых приватных чатов =====
    if not target_tid and status == "new" and is_private:
        await TopicManager.register_source(chat.id, chat_title, "private", 0)
        return

    if not target_tid and real_reply_source_msg_id is not None and source_top_id == 0:
        logger.info(
            f"[SKIP REPLY AUTO CREATE] chat={chat.id}, msg={msg.id} "
            f"— reply без явного source topic, новый топик не создаем"
        )
        return

    # ====================================================
    # ОТПРАВКА В ОСНОВНОЙ КАНАЛ
    # ====================================================
    sent_main_id = await send_to_target(
        msg=msg,
        prefixed_text=prefixed_text,
        target_chat=final_target_chat,
        target_tid=target_tid,
        reply_to_target_id=reply_to_target_id,
        chat=chat,
        chat_id_str=chat_id_str,
        source_top_id=source_top_id,
        chat_title=chat_title,
        chat_type=chat_type,
        source_topic_title=source_topic_title,
        auto_create_topics=auto_create_topics,
        is_extra=False
    )

    if sent_main_id:
        # Перечитываем db после возможного обновления в send_to_target
        db_data = TopicManager.load_db()
        chat_conf = db_data.get(chat_id_str, {})
        actual_tid = chat_conf.get('topics', {}).get(str(source_top_id), {}).get('topic_id') or target_tid
        DB.save(msg.id, final_target_chat, sent_main_id, int(actual_tid))
    else:
        logger.error(f"[FATAL MAIN] Не удалось отправить {msg.id}")

    # ====================================================
    # ОТПРАВКА В ДОПОЛНИТЕЛЬНЫЕ КАНАЛЫ
    # ====================================================
    extra_targets = TopicManager.get_extra_targets(chat_id_str)

    for et in extra_targets:
        extra_chat_id = et["chat_id"]
        extra_topics = et.get("topics", {})
        extra_target_tid = extra_topics.get(str(source_top_id))
        route_keywords = TopicManager.get_extra_topic_keywords(et, source_top_id)

        if TopicManager.is_selected_extra_target(et) and extra_target_tid is None:
            continue

        if route_keywords and not message_matches_keywords(msg, route_keywords):
            logger.info(
                f"[FILTER SKIP EXTRA] Msg {msg.id} (Source Topic:{source_top_id}) "
                f"не содержит ключи {route_keywords} для канала {extra_chat_id}"
            )
            continue

        # Получаем reply_to для доп. канала из таблицы msg_map_extra
        extra_reply_id = None
        if real_reply_source_msg_id is not None:
            extra_mappings = DB.get_extra(real_reply_source_msg_id)
            for em in extra_mappings:
                if (
                    same_optional_int(em["tgt_chat_id"], extra_chat_id)
                    and same_optional_int(em.get("tid"), extra_target_tid)
                ):
                    extra_reply_id = em["tgt_id"]
                    break

        # Целевой топик для этого доп. канала
        if extra_target_tid is not None and int(extra_target_tid) <= 1:
            extra_target_tid = None

        sent_extra_id = await send_to_target(
            msg=msg,
            prefixed_text=prefixed_text,
            target_chat=extra_chat_id,
            target_tid=extra_target_tid,
            reply_to_target_id=extra_reply_id,
            chat=chat,
            chat_id_str=chat_id_str,
            source_top_id=source_top_id,
            chat_title=chat_title,
            chat_type=chat_type,
            source_topic_title=source_topic_title,
            auto_create_topics=auto_create_topics,
            is_extra=True
        )

        if sent_extra_id:
            # Обновляем actual tid из конфига (мог обновиться в send_to_target)
            actual_extra_tid = (
                TopicManager.get_extra_topic(chat_id_str, extra_chat_id, source_top_id)
                or extra_target_tid
            )
            DB.save_extra(msg.id, extra_chat_id, sent_extra_id, int(actual_extra_tid))
        else:
            logger.error(f"[FATAL EXTRA] Не удалось отправить {msg.id} в доп. канал {extra_chat_id}")

async def telethon_edit_handler(event):
    log_full_message(event, tag="EDIT")
    msg = event.message
    rel = DB.get(msg.id)

    if not rel:
        logger.warning(f"[EDIT] Нет маппинга для сообщения {msg.id}")
        return

    try:
        sender = await event.get_sender()
        chat = await event.get_chat()

        if isinstance(chat, Channel) and getattr(chat, 'broadcast', False):
            sender_name = getattr(chat, 'title', 'Unknown')
            sender_id = getattr(chat, 'id', None)
        elif isinstance(sender, User):
            first = sender.first_name or ""
            last = sender.last_name or ""
            sender_name = (first + " " + last).strip() or sender.username or "Unknown"
            sender_id = sender.id
        else:
            sender_name = "Unknown"
            sender_id = None

        user_marker = get_user_marker(sender_id)
        updated_text = build_prefixed_html(sender_name, user_marker, msg, edited=True)

        # ===== Редактируем в основном канале =====
        logger.info(f"[EDIT MAIN] Обновляю сообщение {rel['tgt_id']} в {rel['tgt_chat_id']}")
        await _edit_message(rel['tgt_chat_id'], rel['tgt_id'], msg, updated_text)

        # ===== Редактируем во всех доп. каналах =====
        extra_rels = DB.get_extra(msg.id)
        for er in extra_rels:
            logger.info(f"[EDIT EXTRA] Обновляю {er['tgt_id']} в {er['tgt_chat_id']}")
            await _edit_message(er['tgt_chat_id'], er['tgt_id'], msg, updated_text)

    except Exception as e:
        logger.error(f"[EDIT ERROR] {e}")

async def _edit_message(target_chat: int, target_msg_id: int, msg, updated_text: str):
    """Вспомогательная функция: редактирует одно сообщение в одном канале."""
    try:
        if msg.media and not isinstance(msg.media, MessageMediaWebPage):
            await bot_app.bot.edit_message_caption(
                chat_id=target_chat,
                message_id=target_msg_id,
                caption=updated_text,
                parse_mode="HTML"
            )
        else:
            await bot_app.bot.edit_message_text(
                chat_id=target_chat,
                message_id=target_msg_id,
                text=updated_text,
                parse_mode="HTML"
            )
        logger.info(f"[EDIT OK] {target_msg_id} в {target_chat} обновлено")
    except Exception as e:
        logger.error(f"[EDIT MSG ERROR] chat={target_chat}, msg={target_msg_id}: {e}")

def log_full_message(event, tag="NEW"):
    try:
        msg = event.message
        chat = event.chat
        sender = event.sender

        log_data = {
            "type": tag,
            "date": str(msg.date),
            "chat_id": getattr(chat, "id", None),
            "chat_title": getattr(chat, "title", getattr(chat, "first_name", None)),
            "chat_type": type(chat).__name__,
            "sender_id": getattr(sender, "id", None),
            "sender_username": getattr(sender, "username", None),
            "sender_name": (
                (getattr(sender, "first_name", "") or "") + " " +
                (getattr(sender, "last_name", "") or "")
            ).strip(),
            "message_id": msg.id,
            "text": msg.message,
            "raw_text": msg.raw_text,
            "reply_to_msg_id": getattr(msg.reply_to, "reply_to_msg_id", None),
            "reply_to_top_id": getattr(msg.reply_to, "reply_to_top_id", None),
            "message_thread_id": getattr(msg, "message_thread_id", None),
            "media_type": type(msg.media).__name__ if msg.media else None,
            "file_name": getattr(msg.file, "name", None) if msg.media else None,
            "file_size": getattr(msg.file, "size", None) if msg.media else None,
        }

        logger.info("========== MESSAGE LOG ==========")
        logger.info(json.dumps(log_data, indent=2, ensure_ascii=False))

    except Exception as e:
        logger.error(f"[LOG ERROR] {e}")

async def main():
    global client, bot_app
    DB.init()
    prune_old_logs()

    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("list", cmd_list))
    bot_app.add_handler(CommandHandler("log", cmd_log))
    bot_app.add_handler(CommandHandler("bindtopic", cmd_bindtopic))
    bot_app.add_handler(CommandHandler("backup", cmd_backup))
    bot_app.add_handler(CommandHandler("restore", cmd_restore))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))
    bot_app.add_handler(MessageHandler(filters.Document.ALL, handle_admin_document))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text))

    await bot_app.initialize()
    await bot_app.start()

    if bot_app.job_queue:
        backup_time = dt_time(
            hour=BACKUP_TIME_HOUR_KYIV, minute=0,
            tzinfo=timezone(timedelta(hours=KYIV_OFFSET))
        )
        bot_app.job_queue.run_daily(scheduled_backup_job, time=backup_time, name="daily_backup")
        logger.info(f"[BACKUP] Ежедневный backup запланирован на {BACKUP_TIME_HOUR_KYIV}:00 (Киев)")
    else:
        logger.warning(
            "[BACKUP] JobQueue недоступен — установите: "
            'pip install "python-telegram-bot[job-queue]" для автобэкапа по расписанию. '
            "Ручной /backup продолжит работать."
        )

    client = TelegramClient('support_session', API_ID, API_HASH)
    client.add_event_handler(telethon_handler, events.NewMessage())
    client.add_event_handler(telethon_edit_handler, events.MessageEdited())

    await client.start()
    presence_task = asyncio.create_task(presence_emulation_loop())
    logger.info("🚀 Бот запущен. Поддержка множественных каналов назначения активна.")

    try:
        async with bot_app:
            await bot_app.updater.start_polling()
            await client.run_until_disconnected()
    finally:
        presence_task.cancel()
        try:
            await presence_task
        except asyncio.CancelledError:
            pass
        try:
            await set_account_presence(False)
            logger.info("[PRESENCE] Аккаунт переведен в offline перед остановкой")
        except Exception as e:
            logger.warning(f"[PRESENCE] Не удалось перевести аккаунт в offline при остановке: {e}")

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())

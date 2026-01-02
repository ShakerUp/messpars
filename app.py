import asyncio
import csv
import os
import sys
import json
import signal
import io
import sqlite3
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.tl.types import User, Chat, Channel, MessageActionTopicCreate, MessageMediaPhoto, MessageMediaDocument
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# ====== ЗАГРУЗКА КОНФИГУРАЦИИ ======
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID'))
ALLOWED_CHAT_ID = int(os.getenv('ALLOWED_CHAT_ID', TARGET_CHAT_ID))

# Файлы
CHAT_CSV = 'chats_seen.csv'
MESSAGES_CSV = 'messages_log.csv'
TOPICS_DB_FILE = 'topics_mapping.json'
DB_FILE = 'bot_data.db' # База для редактирования

# Глобальные переменные
client = None
bot_app = None 
seen_chats = set()
running = True

# Исключения (Черный список системных ID + сам бот)
SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), TARGET_CHAT_ID] + SYSTEM_IDS
EXCLUDED_TOPICS = [1]

# ====== NEW: DATABASE FOR EDITS ======
class DB:
    @staticmethod
    def init():
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS msg_map (src_id INTEGER PRIMARY KEY, tgt_id INTEGER, tid INTEGER)')

    @staticmethod
    def save(src_id, tgt_id, tid):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('INSERT OR REPLACE INTO msg_map VALUES (?, ?, ?)', (src_id, tgt_id, tid))

    @staticmethod
    def get(src_id):
        try:
            with sqlite3.connect(DB_FILE) as conn:
                r = conn.execute('SELECT tgt_id, tid FROM msg_map WHERE src_id = ?', (src_id,)).fetchone()
                return {"tgt_id": r[0], "tid": r[1]} if r else None
        except: return None

# ====== ТВОЙ TOPIC MANAGER ======
class TopicManager:
    @staticmethod
    def load_topics_db():
        if not os.path.exists(TOPICS_DB_FILE): return {}
        try:
            with open(TOPICS_DB_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @staticmethod
    def save_topics_db(db):
        with open(TOPICS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _get_db_key(chat_id, source_thread_id):
        t_id = source_thread_id if source_thread_id else 0
        return f"{chat_id}_{t_id}"

    @staticmethod
    def get_topic_id_for_source(chat_id, source_thread_id):
        db = TopicManager.load_topics_db()
        key = TopicManager._get_db_key(chat_id, source_thread_id)
        return db.get(key, {}).get('topic_id')

    @staticmethod
    def save_topic_for_source(chat_id, source_thread_id, chat_title, dest_topic_id):
        db = TopicManager.load_topics_db()
        key = TopicManager._get_db_key(chat_id, source_thread_id)
        db[key] = {
            'chat_title': chat_title,
            'source_thread_id': source_thread_id,
            'topic_id': dest_topic_id,
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        TopicManager.save_topics_db(db)

    @staticmethod
    def remove_topic_mapping(chat_id, source_thread_id):
        db = TopicManager.load_topics_db()
        key = TopicManager._get_db_key(chat_id, source_thread_id)
        if key in db:
            del db[key]
            TopicManager.save_topics_db(db)

# ====== ТВОЙ CSV MANAGER ======
class CSVManager:
    @staticmethod
    def ensure_csv():
        if not os.path.exists(CHAT_CSV):
            with open(CHAT_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['chat_id', 'chat_type', 'chat_title', 'chat_username', 'first_seen_utc'])
        if not os.path.exists(MESSAGES_CSV):
            with open(MESSAGES_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp_utc', 'chat_id', 'chat_title', 'sender_id', 'sender_username', 'message_id', 'has_media', 'text_truncated'])

    @staticmethod
    async def register_chat(chat):
        chat_id = getattr(chat, 'id', None)
        if chat_id is None or chat_id in seen_chats: return
        seen_chats.add(chat_id)
        ctype = type(chat).__name__
        title = getattr(chat, 'title', getattr(chat, 'first_name', 'N/A'))
        username = getattr(chat, 'username', '')
        with open(CHAT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([chat_id, ctype, title, username, datetime.now(timezone.utc).isoformat()])

# ====== FORUM MANAGER (С ТВОЕЙ ЛОГИКОЙ + DB SAVE) ======
class ForumManager:
    @staticmethod
    async def topic_exists(topic_id):
        if topic_id in EXCLUDED_TOPICS: return False
        try:
            await bot_app.bot.edit_forum_topic(chat_id=TARGET_CHAT_ID, message_thread_id=topic_id)
            return True
        except: return False

    @staticmethod
    async def create_topic(chat_id, chat_title, source_thread_name=None, source_thread_id=None):
        try:
            topic_name = (f"{source_thread_name} | {chat_title}" if source_thread_name else f"💬 {chat_title}")[:120]
            result = await bot_app.bot.create_forum_topic(chat_id=TARGET_CHAT_ID, name=topic_name)
            topic_id = result.message_thread_id
            
            await bot_app.bot.send_message(
                chat_id=TARGET_CHAT_ID, message_thread_id=topic_id,
                text=f"📢 **{topic_name}**\nID чата: `{chat_id}`", parse_mode='Markdown'
            )
            TopicManager.save_topic_for_source(chat_id, source_thread_id, chat_title, topic_id)
            return topic_id
        except Exception as e:
            print(f"❌ Ошибка создания темы: {e}"); return None

    @staticmethod
    async def get_or_create_topic(chat_id, chat_title, source_thread_id=None, source_thread_name=None):
        tid = TopicManager.get_topic_id_for_source(chat_id, source_thread_id)
        if tid and await ForumManager.topic_exists(tid): return tid
        if tid: TopicManager.remove_topic_mapping(chat_id, source_thread_id)
        return await ForumManager.create_topic(chat_id, chat_title, source_thread_name, source_thread_id)

# ====== HANDLERS ======

async def all_message_handler(event):
    msg = event.message
    if msg.sender_id in EXCLUDED_SENDERS: return
    if not msg.message and not msg.media: return

    chat = await event.get_chat()
    chat_id = chat.id
    await CSVManager.register_chat(chat)
    
    title = getattr(chat, 'title', getattr(chat, 'first_name', 'Private'))
    
    # Твоя логика получения инфо о топике
    s_tid, s_tname = None, None
    if msg.reply_to and msg.reply_to.forum_topic:
        s_tid = msg.reply_to.reply_to_msg_id
        try:
            m = await event.client.get_messages(chat_id, ids=[s_tid])
            if m and m[0].action and isinstance(m[0].action, MessageActionTopicCreate):
                s_tname = m[0].action.title
        except: pass

    # Твоя логика отправки медиа/текста
    target_tid = await ForumManager.get_or_create_topic(chat_id, title, s_tid, s_tname)
    if not target_tid: return

    try:
        txt = msg.message or ""
        p = {"chat_id": TARGET_CHAT_ID, "message_thread_id": target_tid, "caption": txt}
        if msg.media:
            buf = io.BytesIO(); await msg.download_media(file=buf); buf.seek(0)
            if isinstance(msg.media, MessageMediaPhoto):
                sent = await bot_app.bot.send_photo(photo=buf, **p)
            else:
                sent = await bot_app.bot.send_document(document=buf, **p)
        else:
            sent = await bot_app.bot.send_message(chat_id=TARGET_CHAT_ID, text=txt, message_thread_id=target_tid)
        
        # Сохраняем для редактирования
        DB.save(msg.id, sent.message_id, target_tid)
    except Exception as e: print(f"❌ Ошибка отправки: {e}")

async def edit_handler(event):
    msg = event.message
    rel = DB.get(msg.id)
    if not rel: return
    try:
        now = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M")
        txt = (msg.text or "") + f"\n\n(ред. {now})"
        if msg.media:
            await bot_app.bot.edit_message_caption(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], caption=txt)
        else:
            await bot_app.bot.edit_message_text(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], text=txt)
    except Exception as e:
        if "Message is not modified" not in str(e): print(f"❌ Ошибка правки: {e}")

# ====== STARTUP ======

async def main():
    global client, bot_app
    CSVManager.ensure_csv()
    DB.init()
    
    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    await bot_app.initialize(); await bot_app.start()

    client = TelegramClient('support_session', API_ID, API_HASH)
    client.add_event_handler(all_message_handler, events.NewMessage())
    client.add_event_handler(edit_handler, events.MessageEdited())
    
    await client.start()
    print("🚀 Бот запущен 1 в 1 как раньше + РЕДАКТИРОВАНИЕ.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
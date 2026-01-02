import asyncio
import os
import sys
import json
import io
import sqlite3
import signal
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telegram.ext import ApplicationBuilder

# ====== ЗАГРУЗКА КОНФИГУРАЦИИ ======
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID'))
ALLOWED_SOURCES = [int(s.strip()) for s in os.getenv('ALLOWED_SOURCES', '').split(',') if s.strip()]
ONLY_WHITELIST = os.getenv('ONLY_WHITELIST', 'False').lower() in ('true', '1', 't')

TOPICS_DB_FILE = 'topics_mapping.json'
DB_FILE = 'bot_data.db'

client = None
bot_app = None
VALID_TOPICS = set()

SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), TARGET_CHAT_ID] + SYSTEM_IDS

# ====== DB MANAGER (Исправленный) ======
class DBManager:
    @staticmethod
    def init_db():
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS message_map (
                    source_id INTEGER PRIMARY KEY,
                    target_msg_id INTEGER,
                    topic_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

    @staticmethod
    def save_relation(source_msg_id, target_msg_id, topic_id):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO message_map (source_id, target_msg_id, topic_id) VALUES (?, ?, ?)',
                (source_msg_id, target_msg_id, topic_id)
            )

    @staticmethod
    def get_relation(source_id):
        try:
            with sqlite3.connect(DB_FILE) as conn:
                r = conn.execute(
                    'SELECT target_msg_id, topic_id FROM message_map WHERE source_id = ?',
                    (source_id,)
                ).fetchone()
                return {"target_msg_id": r[0], "topic_id": r[1]} if r else None
        except:
            return None

# ====== TOPIC MANAGER ======
class TopicManager:
    @staticmethod
    def load_db():
        if not os.path.exists(TOPICS_DB_FILE): return {}
        try:
            with open(TOPICS_DB_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @staticmethod
    def save(chat_id, source_thread_id, chat_title, dest_topic_id):
        db = TopicManager.load_db()
        key = f"{chat_id}_{source_thread_id or 0}"
        db[key] = {"chat_title": chat_title, "topic_id": int(dest_topic_id)}
        with open(TOPICS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)

    @staticmethod
    def get_topic_id(chat_id, source_thread_id):
        return TopicManager.load_db().get(f"{chat_id}_{source_thread_id or 0}", {}).get("topic_id")

    @staticmethod
    def remove(chat_id, source_thread_id):
        db = TopicManager.load_db()
        db.pop(f"{chat_id}_{source_thread_id or 0}", None)
        with open(TOPICS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        VALID_TOPICS.discard((chat_id, source_thread_id))

# ====== FORUM MANAGER ======
class ForumManager:
    @staticmethod
    async def get_or_create_topic(chat_id, chat_title, source_thread_id, source_thread_name):
        topic_id = TopicManager.get_topic_id(chat_id, source_thread_id)
        
        # Если топик уже есть в кэше валидных — возвращаем
        if topic_id and (chat_id, source_thread_id) in VALID_TOPICS:
            return int(topic_id)

        # Проверка работоспособности топика
        if topic_id:
            try:
                msg = await bot_app.bot.send_message(TARGET_CHAT_ID, ".", message_thread_id=int(topic_id), disable_notification=True)
                await bot_app.bot.delete_message(TARGET_CHAT_ID, msg.message_id)
                VALID_TOPICS.add((chat_id, source_thread_id))
                return int(topic_id)
            except:
                TopicManager.remove(chat_id, source_thread_id)

        # Создание нового топика
        name = (f"{source_thread_name} | {chat_title}" if source_thread_name else f"💬 {chat_title}")[:120]
        result = await bot_app.bot.create_forum_topic(chat_id=TARGET_CHAT_ID, name=name)
        new_tid = int(result.message_thread_id)
        TopicManager.save(chat_id, source_thread_id, chat_title, new_tid)
        VALID_TOPICS.add((chat_id, source_thread_id))
        return new_tid

    @staticmethod
    async def send_to_topic(msg, chat_id, chat_title, source_thread_id, source_thread_name):
        text = msg.message or ""
        for attempt in (1, 2):
            tid = await ForumManager.get_or_create_topic(chat_id, chat_title, source_thread_id, source_thread_name)
            if not tid: return

            try:
                params = {"chat_id": TARGET_CHAT_ID, "message_thread_id": int(tid), "caption": text}
                if msg.media:
                    buf = io.BytesIO()
                    await msg.download_media(file=buf)
                    buf.seek(0)
                    if isinstance(msg.media, MessageMediaPhoto):
                        sent = await bot_app.bot.send_photo(photo=buf, **params)
                    else:
                        sent = await bot_app.bot.send_document(document=buf, **params)
                else:
                    sent = await bot_app.bot.send_message(chat_id=TARGET_CHAT_ID, text=text, message_thread_id=int(tid))

                # Защита от General
                if sent.message_thread_id != tid:
                    await bot_app.bot.delete_message(TARGET_CHAT_ID, sent.message_id)
                    TopicManager.remove(chat_id, source_thread_id)
                    if attempt == 1: continue
                    return

                # Сохраняем связь для редактирования
                DBManager.save_relation(msg.id, sent.message_id, tid)
                return
            except Exception as e:
                if attempt == 1:
                    TopicManager.remove(chat_id, source_thread_id)
                    continue
                print(f"❌ Ошибка отправки: {e}")

# ====== ОБРАБОТЧИКИ ======
async def msg_handler(event):
    msg = event.message
    if not msg.sender_id or msg.sender_id in SYSTEM_IDS or msg.action: return
    
    chat = await event.get_chat()
    chat_id = getattr(chat, 'id', None)
    if ONLY_WHITELIST and chat_id not in ALLOWED_SOURCES: return
    
    sender = await event.get_sender()
    if getattr(sender, 'id', 0) in EXCLUDED_SENDERS: return

    title = getattr(chat, 'title', '') or getattr(chat, 'first_name', 'Private')
    
    # Логика определения топика в ИСТОЧНИКЕ
    s_tid, s_tname = None, None
    if msg.reply_to and msg.reply_to.forum_topic:
        s_tid = msg.reply_to.reply_to_msg_id
        try:
            # Пытаемся получить заголовок темы
            m = await event.client.get_messages(msg.peer_id, ids=[s_tid])
            if m and m[0].action:
                s_tname = getattr(m[0].action, 'title', None)
        except: pass

    await ForumManager.send_to_topic(msg, chat_id, title, s_tid, s_tname)

async def edit_handler(event):
    msg = event.message
    # Поиск связи в базе
    relation = DBManager.get_relation(msg.id)
    if not relation: return

    try:
        now = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M")
        new_text = (msg.text or "") + f"\n\n(ред. {now})"
        
        if msg.media:
            await bot_app.bot.edit_message_caption(chat_id=TARGET_CHAT_ID, message_id=relation["target_msg_id"], caption=new_text)
        else:
            await bot_app.bot.edit_message_text(chat_id=TARGET_CHAT_ID, message_id=relation["target_msg_id"], text=new_text)
        print(f"✅ Отредактировано: {msg.id}")
    except Exception as e:
        if "Message is not modified" not in str(e):
            print(f"❌ Ошибка правки: {e}")

async def main():
    global client, bot_app
    DBManager.init_db()
    
    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    await bot_app.initialize()
    await bot_app.start()
    
    client = TelegramClient('support_session', API_ID, API_HASH)
    
    # Регистрация хендлеров
    client.add_event_handler(msg_handler, events.NewMessage(incoming=True))
    client.add_event_handler(edit_handler, events.MessageEdited(incoming=True)) # incoming=True обязателен
    
    await client.start()
    print("🚀 Система запущена. Мирроринг и Правки работают.")
    
    try:
        while True: await asyncio.sleep(3600)
    except: pass

if __name__ == "__main__":
    if sys.platform.startswith('win'): asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
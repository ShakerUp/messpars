import asyncio
import csv
import os
import sys
import json
import signal
import io
from datetime import datetime, timezone
from dotenv import load_dotenv  # Добавлено

from telethon import TelegramClient, events
from telethon.tl.types import User, Chat, Channel, MessageActionTopicCreate, MessageMediaPhoto, MessageMediaDocument
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# ====== ЗАГРУЗКА КОНФИГУРАЦИИ ======
load_dotenv() # Загружает переменные из .env

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Настройки чатов (с дефолтными значениями на всякий случай)
TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID', -1003044057818))
ALLOWED_CHAT_ID = int(os.getenv('ALLOWED_CHAT_ID', -1003044057818))

# Обработка списка разрешенных источников
allowed_sources_raw = os.getenv('ALLOWED_SOURCES', '')
ALLOWED_SOURCES = [int(s.strip()) for s in allowed_sources_raw.split(',') if s.strip()]
ONLY_WHITELIST = os.getenv('ONLY_WHITELIST', 'False').lower() in ('true', '1', 't')

# ====== ФАЙЛЫ (можно оставить так или тоже в .env) ======
CHAT_CSV = 'chats_seen.csv'
MESSAGES_CSV = 'messages_log.csv'
SUBSCRIBERS_FILE = 'subscribers.json'
TOPICS_DB_FILE = 'topics_mapping.json'

# ====== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ======
client = None
bot_app = None 
seen_chats = set()
running = True

# Исключения
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), TARGET_CHAT_ID, 777000]
EXCLUDED_TOPICS = [1]

class TopicManager:
    """Менеджер для работы с темами форума"""
    
    @staticmethod
    def load_topics_db():
        if not os.path.exists(TOPICS_DB_FILE):
            return {}
        try:
            with open(TOPICS_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    @staticmethod
    def save_topics_db(db):
        with open(TOPICS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _get_db_key(chat_id, source_thread_id):
        """Создает уникальный ключ: CHATID_THREADID"""
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

class CSVManager:
    """Менеджер для работы с CSV файлами"""
    @staticmethod
    def ensure_csv():
        if os.path.exists(CHAT_CSV):
            with open(CHAT_CSV, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row:
                        try: seen_chats.add(int(row[0]))
                        except ValueError: pass
        if not os.path.exists(CHAT_CSV):
            with open(CHAT_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['chat_id', 'chat_type', 'chat_title', 'chat_username', 'first_seen_utc'])
        if not os.path.exists(MESSAGES_CSV):
            with open(MESSAGES_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp_utc', 'chat_id', 'chat_title', 'sender_id', 'sender_username', 'message_id', 'has_media', 'text_truncated'])

    @staticmethod
    def log_message_row(row: list):
        with open(MESSAGES_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(row)

    @staticmethod
    async def register_chat(chat):
        try:
            chat_id = getattr(chat, 'id', None)
            if chat_id is None or chat_id in seen_chats: return
            seen_chats.add(chat_id)
            if isinstance(chat, User): ctype, title, username = 'User', chat.first_name, chat.username or ''
            elif isinstance(chat, (Chat, Channel)): ctype, title, username = type(chat).__name__, chat.title, chat.username or ''
            else: ctype, title, username = 'Unknown', 'N/A', ''
            with open(CHAT_CSV, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([chat_id, ctype, title, username, datetime.now(timezone.utc).isoformat()])
        except Exception: pass

class ForumManager:
    """Менеджер для работы с форумом"""
    
    @staticmethod
    async def topic_exists(topic_id):
        if topic_id in EXCLUDED_TOPICS: return False
        try:
            await bot_app.bot.edit_forum_topic(
                chat_id=TARGET_CHAT_ID,
                message_thread_id=topic_id
            )
            return True
        except Exception as e:
            if any(x in str(e).lower() for x in ["not found", "bad request"]): return False
            return True

    @staticmethod
    async def create_topic(chat_id, chat_title, source_thread_name=None, source_thread_id=None):
        try:
            if not bot_app or not bot_app.running: return None
            
            base_name = chat_title
            
            # === ЛОГИКА ИМЕНОВАНИЯ: Сначала Топик, потом Канал ===
            if source_thread_name:
                topic_name = f"{source_thread_name} | {base_name}"
            elif source_thread_id:
                topic_name = f"Topic {source_thread_id} | {base_name}"
            else:
                topic_name = f"💬 {base_name}"

            # Обрезка имени (макс 128 символов)
            topic_name = topic_name[:120] + "..." if len(topic_name) > 123 else topic_name
            
            print(f"🆕 Создаем тему: {topic_name}")
            
            result = await bot_app.bot.create_forum_topic(
                chat_id=TARGET_CHAT_ID,
                name=topic_name,
                icon_color=0x6FB9F0,
            )
            
            topic_id = result.message_thread_id
            
            # Приветственное сообщение (можно скрыть, если нужно совсем чисто)
            welcome_text = (
                f"📢 **{topic_name}**\n"
                f"ID чата: `{chat_id}`"
            )
            await bot_app.bot.send_message(
                chat_id=TARGET_CHAT_ID,
                message_thread_id=topic_id,
                text=welcome_text,
                parse_mode='Markdown'
            )
            
            TopicManager.save_topic_for_source(chat_id, source_thread_id, chat_title, topic_id)
            return topic_id
            
        except Exception as e:
            print(f"❌ Ошибка создания темы: {e}")
            return None

    @staticmethod
    async def get_or_create_topic(chat_id, chat_title, source_thread_id=None, source_thread_name=None):
        existing_dest_topic = TopicManager.get_topic_id_for_source(chat_id, source_thread_id)
        
        if existing_dest_topic:
            if await ForumManager.topic_exists(existing_dest_topic):
                return existing_dest_topic
            else:
                TopicManager.remove_topic_mapping(chat_id, source_thread_id)
        
        return await ForumManager.create_topic(chat_id, chat_title, source_thread_name, source_thread_id)

    @staticmethod
    async def send_to_topic(telethon_message, chat_id, chat_title, source_thread_id=None, source_thread_name=None):
        try:
            if not bot_app or not bot_app.running: return

            final_topic_id = await ForumManager.get_or_create_topic(chat_id, chat_title, source_thread_id, source_thread_name)
            
            if not final_topic_id:
                print("❌ Не удалось определить тему назначения")
                return

            text_content = telethon_message.message or ""

            # === ОТПРАВКА МЕДИА ===
            if telethon_message.media:
                print(f"📥 Скачиваем медиа...")
                media_buffer = io.BytesIO()
                await telethon_message.download_media(file=media_buffer)
                media_buffer.seek(0)
                
                try:
                    if isinstance(telethon_message.media, MessageMediaPhoto):
                        await bot_app.bot.send_photo(
                            chat_id=TARGET_CHAT_ID,
                            message_thread_id=final_topic_id,
                            photo=media_buffer,
                            caption=text_content,
                            parse_mode=None
                        )
                    elif isinstance(telethon_message.media, MessageMediaDocument):
                        mime_type = telethon_message.media.document.mime_type
                        if 'video' in mime_type:
                             await bot_app.bot.send_video(
                                chat_id=TARGET_CHAT_ID,
                                message_thread_id=final_topic_id,
                                video=media_buffer,
                                caption=text_content,
                                parse_mode=None
                            )
                        elif 'audio' in mime_type or 'voice' in mime_type:
                             await bot_app.bot.send_audio(
                                chat_id=TARGET_CHAT_ID,
                                message_thread_id=final_topic_id,
                                audio=media_buffer,
                                caption=text_content,
                                parse_mode=None
                            )
                        else:
                            await bot_app.bot.send_document(
                                chat_id=TARGET_CHAT_ID,
                                message_thread_id=final_topic_id,
                                document=media_buffer,
                                caption=text_content,
                                parse_mode=None
                            )
                    else:
                        if text_content:
                            await bot_app.bot.send_message(
                                chat_id=TARGET_CHAT_ID,
                                message_thread_id=final_topic_id,
                                text=text_content + "\n[Неподдерживаемый тип медиа]"
                            )
                except Exception as media_err:
                    print(f"⚠️ Ошибка отправки медиа: {media_err}. Пробуем отправить текст.")
                    if text_content:
                         await bot_app.bot.send_message(
                            chat_id=TARGET_CHAT_ID,
                            message_thread_id=final_topic_id,
                            text=text_content + "\n[Ошибка загрузки медиа]"
                        )
            
            # === ТОЛЬКО ТЕКСТ ===
            elif text_content:
                await bot_app.bot.send_message(
                    chat_id=TARGET_CHAT_ID,
                    message_thread_id=final_topic_id,
                    text=text_content,
                    parse_mode=None
                )
            
            print(f"✅ Переслано в тему {final_topic_id}")
            
        except Exception as e:
            print(f"❌ Ошибка отправки в тему: {e}")

# ====== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======

async def get_source_thread_info(client, message):
    thread_id = None
    thread_name = None

    if message.reply_to and message.reply_to.forum_topic:
        thread_id = message.reply_to.reply_to_msg_id
        try:
            top_messages = await client.get_messages(message.peer_id, ids=[thread_id])
            if top_messages:
                top_msg = top_messages[0]
                if top_msg and top_msg.action and isinstance(top_msg.action, MessageActionTopicCreate):
                    thread_name = top_msg.action.title
                elif top_msg and top_msg.message:
                    thread_name = top_msg.message[:30]
        except Exception: pass
    
    return thread_id, thread_name

# ====== ОБРАБОТЧИКИ ======

async def all_message_handler(event):
    try:
        msg = event.message
        if not msg.message and not msg.media: return

        chat = await event.get_chat()
        chat_id = getattr(chat, 'id', None)

        # ====== ЛОГИКА ФИЛЬТРАЦИИ ИСТОЧНИКОВ ======
        if ONLY_WHITELIST:
            # Проверяем, есть ли ID текущего чата в списке разрешенных
            if chat_id not in ALLOWED_SOURCES:
                # Если это не наш канал, просто игнорируем сообщение
                return
        # ==========================================

        sender = await event.get_sender()
        await CSVManager.register_chat(chat)

        chat_title = getattr(chat, 'title', '') or getattr(chat, 'first_name', '') or 'Private Chat'
        sender_id = getattr(sender, 'id', 'N/A')
        
        if sender_id in EXCLUDED_SENDERS: return

        source_thread_id, source_thread_name = await get_source_thread_info(event.client, msg)

        print(f"🎯 Входящее (РАЗРЕШЕНО) от {chat_title}: {msg.message[:20]}...")

        await ForumManager.send_to_topic(
            msg, chat_id, chat_title, source_thread_id, source_thread_name
        )
        
    except Exception as e:
        print(f'Handler exception: {e}')
        
        
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ALLOWED_CHAT_ID: return
    await update.message.reply_text("✅ Бот активен")

async def restrict_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ALLOWED_CHAT_ID:
        await update.message.reply_text("❌ Только для ShakerupParser.")

# ====== STARTUP ======

async def start_telethon():
    global client
    client = TelegramClient('support_session', API_ID, API_HASH)
    client.add_event_handler(all_message_handler, events.NewMessage(incoming=True))
    await client.start()
    print("👤 Telethon запущен")
    return client

async def start_bot():
    global bot_app
    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(MessageHandler(filters.ALL & ~filters.Chat(chat_id=ALLOWED_CHAT_ID), restrict_all_messages))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    return bot_app

async def shutdown():
    global running, bot_app, client
    running = False
    if bot_app: await bot_app.updater.stop(); await bot_app.stop(); await bot_app.shutdown()
    if client: await client.disconnect()

def signal_handler(signum, frame):
    asyncio.create_task(shutdown())

async def main():
    global bot_app, client
    CSVManager.ensure_csv()
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        await start_bot()
        client = await start_telethon()
        print("🚀 Система работает. Имена: 'Топик | Канал'. Без лишнего текста.")
        while running:
            await asyncio.sleep(1)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await shutdown()

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
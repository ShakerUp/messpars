import asyncio
import os
import sys
import json
import io
import sqlite3
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.tl.types import (
    User, Chat, Channel, MessageActionTopicCreate, 
    MessageMediaPhoto, MessageMediaDocument
)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

# ====== КОНФИГУРАЦИЯ ======
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID'))
ADMIN_ID = 684460638  

TOPICS_DB_FILE = 'topics_mapping.json'
DB_FILE = 'bot_data.db'

# Лимит на размер файла (50 МБ), чтобы защитить оперативную память сервера
MAX_FILE_SIZE = 50 * 1024 * 1024 

client = None
bot_app = None

SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), TARGET_CHAT_ID] + SYSTEM_IDS
EXCLUDED_TOPICS = [1]

# ====== DATABASE (LOG EDITS) ======
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

# ====== TOPIC MANAGER ======
class TopicManager:
    @staticmethod
    def load_db():
        if not os.path.exists(TOPICS_DB_FILE): return {}
        try:
            with open(TOPICS_DB_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @staticmethod
    def save_db(db):
        with open(TOPICS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2, ensure_ascii=False)

    @staticmethod
    def get_status(chat_id, s_tid=0):
        db = TopicManager.load_db()
        chat_data = db.get(str(chat_id))
        if not chat_data: return "new"
        if not chat_data.get('enabled', True): return "paused"
        
        t_key = str(s_tid or 0)
        topic_data = chat_data.get('topics', {}).get(t_key)
        if topic_data and not topic_data.get('enabled', True): return "paused"
        
        return "active" if (topic_data and topic_data.get('topic_id')) else "active_need_topic"

    @staticmethod
    def register_source(chat_id, title, chat_type, s_tid=0, s_tname=None, target_tid=None):
        db = TopicManager.load_db()
        c_key, t_key = str(chat_id), str(s_tid or 0)
        
        if c_key not in db:
            default_enabled = False if chat_type == "private" else True
            db[c_key] = {"title": title, "type": chat_type, "enabled": default_enabled, "topics": {}}
        
        existing_topic = db[c_key]["topics"].get(t_key, {})
        db[c_key]["topics"][t_key] = {
            "topic_id": target_tid or existing_topic.get('topic_id'),
            "title": s_tname or existing_topic.get('title') or ("Личка" if chat_type == "private" else (f"Thread {t_key}" if t_key != "0" else "Main")),
            "enabled": existing_topic.get('enabled', True)
        }
        TopicManager.save_db(db)

# ====== ПАНЕЛЬ УПРАВЛЕНИЯ (UI) ======
async def show_manage_menu(query, cid, db):
    cdata = db.get(str(cid))
    if not cdata:
        await query.edit_message_text("Ошибка: данные не найдены.")
        return
    
    is_private = cdata.get('type') == 'private'
    text = (
        f"⚙️ **Управление:** {cdata['title']}\n"
        f"ID: `{cid}`\n\n"
        f"Статус чата: {'✅ ВКЛЮЧЕН' if cdata['enabled'] else '⏸ ПАУЗА'}"
    )
    
    keyboard = [[InlineKeyboardButton(f"{'🔴 ВЫКЛЮЧИТЬ ЧАТ' if cdata['enabled'] else '🟢 ВКЛЮЧИТЬ ЧАТ'}", callback_data=f"tgc_{cid}")]]
    
    if not is_private:
        keyboard.append([InlineKeyboardButton("--- Ветки чата ---", callback_data="none")])
        for tid, tdata in cdata['topics'].items():
            t_status = "🟢" if tdata['enabled'] else "🔴"
            keyboard.append([InlineKeyboardButton(f"{t_status} {tdata['title']}", callback_data=f"tgt_{cid}_{tid}")])
    
    back_target = "list_privates" if is_private else "list_groups"
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_target)])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    keyboard = [
        [InlineKeyboardButton("👥 ГРУППЫ И КАНАЛЫ", callback_data="list_groups")],
        [InlineKeyboardButton("👤 ЛИЧНЫЕ СООБЩЕНИЯ", callback_data="list_privates")]
    ]
    text = "📂 **Главное меню:**\nВыберите категорию источников:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID or query.data == "none":
        await query.answer(); return
    
    await query.answer()
    db = TopicManager.load_db()
    data = query.data

    if data in ["list_groups", "list_privates"]:
        target_is_private = (data == "list_privates")
        keyboard = []
        for cid, d in db.items():
            is_private = (d.get('type') == 'private')
            if is_private == target_is_private:
                status = "✅" if d['enabled'] else "⏸"
                keyboard.append([InlineKeyboardButton(f"{status} {d['title']}", callback_data=f"manage_{cid}")])
        keyboard.append([InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")])
        await query.edit_message_text(f"📂 **Список: {'Лички' if target_is_private else 'Группы'}**", 
                                    reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif data.startswith("manage_"):
        await show_manage_menu(query, data.split("_")[1], db)

    elif data.startswith("tgc_"):
        cid = data.split("_")[1]
        db[cid]['enabled'] = not db[cid]['enabled']
        TopicManager.save_db(db)
        await show_manage_menu(query, cid, db)

    elif data.startswith("tgt_"):
        _, cid, tid = data.split("_")
        db[cid]['topics'][tid]['enabled'] = not db[cid]['topics'][tid]['enabled']
        TopicManager.save_db(db)
        await show_manage_menu(query, cid, db)

    elif data == "main_menu":
        await cmd_list(update, context)

# ====== FORUM MANAGER ======
class ForumManager:
    @staticmethod
    async def topic_exists(topic_id):
        if not topic_id or int(topic_id) <= 1: return False
        try:
            await bot_app.bot.edit_forum_topic(chat_id=TARGET_CHAT_ID, message_thread_id=int(topic_id))
            return True
        except: return False

    @staticmethod
    async def create_topic(chat_id, chat_title, s_tname=None, s_tid=None):
        try:
            name = (f"{s_tname} | {chat_title}" if s_tname else f"💬 {chat_title}")[:120]
            res = await bot_app.bot.create_forum_topic(chat_id=TARGET_CHAT_ID, name=name)
            tid = res.message_thread_id
            await bot_app.bot.send_message(chat_id=TARGET_CHAT_ID, message_thread_id=tid, text=f"📢 {name}\nID: {chat_id}")
            return tid
        except Exception as e:
            print(f"❌ Ошибка создания темы: {e}"); return None

# ====== ОБРАБОТЧИК СООБЩЕНИЙ ======
async def telethon_handler(event):
    msg = event.message
    if msg.sender_id in EXCLUDED_SENDERS: return
    chat = await event.get_chat()
    
    is_private = isinstance(chat, User)
    chat_type = "private" if is_private else ("channel" if getattr(chat, 'broadcast', False) else "group")
    title = getattr(chat, 'title', getattr(chat, 'first_name', 'User'))
    
    s_tid = 0
    if msg.reply_to and msg.reply_to.forum_topic:
        s_tid = msg.reply_to.reply_to_msg_id

    status = TopicManager.get_status(chat.id, s_tid)
    if status == "paused": return

    db = TopicManager.load_db()
    c_key, t_key = str(chat.id), str(s_tid)
    target_tid = db.get(c_key, {}).get('topics', {}).get(t_key, {}).get('topic_id')

    async def ensure_topic():
        nonlocal target_tid
        s_tname = None
        if s_tid != 0:
            try:
                m = await event.client.get_messages(chat.id, ids=[s_tid])
                if m and m[0].action: s_tname = m[0].action.title
            except: pass
        new_tid = await ForumManager.create_topic(chat.id, title, s_tname, s_tid)
        if new_tid:
            TopicManager.register_source(chat.id, title, chat_type, s_tid, s_tname, new_tid)
            target_tid = new_tid
        return new_tid

    if not target_tid or not await ForumManager.topic_exists(target_tid):
        if status == "new" and is_private:
            TopicManager.register_source(chat.id, title, "private", s_tid)
            print(f"📥 Новое ЛС [{title}] зарегистрировано (выключено).")
            return
        if not await ensure_topic(): return

    for attempt in range(2):
        try:
            caption = msg.message or ""
            params = {"chat_id": TARGET_CHAT_ID, "message_thread_id": target_tid, "caption": caption}
            
            if msg.media:
                # --- ПРОВЕРКА РАЗМЕРА ФАЙЛА ---
                f_size = 0
                f_name = "file"
                if hasattr(msg.media, 'document') and msg.media.document:
                    f_size = msg.media.document.size
                    for attr in msg.media.document.attributes:
                        if hasattr(attr, 'file_name'):
                            f_name = attr.file_name
                elif hasattr(msg.media, 'photo') and msg.media.photo:
                    # У фото берем размер самого большого варианта
                    f_size = msg.media.photo.sizes[-1].size if hasattr(msg.media.photo, 'sizes') else 0

                if f_size > MAX_FILE_SIZE:
                    size_mb = round(f_size / (1024 * 1024), 2)
                    print(f"⚠️ Файл слишком большой ({size_mb} MB). Пропуск.")
                    await bot_app.bot.send_message(
                        chat_id=TARGET_CHAT_ID, 
                        message_thread_id=target_tid,
                        text=f"⚠️ Системой пропущен тяжелый файл: {size_mb} MB\n(Лимит безопасности: {MAX_FILE_SIZE // (1024*1024)} MB)"
                    )
                    return # Прерываем, чтобы не забивать RAM
                # -------------------------------

                buf = io.BytesIO()
                await msg.download_media(file=buf)
                buf.seek(0)
                buf.name = f_name 
                
                if isinstance(msg.media, MessageMediaPhoto):
                    sent = await bot_app.bot.send_photo(photo=buf, **params)
                else:
                    sent = await bot_app.bot.send_document(document=buf, **params)
            else:
                sent = await bot_app.bot.send_message(chat_id=TARGET_CHAT_ID, text=msg.message, message_thread_id=target_tid)
            
            DB.save(msg.id, sent.message_id, target_tid)
            break
        except Exception as e:
            err = str(e)
            if "Topic_deleted" in err or "Thread_id_invalid" in err:
                print(f"⚠️ Топик {target_tid} удален. Пересоздание...")
                if await ensure_topic(): continue
                else: break
            else:
                print(f"❌ Ошибка пересылки: {e}")
                break

async def telethon_edit_handler(event):
    msg = event.message
    rel = DB.get(msg.id)
    if not rel: return
    try:
        txt = (msg.text or "") + f"\n\n(ред. {(datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%H:%M')})"
        if msg.media: await bot_app.bot.edit_message_caption(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], caption=txt)
        else: await bot_app.bot.edit_message_text(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], text=txt)
    except: pass

async def main():
    global client, bot_app
    DB.init()
    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("list", cmd_list))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))
    
    await bot_app.initialize(); await bot_app.start()
    client = TelegramClient('support_session', API_ID, API_HASH)
    client.add_event_handler(telethon_handler, events.NewMessage())
    client.add_event_handler(telethon_edit_handler, events.MessageEdited())
    await client.start()

    print(f"🚀 Бот запущен! Админ: {ADMIN_ID}")
    
    async with bot_app:
        await bot_app.updater.start_polling()
        await client.run_until_disconnected()
        await bot_app.updater.stop()

if __name__ == "__main__":
    if sys.platform.startswith('win'): asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
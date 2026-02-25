import asyncio
import os
import sys
import json
import io
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from telethon import TelegramClient, events
from telethon.tl.types import (
    User, Chat, Channel, MessageActionTopicCreate, 
    MessageMediaPhoto, MessageMediaDocument
)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
DB_FILE = 'bot_data.db'
MAX_FILE_SIZE = 50 * 1024 * 1024 

client = None
bot_app = None

SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), DEFAULT_TARGET_CHAT_ID] + SYSTEM_IDS

# ====== DATABASE ======
class DB:
    @staticmethod
    def init():
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS msg_map (src_id INTEGER PRIMARY KEY, tgt_id INTEGER, tid INTEGER, custom_target_id INTEGER)')

    @staticmethod
    def save(src_id, tgt_id, tid):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('INSERT OR REPLACE INTO msg_map (src_id, tgt_id, tid) VALUES (?, ?, ?)', (src_id, tgt_id, tid))

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
            db[c_key] = {"title": title, "type": chat_type, "enabled": default_enabled, "custom_target_id": None, "topics": {}}
        
        existing_topic = db[c_key]["topics"].get(t_key, {})
        db[c_key]["topics"][t_key] = {
            "topic_id": target_tid or existing_topic.get('topic_id'),
            "title": s_tname or existing_topic.get('title') or ("Личка" if chat_type == "private" else (f"Thread {t_key}" if t_key != "0" else "Main")),
            "enabled": existing_topic.get('enabled', True)
        }
        TopicManager.save_db(db)

# ====== ИНТЕРФЕЙС УПРАВЛЕНИЯ ======
async def show_manage_menu(query, cid, db):
    cdata = db.get(str(cid))
    if not cdata: return
    is_private = cdata.get('type') == 'private'
    custom_target = cdata.get('custom_target_id') or "По умолчанию (из .env)"
    
    text = f"⚙️ **Управление:** {cdata['title']} (`{cid}`)\n\n"
    text += f"Статус: {'✅ ВКЛ' if cdata['enabled'] else '⏸ ПАУЗА'}\n"
    text += f"🎯 Куда шлем: `{custom_target}`\n\n"
    text += "🔍 `[Статус] Имя (ID источника) ➡️ ID топика`"
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 ВЫКЛЮЧИТЬ ЧАТ' if cdata['enabled'] else '🟢 ВКЛЮЧИТЬ ЧАТ'}", callback_data=f"tgc_{cid}")],
        [InlineKeyboardButton("🎯 ИЗМЕНИТЬ КАНАЛ НАЗНАЧЕНИЯ", callback_data=f"editchat_{cid}")]
    ]
    
    if not is_private:
        keyboard.append([InlineKeyboardButton("--- Настройка веток ---", callback_data="none")])
        for tid, tdata in cdata.get('topics', {}).items():
            t_status = "🟢" if tdata['enabled'] else "🔴"
            t_title = tdata.get('title', 'Без названия')
            target_id = tdata.get('topic_id', '???')
            btn_display = f"{t_status} {t_title} ({tid}) ➡️ {target_id}"
            keyboard.append([
                InlineKeyboardButton(btn_display, callback_data=f"editid_{cid}_{tid}"),
                InlineKeyboardButton("❌", callback_data=f"del_{cid}_{tid}")
            ])

    back_target = "list_privates" if is_private else "list_groups"
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_target)])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    keyboard = [[InlineKeyboardButton("👥 ГРУППЫ И КАНАЛЫ", callback_data="list_groups")], 
                [InlineKeyboardButton("👤 ЛИЧНЫЕ СООБЩЕНИЯ", callback_data="list_privates")]]
    text = "📂 **Главное меню:**"
    if update.callback_query: await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else: await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID or query.data == "none": await query.answer(); return
    await query.answer(); db = TopicManager.load_db(); data = query.data

    if data in ["list_groups", "list_privates"]:
        target_priv = (data == "list_privates")
        kb = [[InlineKeyboardButton(f"{'✅' if d['enabled'] else '⏸'} {d['title']}", callback_data=f"manage_{cid}")] 
              for cid, d in db.items() if (d.get('type') == 'private') == target_priv]
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")])
        await query.edit_message_text(f"📂 **Список: {'Лички' if target_priv else 'Группы'}**", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
    
    elif data.startswith("manage_"): await show_manage_menu(query, data.split("_")[1], db)
    
    elif data.startswith("tgc_"):
        cid = data.split("_")[1]; db[cid]['enabled'] = not db[cid]['enabled']; TopicManager.save_db(db)
        await show_manage_menu(query, cid, db)

    elif data.startswith("editchat_"):
        cid = data.split("_")[1]
        user_edit_state[query.from_user.id] = {"mode": "target_chat", "cid": cid}
        await query.message.reply_text(f"📝 Введите **ID нового канала**, куда пересылать сообщения из этого источника.\nЧтобы вернуть стандартный канал, введите `0`.")

    elif data.startswith("editid_"):
        _, cid, tid = data.split("_")
        user_edit_state[query.from_user.id] = {"mode": "topic_id", "cid": cid, "tid": tid}
        await query.message.reply_text(f"📝 Введите новый **Target ID** (ID топика) для ветки `{tid}`:")

    elif data.startswith("del_"):
        _, cid, tid = data.split("_")
        if cid in db and tid in db[cid].get('topics', {}):
            del db[cid]['topics'][tid]
            TopicManager.save_db(db)
            await show_manage_menu(query, cid, db)
            
    elif data == "main_menu": await cmd_list(update, context)

async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_edit_state: return

    state = user_edit_state.pop(user_id)
    new_input = update.message.text.strip()
    db = TopicManager.load_db()
    cid = state["cid"]

    if state["mode"] == "target_chat":
        if new_input == "0":
            db[cid]['custom_target_id'] = None
            text = "✅ Теперь используются настройки по умолчанию."
        else:
            try:
                db[cid]['custom_target_id'] = int(new_input)
                text = f"✅ Сообщения из этого источника теперь будут лететь в канал `{new_input}`"
            except:
                await update.message.reply_text("❌ Ошибка: Введите корректный ID (число).")
                return
    
    elif state["mode"] == "topic_id":
        tid = state["tid"]
        if new_input.isdigit():
            db[cid]['topics'][tid]['topic_id'] = int(new_input)
            text = f"✅ Новый Target ID для ветки `{tid}` установлен: `{new_input}`"
        else:
            await update.message.reply_text("❌ Ошибка: Введите число.")
            return

    TopicManager.save_db(db)
    await update.message.reply_text(text + "\nИспользуйте /list для управления.")

# ====== FORUM MANAGER ======
class ForumManager:
    @staticmethod
    async def create_topic(target_chat, chat_title, s_tname=None):
        try:
            name = (f"{s_tname} | {chat_title}" if s_tname else f"💬 {chat_title}")[:120]
            res = await bot_app.bot.create_forum_topic(chat_id=target_chat, name=name)
            tid = res.message_thread_id
            logger.info(f"[FORUM] Создан новый топик '{name}' ID: {tid} в чате {target_chat}")
            return tid
        except Exception as e: 
            logger.error(f"[FORUM ERROR] Ошибка создания топика: {e}")
            return None

# ====== ОБРАБОТЧИК СООБЩЕНИЙ ======
async def telethon_handler(event):
    msg = event.message
    if msg.sender_id in EXCLUDED_SENDERS: return
    
    chat = await event.get_chat()
    chat_title = getattr(chat, 'title', getattr(chat, 'first_name', 'Unknown'))
    is_private = isinstance(chat, User)
    chat_type = "private" if is_private else ("channel" if getattr(chat, 'broadcast', False) else "group")
    
    db_data = TopicManager.load_db()
    chat_id_str = str(chat.id)
    chat_conf = db_data.get(chat_id_str, {})
    
    # ОПРЕДЕЛЕНИЕ ТАРГЕТ КАНАЛА
    final_target_chat = chat_conf.get('custom_target_id') or DEFAULT_TARGET_CHAT_ID

    source_top_id = 0
    reply_to_target_id = None
    
    if msg.reply_to:
        source_top_id = msg.reply_to.reply_to_top_id or 0
        mapping = DB.get(msg.reply_to.reply_to_msg_id)
        if mapping: reply_to_target_id = mapping['tgt_id']

    target_tid = chat_conf.get('topics', {}).get(str(source_top_id), {}).get('topic_id')
    if not target_tid and not is_private:
        target_tid = chat_conf.get('topics', {}).get("0", {}).get('topic_id')

    status = TopicManager.get_status(chat.id, source_top_id)
    if status == "paused":
        logger.info(f"[SKIP] Чат/Ветка {chat_id_str}/{source_top_id} на паузе")
        return

    # Создание топика если его нет
    if not target_tid:
        if status == "new" and is_private:
            logger.info(f"[NEW] Новая личка {chat_id_str}, регистрирую...")
            TopicManager.register_source(chat.id, chat_title, "private", 0)
            return
        
        logger.info(f"[AUTO] Топик не найден для {chat_title} ({source_top_id}). Создаю в {final_target_chat}...")
        new_tid = await ForumManager.create_topic(final_target_chat, chat_title)
        if not new_tid: 
            logger.error("[FATAL] Не удалось создать топик. Прерываю отправку.")
            return
        target_tid = new_tid
        TopicManager.register_source(chat.id, chat_title, chat_type, source_top_id, target_tid=new_tid)

    success = False
    logger.info(f"[SENDING] Message {msg.id} from {chat_title} ➡️ Channel {final_target_chat}, Topic {target_tid}")
    
    for attempt in range(2):
        try:
            current_reply_id = reply_to_target_id if attempt == 0 else None
            
            # Базовые аргументы (всегда общие)
            send_kwargs = {
                "chat_id": final_target_chat,
                "message_thread_id": int(target_tid),
                "reply_to_message_id": current_reply_id,
            }

            if msg.media:
                # Для медиа добавляем caption
                send_kwargs["caption"] = msg.message or ""
                
                buf = io.BytesIO()
                await msg.download_media(file=buf)
                buf.seek(0)
                buf.name = getattr(msg.file, 'name', 'file') or 'file'

                if isinstance(msg.media, MessageMediaPhoto):
                    sent = await bot_app.bot.send_photo(photo=buf, **send_kwargs)
                elif any(hasattr(a, 'voice') and a.voice for a in getattr(msg.media.document, 'attributes', [])) if hasattr(msg.media, 'document') else False:
                    sent = await bot_app.bot.send_voice(voice=buf, **send_kwargs)
                else:
                    sent = await bot_app.bot.send_document(document=buf, **send_kwargs)
            else:
                # Для обычного текста используем аргумент 'text', а не 'caption'
                sent = await bot_app.bot.send_message(text=msg.message, **send_kwargs)

            DB.save(msg.id, sent.message_id, int(target_tid))
            logger.info(f"[SUCCESS] Message {msg.id} forwarded as {sent.message_id}")
            success = True
            break
        except Exception as e:
            err_str = str(e)
            if ("reply" in err_str.lower() or "Message to be replied not found" in err_str) and attempt == 0:
                logger.warning(f"[RETRY] Ошибка Reply, пробую без него для {msg.id}")
                continue
            else:
                logger.error(f"[SEND ERROR] Попытка {attempt+1} провалена для {msg.id}: {e}")
                break

    if not success:
        logger.error(f"[FATAL] Не удалось отправить сообщение {msg.id} после всех попыток")

async def telethon_edit_handler(event):
    msg = event.message
    rel = DB.get(msg.id)
    if not rel: return
    
    db_data = TopicManager.load_db()
    chat_id_str = str(event.chat_id)
    target_chat = db_data.get(chat_id_str, {}).get('custom_target_id') or DEFAULT_TARGET_CHAT_ID
    
    try:
        txt = (msg.text or "") + f"\n\n(ред. {(datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%H:%M')})"
        if msg.media:
            await bot_app.bot.edit_message_caption(chat_id=target_chat, message_id=rel["tgt_id"], caption=txt)
        else:
            await bot_app.bot.edit_message_text(chat_id=target_chat, message_id=rel["tgt_id"], text=txt)
        logger.info(f"[EDIT] Сообщение {rel['tgt_id']} обновлено")
    except Exception as e:
        logger.error(f"[EDIT ERROR] Не удалось отредактировать {rel['tgt_id']}: {e}")

async def main():
    global client, bot_app
    DB.init()
    bot_app = ApplicationBuilder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("list", cmd_list))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text))
    
    await bot_app.initialize()
    await bot_app.start()

    client = TelegramClient('support_session', API_ID, API_HASH)
    client.add_event_handler(telethon_handler, events.NewMessage())
    client.add_event_handler(telethon_edit_handler, events.MessageEdited())
    
    await client.start()
    logger.info("🚀 Бот запущен. Ошибка 'caption' исправлена.")

    async with bot_app:
        await bot_app.updater.start_polling()
        await client.run_until_disconnected()

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
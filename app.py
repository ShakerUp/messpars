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
from telegram.ext import MessageHandler, filters
from telethon.tl.types import (
    User, Chat, Channel, MessageActionTopicCreate, 
    MessageMediaPhoto, MessageMediaDocument
)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

# ====== НАСТРОЙКА ЛОГИРОВАНИЯ (ТОЛЬКО ДАННЫЕ) ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    handlers=[
        logging.FileHandler("bot_messages.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Состояние редактирования для админа
user_edit_state = {}

# Полностью глушим системные логи библиотек
logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("telegram").setLevel(logging.CRITICAL)
logging.getLogger("telethon").setLevel(logging.CRITICAL)

# ====== КОНФИГУРАЦИЯ ======
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHAT_ID = int(os.getenv('TARGET_CHAT_ID'))
ADMIN_ID = 684460638  

TOPICS_DB_FILE = 'topics_mapping.json'
DB_FILE = 'bot_data.db'
MAX_FILE_SIZE = 50 * 1024 * 1024 

client = None
bot_app = None

SYSTEM_IDS = [777000, 1000, 1087968824]
EXCLUDED_SENDERS = [int(BOT_TOKEN.split(':')[0]), TARGET_CHAT_ID] + SYSTEM_IDS

# ====== DATABASE ======
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

# ====== ИНТЕРФЕЙС УПРАВЛЕНИЯ ======
async def show_manage_menu(query, cid, db):
    cdata = db.get(str(cid))
    if not cdata: return
    is_private = cdata.get('type') == 'private'
    
    # Заголовок с названием и ID чата
    text = f"⚙️ **Управление:** {cdata['title']} (`{cid}`)\n\n"
    text += f"Статус: {'✅ ВКЛ' if cdata['enabled'] else '⏸ ПАУЗА'}\n"
    text += "🔍 Формат: `[Статус] Имя (ID источника) ➡️ ID топика`"
    
    keyboard = [[InlineKeyboardButton(f"{'🔴 ВЫКЛЮЧИТЬ ЧАТ' if cdata['enabled'] else '🟢 ВКЛЮЧИТЬ ЧАТ'}", callback_data=f"tgc_{cid}")]]
    
    if not is_private:
        keyboard.append([InlineKeyboardButton("--- Настройка веток ---", callback_data="none")])
        for tid, tdata in cdata.get('topics', {}).items():
            t_status = "🟢" if tdata['enabled'] else "🔴"
            t_title = tdata.get('title', 'Без названия')
            target_id = tdata.get('topic_id', '???')
            
            # Кнопка отображает: [Статус] Название (ID источника) -> ID топика
            # При нажатии на нее сработает 'editid', чтобы можно было изменить Target ID
            btn_display = f"{t_status} {t_title} ({tid}) ➡️ {target_id}"
            
            btn_toggle = InlineKeyboardButton(btn_display, callback_data=f"editid_{cid}_{tid}")
            btn_delete = InlineKeyboardButton("❌", callback_data=f"del_{cid}_{tid}")
            
            keyboard.append([btn_toggle, btn_delete])
            
    back_target = "list_privates" if is_private else "list_groups"
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_target)])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    keyboard = [[InlineKeyboardButton("👥 ГРУППЫ И КАНАЛЫ", callback_data="list_groups")], [InlineKeyboardButton("👤 ЛИЧНЫЕ СООБЩЕНИЯ", callback_data="list_privates")]]
    text = "📂 **Главное меню:**"
    if update.callback_query: await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else: await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID or query.data == "none": await query.answer(); return
    await query.answer(); db = TopicManager.load_db(); data = query.data
    if data in ["list_groups", "list_privates"]:
        target_priv = (data == "list_privates")
        kb = [[InlineKeyboardButton(f"{'✅' if d['enabled'] else '⏸'} {d['title']}", callback_data=f"manage_{cid}")] for cid, d in db.items() if (d.get('type') == 'private') == target_priv]
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")])
        await query.edit_message_text(f"📂 **Список: {'Лички' if target_priv else 'Группы'}**", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
    elif data.startswith("manage_"): await show_manage_menu(query, data.split("_")[1], db)
    elif data.startswith("tgc_"):
        cid = data.split("_")[1]; db[cid]['enabled'] = not db[cid]['enabled']; TopicManager.save_db(db)
        await show_manage_menu(query, cid, db)
    elif data.startswith("tgt_"):
        _, cid, tid = data.split("_"); db[cid]['topics'][tid]['enabled'] = not db[cid]['topics'][tid]['enabled']; TopicManager.save_db(db)
        await show_manage_menu(query, cid, db)
    elif data.startswith("editid_"):
        _, cid, tid = data.split("_")
        # Сохраняем в память, что админ хочет поменять ID для конкретной ветки
        user_edit_state[query.from_user.id] = {"cid": cid, "tid": tid}
        await query.message.reply_text(
            f"📥 **Настройка ветки `{tid}`**\n\n"
            f"Введите новый **ID топика** в вашем канале, куда нужно зеркалить сообщения.\n"
            f"Чтобы просто включить/выключить ветку, введите `on` или `off`."
        )
        await query.message.reply_text(
            f"📝 Введите новый **Target ID** (ID топика в вашем канале) для ветки `{tid}`:\n"
            "Чтобы отменить, просто введите что-то другое или используйте /list"
        )
    elif data.startswith("del_"):
        _, cid, tid = data.split("_")
        cid, tid = str(cid), str(tid)
        if cid in db and tid in db[cid].get('topics', {}):
            # Удаляем конкретный топик из маппинга
            deleted_topic_name = db[cid]['topics'][tid].get('title', tid)
            del db[cid]['topics'][tid]
            TopicManager.save_db(db)
            # Опционально: можно отправить уведомление
            logger.info(f"[ADMIN] Удален маппинг топика: {deleted_topic_name} (Source ID: {tid})")
            # Обновляем меню
            await show_manage_menu(query, cid, db)
    elif data == "main_menu": await cmd_list(update, context)


async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Проверяем, что пишет админ и что он нажал кнопку редактирования
    if user_id != ADMIN_ID or user_id not in user_edit_state:
        return

    state = user_edit_state.pop(user_id) # Завершаем режим редактирования
    new_input = update.message.text.strip().lower()
    
    db = TopicManager.load_db()
    cid, tid = state["cid"], state["tid"]

    if cid not in db or tid not in db[cid]["topics"]:
        await update.message.reply_text("❌ Ошибка: Ветка не найдена в базе.")
        return

    # Логика вкл/выкл или смена ID
    if new_input in ['on', 'off']:
        db[cid]['topics'][tid]['enabled'] = (new_input == 'on')
        text = f"✅ Ветка `{tid}` {'включена' if new_input == 'on' else 'выключена'}."
    elif new_input.isdigit():
        db[cid]['topics'][tid]['topic_id'] = int(new_input)
        text = f"✅ Новый Target ID для ветки `{tid}` установлен: `{new_input}`"
    else:
        await update.message.reply_text("❌ Ошибка: Введите число (ID) или on/off. Настройка отменена.")
        return

    TopicManager.save_db(db)
    await update.message.reply_text(text)
    # Показываем меню снова, чтобы видеть изменения
    # Для этого нам нужен query, но в текстовом хендлере его нет, 
    # так что просто отправляем новое сообщение
    await update.message.reply_text("Используйте /list для возврата в меню.")
    
    
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
            return None

# ====== ОБРАБОТЧИК СООБЩЕНИЙ ======
async def telethon_handler(event):
    msg = event.message
    if msg.sender_id in EXCLUDED_SENDERS: return
    
    chat = await event.get_chat()
    chat_title = getattr(chat, 'title', getattr(chat, 'first_name', 'Unknown'))
    is_private = isinstance(chat, User)
    chat_type = "private" if is_private else ("channel" if getattr(chat, 'broadcast', False) else "group")
    
    target_tid = None
    reply_to_target_id = None
    source_top_id = 0

    # --- 1. ОПРЕДЕЛЕНИЕ ИСТОЧНИКА И ВИЗУАЛЬНОЙ СВЯЗИ ---
    if msg.reply_to:
        # Пытаемся взять ID ветки напрямую из сообщения
        source_top_id = msg.reply_to.reply_to_top_id or 0
        parent_msg_id = msg.reply_to.reply_to_msg_id
        
        # Ищем в БД ТОЛЬКО для того, чтобы привязать ответ (Reply)
        mapping = DB.get(parent_msg_id)
        if mapping:
            reply_to_target_id = mapping['tgt_id']
            # ВАЖНО: Мы НЕ берем target_tid из mapping здесь, 
            # чтобы не перебивать актуальный конфиг из JSON.
        
        # Если source_top_id не определен (ответ в обычном чате), запрашиваем API
        if not source_top_id:
            try:
                reply_obj = await event.get_reply_message()
                if reply_obj and reply_obj.reply_to:
                    source_top_id = reply_obj.reply_to.reply_to_top_id or reply_obj.reply_to.reply_to_msg_id or 0
                else:
                    source_top_id = parent_msg_id
            except Exception as e:
                logger.warning(f"API Fallback error: {e}")

    # --- 2. ЖЕСТКИЙ ПРИОРИТЕТ АКТУАЛЬНОГО КОНФИГА (JSON) ---
    db_data = TopicManager.load_db()
    chat_data = db_data.get(str(chat.id), {})
    
    # ВСЕГДА берем топик из конфига, ориентируясь на source_top_id
    target_tid = chat_data.get('topics', {}).get(str(source_top_id), {}).get('topic_id')

    # Если в конфиге этой ветки нет, пробуем Main (0)
    if not target_tid and not is_private:
        target_tid = chat_data.get('topics', {}).get("0", {}).get('topic_id')
    
    # Только если в конфиге ВООБЩЕ ничего не найдено, берем из базы (как последний шанс)
    if not target_tid and msg.reply_to:
        mapping = DB.get(msg.reply_to.reply_to_msg_id)
        if mapping:
            target_tid = mapping['tid']
            logger.info(f"[FALLBACK] В конфиге нет ветки {source_top_id}, шлю в старый топик {target_tid}")

    # --- 3. ПРОВЕРКА СТАТУСА ---
    status = TopicManager.get_status(chat.id, source_top_id)
    if status == "paused": return

    # --- 4. СОЗДАНИЕ ТОПИКА (ЕСЛИ НУЖНО) ---
    if not target_tid:
        if status == "new" and is_private:
            TopicManager.register_source(chat.id, chat_title, "private", 0)
            return
        
        logger.info(f"Создаю новый топик для ветки {source_top_id}...")
        new_tid = await ForumManager.create_topic(chat.id, chat_title, s_tid=source_top_id)
        if not new_tid: return
        target_tid = new_tid
        TopicManager.register_source(chat.id, chat_title, chat_type, source_top_id, target_tid=new_tid)

# --- 5. ОТПРАВКА ---
    # Делаем две попытки: первая с Reply, вторая (если первая упала) — без него
    success = False
    for attempt in range(2):
        try:
            # Если это вторая попытка, принудительно убираем привязку к ответу
            current_reply_id = reply_to_target_id if attempt == 0 else None
            
            send_kwargs = {
                "chat_id": TARGET_CHAT_ID,
                "message_thread_id": int(target_tid),
                "reply_to_message_id": current_reply_id,
            }

            if msg.media:
                buf = io.BytesIO()
                await msg.download_media(file=buf)
                buf.seek(0)
                
                # Определяем имя файла
                f_name = getattr(msg.file, 'name', 'file') or 'file'
                buf.name = f_name

                # ПРОВЕРКА НА VOICE (Голосовое сообщение)
                is_voice = False
                if isinstance(msg.media, MessageMediaDocument):
                    for attr in msg.media.document.attributes:
                        if hasattr(attr, 'voice') and attr.voice:
                            is_voice = True
                            break

                if isinstance(msg.media, MessageMediaPhoto):
                    sent = await bot_app.bot.send_photo(photo=buf, caption=msg.message or "", **send_kwargs)
                elif is_voice:
                    # Отправляем как голосовое сообщение
                    sent = await bot_app.bot.send_voice(voice=buf, caption=msg.message or "", **send_kwargs)
                else:
                    # Отправляем как обычный документ/файл
                    sent = await bot_app.bot.send_document(document=buf, caption=msg.message or "", **send_kwargs)
            else:
                sent = await bot_app.bot.send_message(text=msg.message, **send_kwargs)

            # Если дошли сюда — отправка успешна
            DB.save(msg.id, sent.message_id, int(target_tid))
            logger.info(f"[SUCCESS] Отправлено в {target_tid} (Попытка {attempt+1})")
            success = True
            break

        except Exception as e:
            err_msg = str(e)
            if "Message to be replied not found" in err_msg or "Reply_message_id_invalid" in err_msg:
                logger.warning(f"[RETRY] Сообщение для ответа удалено в топике {target_tid}. Пробую отправить без Reply...")
                continue 
            else:
                logger.error(f"Ошибка финальной отправки: {e}")
                break

    if not success:
        logger.error(f"[FATAL] Не удалось отправить сообщение {msg.id} даже без Reply")

async def telethon_edit_handler(event):
    msg = event.message
    rel = DB.get(msg.id)
    if not rel: return
    try:
        txt = (msg.text or "") + f"\n\n(ред. {(datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%H:%M')})"
        if msg.media:
            await bot_app.bot.edit_message_caption(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], caption=txt)
        else:
            await bot_app.bot.edit_message_text(chat_id=TARGET_CHAT_ID, message_id=rel["tgt_id"], text=txt)
        logger.info(f"[EDIT] Updated message {rel['tgt_id']} in Target")
    except Exception as e:
        logger.error(f"[ERROR] Edit failed for {rel['tgt_id']}: {e}")

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
    logger.info("🚀 Бот запущен. Логи сетевых запросов отключены.")

    async with bot_app:
        await bot_app.updater.start_polling()
        await client.run_until_disconnected()
        await bot_app.updater.stop()

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())

import logging
import sqlite3
import datetime
import asyncio
import traceback # Import traceback for detailed error logging
import json
from telegram import Update, BotCommand, InputFile, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, WebAppInfo
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    Application,
    CallbackQueryHandler,
    JobQueue
)
from telegram.helpers import escape_markdown
from telegram.error import TelegramError, BadRequest
import random
from PIL import Image, ImageDraw, ImageFont
import io
import math # Для log2 и ceil для турнирной сетки

# --- Конфигурация и логирование ---
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Глобальные переменные и константы ---
# Состояния (теперь они будут храниться в БД, но для понимания логики пусть будут здесь)
START, SET_CODE, WAIT_READY, IN_MATCH, SET_NEW_ROUND_CODE, AWAITING_BROADCAST_MESSAGE, WAITING_FOR_TOURNAMENT, AWAITING_TOURNAMENT_DETAILS, AWAITING_EXIT_CONFIRMATION = 'START', 'SET_CODE', 'WAIT_READY', 'IN_MATCH', 'SET_NEW_ROUND_CODE', 'AWAITING_BROADCAST_MESSAGE', 'WAITING_FOR_TOURNAMENT', 'AWAITING_TOURNAMENT_DETAILS', 'AWAITING_EXIT_CONFIRMATION'
DB_PATH = 'game.db'
CHECK_INTERVAL_SECONDS = 10 # Интервал проверки таймаутов и статуса турнира (уменьшен для более быстрого отклика турнирной логики)
SHOW_ALL_USERS_CALLBACK = "show_all_users_callback"
SHOW_ALL_MATCH_IDS_CALLBACK = "show_all_match_ids_callback"

CODE_LENGTH = 5 # Определение длины кода

WAIT_READY_TIMEOUT_SECONDS = 600  # 10 минут
IN_MATCH_TIMEOUT_SECONDS = 900   # 15 минут (900 секунд)

# URL вашего Web App. ЗАМЕНИТЕ НА ВАШ РЕАЛЬНЫЙ HTTPS URL!
# Это должен быть путь к вашему index.html на GitHub Pages
WEB_APP_URL = "[https://din-stock.github.io/]" # <-- Вставьте ваш адрес здесь!

BOT_TOKEN = "" # Будет загружен из config.txt
ADMIN_USER_ID = 0 # Будет загружен из config.txt

# --- НОВЫЙ КЛАСС-ФИЛЬТР (оставлен, так как он не использует '&' в проблемном месте) ---
class WebAppFilter(filters.BaseFilter):
    def filter(self, update: Update) -> bool:
        """Проверяет, содержит ли обновление данные Web App."""
        # Проверяем, что это сообщение и что у него есть web_app_data
        return bool(update.message and update.message.web_app_data)

# Создаем экземпляр нашего фильтра
web_app_data_filter_instance = WebAppFilter()
# --- КОНЕЦ НОВОГО КЛАССА-ФИЛЬТРА ---

# --- НОВЫЕ ФУНКЦИИ-ФИЛЬТРЫ ДЛЯ ОБХОДА ОШИБКИ С ОПЕРАТОРОМ '&' ---
def filter_text_no_command(update: Update) -> bool:
    """Фильтр: текстовое сообщение, не являющееся командой."""
    return filters.TEXT.filter(update) and not filters.COMMAND.filter(update)

def filter_text_no_command_and_5_digits(update: Update) -> bool:
    """Фильтр: текстовое сообщение, не являющееся командой, и содержит 5 цифр."""
    return filters.TEXT.filter(update) and \
           not filters.COMMAND.filter(update) and \
           filters.Regex(r'^\d{5}$').filter(update)

def filter_text_no_command_and_digit_space_digit(update: Update) -> bool:
    """Фильтр: текстовое сообщение, не являющееся командой, и соответствует формату 'цифра пробел цифра'."""
    return filters.TEXT.filter(update) and \
           not filters.COMMAND.filter(update) and \
           filters.Regex(r'^\d\s+\d$').filter(update)
# --- КОНЕЦ НОВЫХ ФУНКЦИЙ-ФИЛЬТРОВ ---


# --- БАЗА ДАННЫХ ---
def init_db():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Таблица players - изменения
        cur.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT, -- Добавлено для update_user_info
                last_name TEXT,  -- Добавлено для update_user_info
                secret_code TEXT,
                state TEXT DEFAULT 'START',
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_admin INTEGER DEFAULT 0,
                is_ready INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                tournament_id INTEGER, -- ID текущего турнира, в котором участвует игрок
                current_round INTEGER DEFAULT 0, -- Текущий раунд игрока в турнире (в рамках турнира)
                is_eliminated INTEGER DEFAULT 0, -- 1, если игрок выбыл из турнира
                FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id)
            )
        ''')

        # Таблица matches - изменения
        cur.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player1_id INTEGER,
                player2_id INTEGER,
                current_player_turn INTEGER,
                secret_code_player1 TEXT,
                secret_code_player2 TEXT,
                player1_cracked_code TEXT DEFAULT '["-", "-", "-", "-", "-"]',
                player2_cracked_code TEXT DEFAULT '["-", "-", "-", "-", "-"]',
                status TEXT DEFAULT 'active', -- active, completed, cancelled, timeout
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tournament_id INTEGER, -- Связь с турниром
                round_number INTEGER, -- Раунд турнира, в котором проходит матч
                winner_id INTEGER, -- Победитель матча
                loser_id INTEGER, -- Проигравший матча
                FOREIGN KEY (player1_id) REFERENCES players(user_id),
                FOREIGN KEY (player2_id) REFERENCES players(user_id),
                FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id),
                FOREIGN KEY (winner_id) REFERENCES players(user_id),
                FOREIGN KEY (loser_id) REFERENCES players(user_id)
            )
        ''')

        # НОВАЯ ТАБЛИЦА: tournaments
        # Добавлены tournament_name и min_players
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                tournament_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_name TEXT, -- Название турнира
                status TEXT DEFAULT 'registration', -- registration, active, completed
                current_round INTEGER DEFAULT 0,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                winner_id INTEGER,
                second_place_id INTEGER,
                third_place_id INTEGER,
                admin_id INTEGER, -- кто создал турнир
                players_count INTEGER DEFAULT 0, -- Начальное количество игроков в турнире
                min_players INTEGER DEFAULT 2, -- Минимальное количество игроков для старта
                FOREIGN KEY (winner_id) REFERENCES players(user_id),
                FOREIGN KEY (second_place_id) REFERENCES players(user_id),
                FOREIGN KEY (third_place_id) REFERENCES players(user_id),
                FOREIGN KEY (admin_id) REFERENCES players(user_id)
            )
        ''')

        # НОВАЯ ТАБЛИЦА: tournament_players - для отслеживания участников турнира
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tournament_players (
                tournament_player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                user_id INTEGER,
                current_round INTEGER, -- Раунд, в котором находится игрок в рамках турнира
                is_eliminated INTEGER DEFAULT 0, -- 1, если выбыл из турнира
                is_bye INTEGER DEFAULT 0, -- 1, если получил свободный проход в этом раунде
                FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id),
                FOREIGN KEY (user_id) REFERENCES players(user_id)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS admin_messages (
                message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: admin_data для хранения ID сообщений для обновления
        cur.execute('''
            CREATE TABLE IF NOT EXISTS admin_data (
                admin_user_id INTEGER PRIMARY KEY,
                last_active_matches_msg_id INTEGER,
                last_tournament_status_msg_id INTEGER
            )
        ''')
        
        conn.commit()
        logger.info("База данных инициализирована.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД: {e}")
    finally:
        if conn:
            conn.close()

# --- Вспомогательные функции для работы с admin_data ---
def _get_admin_message_id(admin_user_id: int, message_type: str) -> int | None:
    """
    Получает ID последнего сообщения указанного типа для данного администратора.
    message_type может быть 'active_matches' или 'tournament_status'.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        column_name = f"last_{message_type}_msg_id"
        cur.execute(f'SELECT {column_name} FROM admin_data WHERE admin_user_id = ?', (admin_user_id,))
        result = cur.fetchone()
        logger.debug(f"DB Read: admin_user_id={admin_user_id}, message_type={message_type}, result={result}")
        return result[0] if result and result[0] is not None else None
    except sqlite3.Error as e:
        logger.error(f"Error reading admin message ID from DB: {e}")
        return None
    finally:
        if conn:
            conn.close()

def _set_admin_message_id(admin_user_id: int, message_type: str, message_id: int):
    """
    Сохраняет ID последнего сообщения указанного типа для данного администратора.
    message_type может быть 'active_matches' или 'tournament_status'.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Убедимся, что строка для админа существует, прежде чем обновлять ее
        cur.execute('INSERT OR IGNORE INTO admin_data (admin_user_id) VALUES (?)', (admin_user_id,))
        column_name = f"last_{message_type}_msg_id"
        cur.execute(f'UPDATE admin_data SET {column_name} = ? WHERE admin_user_id = ?', (message_id, admin_user_id))
        conn.commit()
        logger.debug(f"DB Write: admin_user_id={admin_user_id}, message_type={message_type}, message_id={message_id}")
    except sqlite3.Error as e:
        logger.error(f"Error writing admin message ID to DB: {e}")
    finally:
        if conn:
            conn.close()

async def _clear_admin_message(admin_user_id: int, message_type: str, context: ContextTypes.DEFAULT_TYPE):
    """
    Удаляет сообщение определенного типа из чата администратора и очищает его ID из БД.
    """
    message_id = _get_admin_message_id(admin_user_id, message_type)
    if message_id:
        try:
            await context.bot.delete_message(chat_id=admin_user_id, message_id=message_id)
            logger.info(f"Successfully deleted {message_type} message {message_id} for admin {admin_user_id}.")
        except BadRequest as e:
            if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
                logger.warning(f"Message {message_id} for admin {admin_user_id} ({message_type}) already deleted or cannot be deleted: {e}")
            else:
                logger.error(f"Failed to delete {message_type} message {message_id} for admin {ADMIN_USER_ID}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error deleting {message_type} message {message_id} for admin {ADMIN_USER_ID}: {e}\n{traceback.format_exc()}")
        finally:
            _set_admin_message_id(ADMIN_USER_ID, message_type, None) # Clear ID from DB regardless


# --- Вспомогательные функции ---
async def update_user_state(user_id: int, new_state: str):
    """
    Обновляет состояние пользователя в базе данных.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Используем INSERT OR IGNORE для создания записи, если её нет
        cursor.execute("INSERT OR IGNORE INTO players (user_id, state, last_activity) VALUES (?, ?, ?)",
                       (user_id, new_state, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        cursor.execute("UPDATE players SET state = ?, last_activity = ? WHERE user_id = ?", (new_state, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
        conn.commit()
        logger.info(f"Состояние пользователя {user_id} обновлено на: {new_state}")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении состояния пользователя {user_id}: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в update_user_state: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

# НОВАЯ ФУНКЦИЯ: update_user_info
def update_user_info(user_id: int, username: str | None, first_name: str | None, last_name: str | None):
    """
    Обновляет информацию о пользователе (username, first_name, last_name) в базе данных.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('''
            INSERT OR IGNORE INTO players (user_id) VALUES (?)
        ''', (user_id,)) # Убедимся, что запись существует
        cur.execute('''
            UPDATE players SET username = ?, first_name = ?, last_name = ? WHERE user_id = ?
        ''', (username, first_name, last_name, user_id))
        conn.commit()
        logger.info(f"Информация о пользователе {user_id} обновлена.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении информации о пользователе {user_id}: {e}")
    finally:
        if conn:
            conn.close()

def update_player_activity(user_id, timestamp=None):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if timestamp is None:
            timestamp = datetime.datetime.now()
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        cur.execute('UPDATE players SET last_activity = ? WHERE user_id = ?', (formatted_timestamp, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при обновлении активности игрока {user_id}: {e}")
    finally:
        if conn:
            conn.close()

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_USER_ID

# Декоратор для обновления активности пользователя
def update_activity_decorator(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user:
            update_player_activity(update.effective_user.id, datetime.datetime.now())
        return await func(update, context, *args, **kwargs)
    return wrapper

async def send_message_to_all_players(context: ContextTypes.DEFAULT_TYPE, message: str, exclude_user_id: int = None):
    """Отправляет сообщение всем игрокам в базе данных."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT user_id FROM players')
        players = cur.fetchall()

        for (user_id,) in players:
            if user_id == exclude_user_id:
                continue
            try:
                await context.bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.MARKDOWN_V2)
                await asyncio.sleep(0.05) # Небольшая задержка, чтобы избежать лимитов Telegram
            except Exception as e:
                logger.warning(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при отправке сообщения всем игрокам: {e}")
    finally:
        if conn:
            conn.close()

# --- Core game logic functions ---

async def start_match(player1_id: int, player2_id: int, tournament_id: int, round_number: int, bot):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('SELECT secret_code, username FROM players WHERE user_id = ?', (player1_id,))
        player1_code_data = cur.fetchone()
        player1_code = player1_code_data[0] if player1_code_data else None
        player1_username_raw = player1_code_data[1] if player1_code_data else None
        player1_username = player1_username_raw if player1_username_raw else f"Игрок {player1_id}"

        cur.execute('SELECT secret_code, username FROM players WHERE user_id = ?', (player2_id,))
        player2_code_data = cur.fetchone()
        player2_code = player2_code_data[0] if player2_code_data else None
        player2_username_raw = player2_code_data[1] if player2_code_data else None
        player2_username = player2_username_raw if player2_code_data else f"Игрок {player2_id}"

        # --- ДОБАВЛЕНО: Проверка на наличие секретных кодов перед началом матча ---
        error_message_missing_code = escape_markdown("Не удалось начать матч: у вас или вашего оппонента не установлен секретный код. Пожалуйста, установите его с помощью /set_code.", version=2)
        if player1_code is None or player2_code is None:
            logger.error(f"Ошибка: Один из игроков ({player1_id} или {player2_id}) не имеет секретного кода при попытке начать матч {tournament_id}, раунд {round_number}. Код игрока 1: {player1_code}, Код игрока 2: {player2_code}")
            await bot.send_message(chat_id=player1_id, text=error_message_missing_code, parse_mode=ParseMode.MARKDOWN_V2)
            await bot.send_message(chat_id=player2_id, text=error_message_missing_code, parse_mode=ParseMode.MARKDOWN_V2)
            return # Прерываем функцию, если коды отсутствуют
        # --- КОНЕЦ ДОБАВЛЕННОГО БЛОКА ---

        initial_cracked_code = json.dumps(["-"] * CODE_LENGTH)
        cur.execute('''
            INSERT INTO matches (player1_id, player2_id, current_player_turn, secret_code_player1, secret_code_player2,
                                 player1_cracked_code, player2_cracked_code, tournament_id, round_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (player1_id, player2_id, player1_id, player1_code, player2_code,
              initial_cracked_code, initial_cracked_code, tournament_id, round_number))
        match_id = cur.lastrowid
        conn.commit()

        cur.execute('UPDATE players SET state = ?, is_ready = 0 WHERE user_id = ? OR user_id = ?', (IN_MATCH, player1_id, player2_id))
        conn.commit()

        # Принудительно сбрасываем last_activity для игрока, который ходит первым
        update_player_activity(player1_id, datetime.datetime.now()) # Сброс таймера для первого игрока

        await bot.send_message(chat_id=player1_id,
                               text=f"🔥 \\*Матч начался\\!\\* Вы играете против \\*{escape_markdown(player2_username, version=2)}\\* в раунде \\*{round_number}\\* турнира\\.\\n"
                                    f"Ваш код\\: \\*{escape_markdown(player1_code, version=2)}\\*\\.\\n" # Убедитесь, что код отображается
                                    f"Вы ходите первым\\. Используйте \\`цифра позиция\\` для вашего предположения \\(позиции от 1 до {CODE_LENGTH}\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=player2_id,
                               text=f"🔥 \\*Матч начался\\!\\* Вы играете против \\*{escape_markdown(player1_username, version=2)}\\* в раунде \\*{round_number}\\* турнира\\.\\n"
                                    f"Ваш код\\: \\*{escape_markdown(player2_code, version=2)}\\*\\.\\n" # Убедитесь, что код отображается
                                    f"Ожидайте хода \\*{escape_markdown(player1_username, version=2)}\\*\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в start_match: {e}")
        await bot.send_message(chat_id=player1_id, text="Произошла ошибка при начале матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=player2_id, text="Произошла ошибка при начале матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в start_match: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await bot.send_message(chat_id=player1_id, text="Произошла непредвиденная ошибка при начале матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=player2_id, text="Произошла непредвиденная ошибка при начале матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def end_match(match_id: int, winner_id: int, bot):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('SELECT player1_id, player2_id, secret_code_player1, secret_code_player2, tournament_id, round_number FROM matches WHERE match_id = ?', (match_id,))
        match_info = cur.fetchone()
        player1_id, player2_id, secret_code_player1, secret_code_player2, tournament_id, round_number = match_info

        loser_id = player2_id if winner_id == player1_id else player1_id

        cur.execute('UPDATE matches SET status = ?, winner_id = ?, loser_id = ? WHERE match_id = ?', ('completed', winner_id, loser_id, match_id))

        # Обновляем состояние игроков после матча
        cur.execute('UPDATE players SET state = ?, is_ready = 0 WHERE user_id = ?', (WAITING_FOR_TOURNAMENT, winner_id))
        cur.execute('UPDATE players SET state = ?, is_eliminated = 1, is_ready = 0 WHERE user_id = ?', (START, loser_id))
        cur.execute('UPDATE players SET secret_code = NULL WHERE user_id = ?', (winner_id,)) # Победитель должен установить новый код
        conn.commit()

        # Обновляем статус игрока в tournament_players
        cur.execute('UPDATE tournament_players SET current_round = current_round + 1 WHERE user_id = ? AND tournament_id = ?', (winner_id, tournament_id))
        cur.execute('UPDATE tournament_players SET is_eliminated = 1 WHERE user_id = ? AND tournament_id = ?', (loser_id, tournament_id))
        conn.commit()

        winner_username_raw = (await bot.get_chat(winner_id)).username or (await bot.get_chat(winner_id)).first_name
        winner_username = winner_username_raw if winner_username_raw else f"Игрок {winner_id}" # Handle None username

        loser_username_raw = (await bot.get_chat(loser_id)).username or (await bot.get_chat(loser_id)).first_name
        loser_username = loser_username_raw if loser_username_raw else f"Игрок {loser_id}" # Handle None username

        await bot.send_message(chat_id=winner_id, text=f"🎉 \\*Поздравляем\\! Вы победили в матче против {escape_markdown(loser_username, version=2)}\\*\\! Ваш код был\\: \\*{escape_markdown(secret_code_player1 if winner_id == player1_id else secret_code_player2, version=2)}\\*\\.\\n"
                                                        f"Вы переходите в следующий раунд турнира\\. Пожалуйста, придумайте и введите новый {CODE_LENGTH}\\-значный секретный код для следующего раунда\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=loser_id, text=f"😔 \\*Вы проиграли матч против {escape_markdown(winner_username, version=2)}\\*\\. Ваш код был\\: \\*{escape_markdown(secret_code_player1 if loser_id == player1_id else secret_code_player2, version=2)}\\*\\.\\n"
                                                       f"Вы выбыли из турнира\\. Спасибо за участие\\!\\", parse_mode=ParseMode.MARKDOWN_V2)

        logger.info(f"Матч {match_id} завершен. Победитель: {winner_id}, Проигравший: {loser_id}.")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в end_match: {e}")
        await bot.send_message(chat_id=winner_id, text="Произошла ошибка при завершении матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=loser_id, text="Произошла ошибка при завершении матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в end_match: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await bot.send_message(chat_id=winner_id, text="Произошла непредвиденная ошибка при завершении матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await bot.send_message(chat_id=loser_id, text="Произошла непредвиденная ошибка при завершении матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def end_tournament(tournament_id: int, bot, winner_id: int = None):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("UPDATE tournaments SET status = 'completed', end_time = CURRENT_TIMESTAMP WHERE tournament_id = ?", (tournament_id,))
        conn.commit()

        winner_username = "Не определен"
        second_place_username = "Не определен"
        third_place_username = "Не определен"

        if winner_id:
            cur.execute('UPDATE tournaments SET winner_id = ? WHERE tournament_id = ?', (winner_id, tournament_id))
            conn.commit()
            winner_username_raw = (await bot.get_chat(winner_id)).username or (await bot.get_chat(winner_id)).first_name
            winner_username = winner_username_raw if winner_username_raw else f"Игрок {winner_id}" # Handle None username

            cur.execute('''
                SELECT tp.user_id, p.username
                FROM tournament_players tp
                JOIN players p ON tp.user_id = p.user_id
                WHERE tp.tournament_id = ? AND tp.is_eliminated = 1 AND tp.user_id != ?
                ORDER BY tp.current_round DESC, p.losses DESC
                LIMIT 2
            ''', (tournament_id, winner_id))

            top_losers = cur.fetchall()

            if top_losers:
                if len(top_losers) >= 1:
                    second_place_id = top_losers[0][0]
                    second_place_username_raw = top_losers[0][1]
                    second_place_username = second_place_username_raw if second_place_username_raw else f"Игрок {second_place_id}" # Handle None username
                    cur.execute('UPDATE tournaments SET second_place_id = ? WHERE tournament_id = ?', (second_place_id, tournament_id))
                if len(top_losers) >= 2:
                    third_place_id = top_losers[1][0]
                    third_place_username_raw = top_losers[1][1]
                    third_place_username = third_place_username_raw if third_place_username_raw else f"Игрок {third_place_id}" # Handle None username
                    cur.execute('UPDATE tournaments SET third_place_id = ? WHERE tournament_id = ?', (third_place_id, tournament_id))
            conn.commit()

        cur.execute('UPDATE players SET tournament_id = NULL, current_round = 0, is_eliminated = 0, state = ?, secret_code = NULL WHERE tournament_id = ?', (SET_CODE, tournament_id))
        cur.execute('DELETE FROM tournament_players WHERE tournament_id = ?', (tournament_id,))
        conn.commit()


        cur.execute('SELECT DISTINCT player1_id FROM matches WHERE tournament_id = ? UNION SELECT DISTINCT player2_id FROM matches WHERE tournament_id = ?', (tournament_id, tournament_id))
        all_participated_player_ids = [row[0] for row in cur.fetchall()]

        final_message = f"🏆 \\*Турнир ID {tournament_id} завершен\\!\\* 🏆\\n\\n"
        final_message += f"🥇 \\*1 место\\*\\: {escape_markdown(winner_username, version=2)}\\n"
        if 'second_place_id' in locals() and second_place_id:
             final_message += f"🥈 \\*2 место\\*\\: {escape_markdown(second_place_username, version=2)}\\n"
        if 'third_place_id' in locals() and third_place_id:
            final_message += f"🥉 \\*3 место\\*\\: {escape_markdown(third_place_username, version=2)}\\n"

        final_message += "\\nСпасибо всем за участие\\! Ждем вас в следующем турнире\\! 🎉"

        for player_id in all_participated_player_ids:
            try:
                await bot.send_message(chat_id=player_id, text=final_message, parse_mode=ParseMode.MARKDOWN_V2)
                cur.execute('UPDATE players SET state = ?, tournament_id = NULL, current_round = 0, is_eliminated = 0, secret_code = NULL WHERE user_id = ?', (SET_CODE, player_id))
                conn.commit()
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение о завершении турнира игроку {player_id}: {e}")

        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Турнир ID \\*{tournament_id}\\* успешно завершен и оповещения отправлены\\. Результаты\\:\\n{final_message}", parse_mode=ParseMode.MARKDOWN_V2)


    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в end_tournament: {e}")
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла ошибка при завершении турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в end_tournament: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла непредвиденная ошибка при завершении турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def prepare_next_round(tournament_id: int, round_number: int, bot):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Fetch active players who are not eliminated AND have set their secret_code
        # Their current_round should be the round we are preparing for (round_number)
        cur.execute('''
            SELECT tp.user_id, p.username
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = ?
              AND tp.is_eliminated = 0
              AND tp.current_round = ?
              AND p.secret_code IS NOT NULL -- Ensure they have set their code for this round
              AND p.state = ? -- Ensure they are in the correct state (WAITING_FOR_TOURNAMENT after setting new code)
            ORDER BY RANDOM()
        ''', (tournament_id, round_number, WAITING_FOR_TOURNAMENT))
        eligible_players_for_pairing = cur.fetchall() # Renamed for clarity

        if len(eligible_players_for_pairing) == 0:
            logger.info(f"В турнире {tournament_id} в раунде {round_number} нет активных игроков с установленным кодом. Завершаем турнир (возможно, ошибка или все выбыли).")
            await check_tournament_status(tournament_id, bot) # Re-check status, might lead to tournament end
            return

        if len(eligible_players_for_pairing) == 1:
            winner_id = eligible_players_for_pairing[0][0]
            logger.info(f"В турнире {tournament_id} остался 1 победитель в раунде {round_number}: {winner_id}. Завершаем турнир.")
            await end_tournament(tournament_id, bot, winner_id=winner_id)
            return

        # Convert to list of IDs for shuffling and popping
        players_to_pair_ids = [p[0] for p in eligible_players_for_pairing]
        random.shuffle(players_to_pair_ids)

        bye_player_id = None
        if len(players_to_pair_ids) % 2 != 0:
            bye_player_id = players_to_pair_ids.pop(0) # Remove from the list of players to be paired
            
            # Fetch username for the BYE player
            cur.execute('SELECT username FROM players WHERE user_id = ?', (bye_player_id,))
            bye_username_raw = cur.fetchone()[0]
            bye_username = bye_username_raw if bye_username_raw else f"Пользователь {bye_player_id}"

            # Update bye player for next round
            cur.execute('UPDATE tournament_players SET is_bye = 1, current_round = current_round + 1 WHERE user_id = ? AND tournament_id = ?', (bye_player_id, tournament_id))
            cur.execute('UPDATE players SET secret_code = NULL, state = ? WHERE user_id = ?', (SET_NEW_ROUND_CODE, bye_player_id))
            conn.commit()

            # Принудительно сбрасываем last_activity для BYE игрока
            update_player_activity(bye_player_id, datetime.datetime.now())

            # --- ИЗМЕНЕНО: Более подробное оповещение для BYE игрока ---
            await bot.send_message(chat_id=bye_player_id,
                                   text=f"✨ \\*Поздравляем\\!\\* В этом раунде \\(\\*\\{round_number}\\*\\) турнира вы получили \\*свободный проход\\* \\(Bye\\) и автоматически переходите в следующий раунд\\! 🎉\\n\\n"
                                        f"Для продолжения участия в турнире, пожалуйста, придумайте и введите \\*новый {CODE_LENGTH}\\-значный секретный код\\* \\(все цифры должны быть уникальными и не начинаться с нуля\\)\\.\\n"
                                        f"Вы можете это сделать, просто отправив код \\(например, \\`12345\\`\\) или используя команду \\`/set_code 12345\\`\\.\\n"
                                        f"После установки кода вы автоматически будете ожидать следующего матча\\.",
                                   parse_mode=ParseMode.MARKDOWN_V2)
            # -----------------------------------------------------------

            await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Игрок {escape_markdown(bye_username, version=2)} \\({bye_player_id}\\) получил свободный проход \\(Bye\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Игрок {bye_player_id} получил BYE в турнире {tournament_id} раунде {round_number}.")
            logger.info(f"BYE игрок {bye_player_id} обновлен: state={SET_NEW_ROUND_CODE}, secret_code=NULL, current_round={round_number + 1}")

        matches_created = 0
        for i in range(0, len(players_to_pair_ids), 2):
            if i + 1 < len(players_to_pair_ids):
                player1_id = players_to_pair_ids[i]
                player2_id = players_to_pair_ids[i+1]
                await start_match(player1_id, player2_id, tournament_id, round_number, bot)
                matches_created += 1

        if matches_created == 0 and bye_player_id is None:
            logger.warning(f"В турнире {tournament_id} в раунде {round_number} не удалось создать матчи. Количество игроков: {len(eligible_players_for_pairing)}. Проверяем статус турнира.")
            await check_tournament_status(tournament_id, bot)
            return

        if matches_created > 0:
            await bot.send_message(chat_id=ADMIN_USER_ID, text=f"В турнире \\*{tournament_id}\\*, раунд \\*{round_number}\\*:\\\\ Создано \\*{matches_created}\\* матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в prepare_next_round: {e}")
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла ошибка при подготовке раунда {round_number} турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в prepare_next_round: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла непредвиденная ошибка при подготовке раунда {round_number} турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def check_tournament_status(tournament_id: int, bot):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT current_round, status FROM tournaments WHERE tournament_id = ?", (tournament_id,))
        tournament_data = cur.fetchone()
        if not tournament_data:
            logger.warning(f"Турнир {tournament_id} не найден в check_tournament_status.")
            return

        current_tournament_round, tournament_status = tournament_data

        if tournament_status != 'active':
            return # Only process active tournaments

        # 1. Check if all matches for the current_tournament_round are completed or cancelled
        cur.execute('''
            SELECT COUNT(*) FROM matches
            WHERE tournament_id = ? AND round_number = ? AND status = 'active'
        ''', (tournament_id, current_tournament_round))
        active_matches_in_current_round = cur.fetchone()[0]

        if active_matches_in_current_round == 0:
            # All matches for the current round are done. Now, prepare for the next round.

            # Identify players who are eligible for the NEXT round (winners from current round, or BYE players who advanced)
            # These players will have current_round = current_tournament_round + 1 in tournament_players table
            cur.execute('''
                SELECT tp.user_id, p.username, p.secret_code, p.state
                FROM tournament_players tp
                JOIN players p ON tp.user_id = p.user_id
                WHERE tp.tournament_id = ? AND tp.is_eliminated = 0 AND tp.current_round = ?
            ''', (tournament_id, current_tournament_round + 1)) # Look for players already advanced to next round
            eligible_players_for_next_round = cur.fetchall()

            if not eligible_players_for_next_round:
                # No players left for the next round (everyone eliminated or no one advanced)
                logger.info(f"В турнире {tournament_id} нет активных игроков для раунда {current_tournament_round + 1}. Завершаем турнир.")
                await end_tournament(tournament_id, bot)
                return

            if len(eligible_players_for_next_round) == 1:
                # Only one player left, they are the tournament winner
                winner_id = eligible_players_for_next_round[0][0]
                logger.info(f"Турнир {tournament_id} завершен! Победитель: {winner_id}.")
                await end_tournament(tournament_id, bot, winner_id=winner_id)
                return

            # If more than one player, check if all of them have set their new codes
            all_players_ready_for_next_round = True
            for player_id, username_raw, secret_code, state in eligible_players_for_next_round:
                if secret_code is None or state != WAITING_FOR_TOURNAMENT:
                    all_players_ready_for_next_round = False
                    # Send a reminder if they are in SET_NEW_ROUND_CODE and haven't set code
                    if state == SET_NEW_ROUND_CODE:
                        try:
                            await bot.send_message(chat_id=player_id,
                                                   text=f"Напоминание: Вы перешли в следующий раунд турнира\\. Пожалуйста, придумайте и введите новый {CODE_LENGTH}\\-значный секретный код\\.",
                                                   parse_mode=ParseMode.MARKDOWN_V2)
                        except Exception as e:
                            logger.warning(f"Не удалось отправить напоминание игроку {player_id} о новом коде: {e}")
                    break # No need to check others, if one is not ready

            if all_players_ready_for_next_round:
                # All players are ready with new codes, proceed to prepare matches for the next round
                next_round_number = current_tournament_round + 1
                cur.execute("UPDATE tournaments SET current_round = ? WHERE tournament_id = ?", (next_round_number, tournament_id))
                conn.commit()
                await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Все матчи раунда \\*{current_tournament_round}\\* турнира \\*{tournament_id}\\* завершены\\. Начинается раунд \\*{next_round_number}\\*\\.", parse_mode=ParseMode.MARKDOWN_V2)
                logger.info(f"Все матчи раунда {current_tournament_round} турнира {tournament_id} завершены. Начинается раунд {next_round_number}.")
                await prepare_next_round(tournament_id, next_round_number, bot)
            else:
                logger.info(f"Турнир {tournament_id}, раунд {current_tournament_round + 1}: Ожидаем, пока игроки установят новые коды.")
                # Do nothing, the job will run again and re-evaluate when players set codes
        else:
            logger.info(f"Турнир {tournament_id}, раунд {current_tournament_round}: Еще есть активные матчи ({active_matches_in_current_round}).")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в check_tournament_status: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в check_tournament_status: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

async def _begin_tournament_logic(tournament_id: int, bot):
    """
    Внутренняя логика запуска турнира, вызывается после проверки готовности.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("UPDATE tournaments SET status = 'active', current_round = 1, start_time = CURRENT_TIMESTAMP WHERE tournament_id = ?", (tournament_id,))
        conn.commit()

        cur.execute('SELECT user_id FROM tournament_players WHERE tournament_id = ?', (tournament_id,))
        registered_players_ids = [row[0] for row in cur.fetchall()]

        for player_id in registered_players_ids:
            try:
                await bot.send_message(chat_id=player_id, text=f"🚀 \\*Турнир ID {tournament_id} начинается\\!\\* Первый раунд стартует сейчас\\.", parse_mode=ParseMode.MARKDOWN_V2)
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.warning(f"Не удалось отправить сообщение о начале турнира игроку {player_id}: {e}")

        logger.info(f"Турнир {tournament_id} начат администратором.")

        await prepare_next_round(tournament_id, 1, bot)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в _begin_tournament_logic: {e}")
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла ошибка при автоматическом запуске турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в _begin_tournament_logic: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await bot.send_message(chat_id=ADMIN_USER_ID, text=f"Произошла непредвиденная ошибка при автоматическом запуске турнира {tournament_id}\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state FROM players WHERE user_id = ?', (user_id,))
        player_state_data = cur.fetchone()
        player_state = player_state_data[0] if player_state_data else None

        if player_state != IN_MATCH:
            # Отправляем сообщение только если это не вызов из WebApp
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы не в активном матче\\. Используйте /ready для регистрации на турнир или /start для получения информации\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        cur.execute('SELECT * FROM matches WHERE (player1_id = ? OR player2_id = ?) AND status = \'active\'', (user_id, user_id))
        match_data = cur.fetchone()

        if not match_data:
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Активный матч не найден\\. Возможно, ваш предыдущий матч завершился или был отменен\\. Используйте /start для проверки статуса\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        match_id, player1_id, player2_id, current_player_turn, secret_code_player1, secret_code_player2, player1_cracked_code_json, player2_cracked_code_json, status, start_time, tournament_id, round_number, _, _ = match_data

        if user_id != current_player_turn:
            if not (update.message and update.message.web_app_data):
                cur.execute('SELECT username FROM players WHERE user_id = ?', (current_player_turn,))
                current_player_username_raw = cur.fetchone()
                current_player_username = current_player_username_raw[0] if current_player_username_raw and current_player_username_raw[0] else "Неизвестный игрок" # Handle None username
                await update.message.reply_text(f"Сейчас ход оппонента \\(\\*\\{escape_markdown(current_player_username, version=2)}\\*\\)\\. Пожалуйста, подождите\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Проверка аргументов для guess, учитывая вызов из WebApp
        if len(context.args) != 2 or not context.args[0].isdigit() or not context.args[1].isdigit():
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text(f"Пожалуйста, введите ваше предположение в формате\\: \\`цифра позиция\\`\\, например\\: \\`7 3\\` \\(позиции от 1 до {CODE_LENGTH}\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        if len(context.args[0]) != 1:
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Пожалуйста, введите \\*одну цифру\\* для угадывания\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        guessed_digit = context.args[0]
        guessed_position_str = context.args[1]

        if not (1 <= int(guessed_position_str) <= CODE_LENGTH):
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text(f"Позиция должна быть числом от 1 до {CODE_LENGTH}\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        guessed_position_index = int(guessed_position_str) - 1

        opponent_id = player2_id if user_id == player1_id else player1_id
        opponent_secret_code = secret_code_player2 if user_id == player1_id else secret_code_player1

        # --- ДОБАВЛЕНО: Проверка на None для opponent_secret_code ---
        if opponent_secret_code is None:
            logger.error(f"Ошибка в guess: Секретный код оппонента ({opponent_id}) равен None в матче {match_id}.")
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Произошла ошибка\\: секретный код вашего оппонента не найден\\. Пожалуйста, свяжитесь с администратором\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return
        # --- КОНЕЦ ДОБАВЛЕННОГО БЛОКА ---

        cracked_code_list = json.loads(player1_cracked_code_json) if user_id == player1_id else json.loads(player2_cracked_code_json)

        response_message = ""

        if opponent_secret_code[guessed_position_index] == guessed_digit:
            if cracked_code_list[guessed_position_index] == '-':
                response_message = f"✅ \\*Верно\\!\\* Цифра \\*{escape_markdown(guessed_digit, version=2)}\\* на позиции \\*{escape_markdown(guessed_position_str, version=2)}\\* правильная\\."
                cracked_code_list[guessed_position_index] = guessed_digit
            else:
                response_message = f"✅ \\*Верно\\!\\* Цифра \\*{escape_markdown(guessed_digit, version=2)}\\* на позиции \\*{escape_markdown(guessed_position_str, version=2)}\\* уже была угадана ранее\\."
        elif guessed_digit in opponent_secret_code:
            response_message = f"↔️ \\*Не на месте\\*\\: Цифра \\*{escape_markdown(guessed_digit, version=2)}\\* есть в коде, но на \\*другой позиции\\*\\."
        else:
            response_message = f"❌ \\*Нет в коде\\*\\: Цифры \\*{escape_markdown(guessed_digit, version=2)}\\* нет в коде соперника\\."

        if user_id == player1_id:
            cur.execute('UPDATE matches SET player1_cracked_code = ? WHERE match_id = ?', (json.dumps(cracked_code_list), match_id))
        else:
            cur.execute('UPDATE matches SET player2_cracked_code = ? WHERE match_id = ?', (json.dumps(cracked_code_list), match_id))
        conn.commit()

        current_progress_str = " ".join(cracked_code_list)
        
        # Отправляем сообщение в чат бота, если это не из WebApp
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text(f"{response_message}\\nВаш прогресс\\: \\`{escape_markdown(current_progress_str, version=2)}\\`", parse_mode=ParseMode.MARKDOWN_V2)

        if all(digit != "-" for digit in cracked_code_list):
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Поздравляю\\! Вы успешно взломали код соперника\\! 🎉", parse_mode=ParseMode.MARKDOWN_V2)
            await end_match(match_id, user_id, context.bot)
            await check_tournament_status(tournament_id, context.bot)
        else:
            next_player_turn = opponent_id
            cur.execute('UPDATE matches SET current_player_turn = ? WHERE match_id = ?', (next_player_turn, match_id))
            update_player_activity(next_player_turn, datetime.datetime.now())
            conn.commit()

            cur.execute('SELECT username FROM players WHERE user_id = ?', (next_player_turn,))
            next_player_username_raw = cur.fetchone()
            next_player_username = next_player_username_raw[0] if next_player_username_raw and next_player_username_raw[0] else "Неизвестный игрок"

            # Отправляем сообщения в чат бота, если это не из WebApp
            if not (update.message and update.message.web_app_data):
                await context.bot.send_message(chat_id=user_id, text=f"Ход передан \\*{escape_markdown(next_player_username, version=2)}\\*\\.", parse_mode=ParseMode.MARKDOWN_V2)

                opponent_cracked_code_list = json.loads(player2_cracked_code_json) if user_id == player1_id else json.loads(player1_cracked_code_json)
                opponent_current_progress_str = " ".join(opponent_cracked_code_list)

                await context.bot.send_message(chat_id=next_player_turn,
                                               text=f"Ваш ход\\! Результат хода оппонента\\: {response_message}\\n"
                                                    f"Ваш текущий прогресс\\: \\`{escape_markdown(opponent_current_progress_str, version=2)}\\`\\n"
                                                    f"Используйте \\`цифра позиция\\` для вашего предположения\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в guess: {e}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла ошибка при обработке вашего предположения\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в guess: {e}\n{traceback.format_exc()}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла непредвиденная ошибка при обработке вашего предположения\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- Admin specific functions ---

async def admin_create_tournament_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    input_text = update.message.text.strip()

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        parts = input_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[0].isdigit():
            await update.message.reply_text("Неверный формат\\. Пожалуйста, введите точное количество игроков \\(число\\) и название турнира \\(например, \\`8 Весенний турнир\\`\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        min_players = int(parts[0])
        tournament_name = parts[1].strip()

        if min_players < 2:
            await update.message.reply_text("Минимальное количество игроков должно быть не меньше 2\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Проверяем, есть ли уже активный турнир (на всякий случай, хотя уже проверяли в start_new_tournament)
        cur.execute("SELECT tournament_id, status FROM tournaments WHERE status IN ('registration', 'active')")
        active_tournament = cur.fetchone()
        if active_tournament:
            tour_id, tour_status = active_tournament
            msg = f"Уже есть активный турнир \\(\\*ID\\: {tour_id}\\*, статус\\: \\*{escape_markdown(tour_status, version=2)}\\*\\)\\. Завершите его, прежде чем начинать новый\\."
            # Если это callback_query и сообщение совпадает с effective_message, редактируем его
            if update.callback_query and update.callback_query.message == update.effective_message:
                await update.callback_query.message.edit_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
            else: # Иначе просто отвечаем
                await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
            return

        cur.execute("INSERT INTO tournaments (tournament_name, status, current_round, admin_id, min_players) VALUES (?, ?, ?, ?, ?)",
                    (tournament_name, 'registration', 0, user_id, min_players))
        tournament_id = cur.lastrowid
        conn.commit()

        await update.message.reply_text(
            f"✅ Турнир '{escape_markdown(tournament_name, version=2)}' \\(\\*ID\\: \\*{tournament_id}\\*\\) успешно создан\\! Открыта регистрация\\. "
            f"Минимальное количество игроков для старта\\: \\*{min_players}\\*\\.\\n"
            f"Игроки могут присоединиться, используя команду /ready\\.", parse_mode=ParseMode.MARKDOWN_V2)

        # Сбрасываем состояние админа
        cur.execute('UPDATE players SET state = ? WHERE user_id = ?', (START, user_id))
        conn.commit()

        logger.info(f"Администратор {user_id} создал новый турнир ID {tournament_id} с названием '{tournament_name}' и мин. игроками {min_players}.")

        registration_message = (
            f"🎉 Объявление\\! 🎉\\n\\n"
            f"Администратор запустил новый турнир '{escape_markdown(tournament_name, version=2)}' Mastermind\\! "
            f"Пришло время показать свои навыки\\.\\n\\n"
            f"Чтобы зарегистрироваться, установите свой {CODE_LENGTH}\\-значный код и используйте команду\\:\\n"
            f"/ready\\n\\n"
            f"Минимальное количество игроков для старта\\: \\*{min_players}\\*\\.\\n"
            f"Успейте присоединиться\\!"
        )
        await send_message_to_all_players(context, registration_message, exclude_user_id=ADMIN_USER_ID)
        logger.info(f"Отправлено оповещение о новом турнире {tournament_id} всем игрокам.")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_create_tournament_details: {e}")
        await update.message.reply_text("Произошла ошибка при создании турнира\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в admin_create_tournament_details: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await update.message.reply_text("Произошла непредвиденная ошибка при создании турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def admin_send_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    broadcast_message = update.message.text

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('UPDATE players SET state = ? WHERE user_id = ?', (START, user_id))
        conn.commit()

        await update.message.reply_text("Начинаю рассылку\\. Это может занять некоторое время\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)

        cur.execute('SELECT user_id FROM players WHERE user_id != ?', (ADMIN_USER_ID,))
        player_ids = [row[0] for row in cur.fetchall()]

        sent_count = 0
        failed_count = 0

        escaped_broadcast_message = escape_markdown(broadcast_message, version=2)

        for chat_id in player_ids:
            try:
                await context.bot.send_message(chat_id=chat_id, text=escaped_broadcast_message, parse_mode=ParseMode.MARKDOWN_V2)
                sent_count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")
                failed_count += 1

        await context.bot.send_message(chat_id=user_id, text=f"Рассылка завершена\\!\\n"
                                                              f"Отправлено сообщений\\: {sent_count}\\n"
                                                              f"Не удалось отправить\\: {failed_count}", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_send_broadcast: {e}")
        await update.message.reply_text("Произошла ошибка при получении списка игроков для рассылки\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в admin_send_broadcast: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await update.message.reply_text("Произошла непредвиденная ошибка при рассылке\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def admin_broadcast_prompt(update_obj: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update_obj.effective_user.id
    if not is_admin(user_id):
        await update_obj.effective_message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('UPDATE players SET state = ? WHERE user_id = ?', (AWAITING_BROADCAST_MESSAGE, user_id))
        conn.commit()
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text("Пожалуйста, введите сообщение для рассылки всем пользователям\\.", parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text("Пожалуйста, введите сообщение для рассылки всем пользователям\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_broadcast_prompt: {e}")
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text("Произошла ошибка при подготовке к рассылке\\.", parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text("Произошла ошибка при подготовке к рассылке\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()


async def admin_end_tournament_prompt(update_obj: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['awaiting_tournament_id_to_end'] = True
    if update_obj.callback_query:
        await update_obj.callback_query.message.edit_text("Пожалуйста, введите ID турнира, который вы хотите завершить, или \\`текущий\\` для последнего активного турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    elif update_obj.message:
        await update_obj.message.reply_text("Пожалуйста, введите ID турнира, который вы хотите завершить, или \\`текущий\\` для последнего активного турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def admin_end_tournament_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not context.user_data.get('awaiting_tournament_id_to_end'):
        await update.message.reply_text("Неожиданный ввод\\. Используйте кнопку 'Завершить текущий турнир' в админ\\-панели\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    input_id = update.message.text.strip().lower()
    tournament_id_to_end = None

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        if input_id == 'текущий':
            cur.execute("SELECT tournament_id FROM tournaments WHERE status IN ('registration', 'active') ORDER BY tournament_id DESC LIMIT 1")
            tour_data = cur.fetchone()
            if tour_data:
                tournament_id_to_end = tour_data[0]
            else:
                await update.message.reply_text("Нет активных или регистрирующихся турниров для завершения\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
        elif input_id.isdigit():
            tournament_id_to_end = int(input_id)
            cur.execute("SELECT tournament_id, status FROM tournaments WHERE tournament_id = ?", (tournament_id_to_end,))
            tour_data = cur.fetchone()
            if not tour_data:
                await update.message.reply_text(f"Турнир с ID \\*{tournament_id_to_end}\\* не найден\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
            if tour_data[1] == 'completed':
                await update.message.reply_text(f"Турнир с ID \\*{tournament_id_to_end}\\* уже завершен\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
        else:
            await update.message.reply_text("Пожалуйста, введите корректный ID турнира или слово \\`текущий\\`\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        if tournament_id_to_end:
            cur.execute('UPDATE matches SET status = \'cancelled\' WHERE tournament_id = ? AND status = \'active\'', (tournament_id_to_end,))
            conn.commit()

            await end_tournament(tournament_id_to_end, context.bot)
            await update.message.reply_text(f"Турнир \\*{tournament_id_to_end}\\* был принудительно завершен\\.", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await update.message.reply_text("Не удалось определить турнир для завершения\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_end_tournament_by_id: {e}")
        await update.message.reply_text("Произошла ошибка при завершении турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

    context.user_data['awaiting_tournament_id_to_end'] = False


async def admin_cancelmatch_prompt(update_obj: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['awaiting_match_id'] = True
    if update_obj.callback_query:
        await update_obj.callback_query.message.edit_text("Пожалуйста, введите ID матча, который вы хотите отменить\\.", parse_mode=ParseMode.MARKDOWN_V2)
    elif update_obj.message:
        await update_obj.message.reply_text("Пожалуйста, введите ID матча, который вы хотите отменить\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def admin_cancelmatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not context.user_data.get('awaiting_match_id'):
        await update.message.reply_text("Неожиданный ввод\\. Используйте кнопку 'Отменить матч по ID' в админ\\-панели\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    match_id_str = update.message.text
    if not match_id_str.isdigit():
        await update.message.reply_text("ID матча должен быть числом\\. Пожалуйста, введите корректный ID\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    match_id = int(match_id_str)

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT player1_id, player2_id, status, tournament_id, round_number FROM matches WHERE match_id = ?', (match_id,))
        match_data = cur.fetchone()

        if not match_data:
            await update.message.reply_text(f"Матч с ID {match_id} не найден\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        player1_id, player2_id, status, tournament_id, round_number = match_data

        if status != 'active':
            await update.message.reply_text(f"Матч с ID {match_id} уже не активен \\(\\*статус\\: \\*{escape_markdown(status, version=2)}\\*\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        cur.execute('UPDATE matches SET status = \'cancelled\' WHERE match_id = ?', (match_id,))
        initial_cracked_code = json.dumps(["-"] * CODE_LENGTH)
        cur.execute('UPDATE matches SET player1_cracked_code = ?, player2_cracked_code = ? WHERE match_id = ?',
                    (initial_cracked_code, initial_cracked_code, match_id))

        cur.execute('UPDATE players SET state = ?, is_ready = 0 WHERE user_id = ? OR user_id = ?', (WAITING_FOR_TOURNAMENT, player1_id, player2_id))
        conn.commit()

        await update.message.reply_text(f"Матч с ID \\*{match_id}\\* успешно отменен\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await context.bot.send_message(chat_id=player1_id, text="Ваш матч был отменен администратором\\. Пожалуйста, дождитесь новых матчей в турнире\\.", parse_mode=ParseMode.MARKDOWN_V2)
        await context.bot.send_message(chat_id=player2_id, text="Ваш матч был отменен администратором\\. Пожалуйста, дождитесь новых матчей в турнире\\.", parse_mode=ParseMode.MARKDOWN_V2)

        await check_tournament_status(tournament_id, context.bot)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_cancelmatch: {e}")
        await update.message.reply_text("Произошла ошибка при отмене матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

    context.user_data['awaiting_match_id'] = False

async def admin_stats(update_obj: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update_obj.effective_user.id
    if user_id != ADMIN_USER_ID:
        await update_obj.effective_message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('SELECT COUNT(*) FROM players')
        total_players = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM players WHERE state = ?', (WAITING_FOR_TOURNAMENT,))
        waiting_players = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM players WHERE state = ?', (IN_MATCH,))
        in_match_players = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM matches')
        total_matches = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM matches WHERE status = ?', ('active',))
        active_matches = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM matches WHERE status = ?', ('completed',))
        completed_matches = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM tournaments WHERE status = ?', ('registration',))
        registration_tournaments = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM tournaments WHERE status = ?', ('active',))
        active_tournaments = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) FROM tournaments WHERE status = ?', ('completed',))
        completed_tournaments = cur.fetchone()[0]

        stats_message = (
            "\\*📊 Общая Статистика Бота 'Crack the Code'\\:*\\n"
            f"👥 \\*Игроки\\*\\:\\n"
            f"  \\\\- Всего зарегистрировано\\: {total_players}\\n"
            f"  \\\\- Ожидают турнира/кода\\: {waiting_players}\\n"
            f"  \\\\- В активном матче\\: {in_match_players}\\n"
            f"⚔️ \\*Матчи\\*\\:\\n"
            f"  \\\\- Всего проведено\\: {total_matches}\\n"
            f"  \\\\- Активных\\: {active_matches}\\n"
            f"  \\\\- Завершенных\\: {completed_matches}\\n"
            f"🏆 \\*Турниры\\*\\:\\n"
            f"  \\\\- В стадии регистрации\\: {registration_tournaments}\\n"
            f"  \\\\- Активных\\: {active_tournaments}\\n"
            f"  \\\\- Завершенных\\: {completed_tournaments}"
        )

        keyboard = [
            [InlineKeyboardButton("👤 Все зарегистрированные пользователи", callback_data=SHOW_ALL_USERS_CALLBACK)],
            [InlineKeyboardButton("⚔️ Все ID матчей", callback_data=SHOW_ALL_MATCH_IDS_CALLBACK)],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text(stats_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text(stats_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_stats: {e}")
        error_message = "Произошла ошибка при получении статистики\\."
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в admin_stats: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        error_message = "Произошла непредвиденная ошибка при получении статистики\\."
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def show_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отправляет админу список всех зарегистрированных пользователей.
    Получает user_id и username из таблицы 'players'.
    """
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    if user_id != ADMIN_USER_ID:
        await query.edit_message_text("У вас нет прав для выполнения этой команды.")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT user_id, username FROM players")
        users_data = cursor.fetchall()

        if not users_data:
            await query.edit_message_text("В базе данных нет зарегистрированных пользователей\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        user_list_messages = ["👤 \\*Список зарегистрированных пользователей\\*\\:\\n"]
        current_message_content = user_list_messages[0]

        for p_id, p_username_raw in users_data:
            p_username = p_username_raw if p_username_raw else f"ID: {p_id}" # Handle None username
            user_display = f"@{escape_markdown(p_username_raw, version=2)}" if p_username_raw else f"ID\\: \\`{p_id}\\`" # Use raw for @ display
            line = f"{user_display}\\n"

            if len(current_message_content) + len(line) > 4000:
                user_list_messages.append(line)
                current_message_content = line
            else:
                current_message_content += line
            user_list_messages[-1] = current_message_content

        for msg in user_list_messages:
            await context.bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в show_all_users: {e}")
        await query.edit_message_text("Произошла ошибка при получении списка пользователей\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в show_all_users: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await query.edit_message_text("Произошла непредвиденная ошибка при получении списка пользователей\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def show_all_match_ids(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отправляет админу список всех ID матчей.
    Получает match_id из таблицы 'matches'.
    """
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    if user_id != ADMIN_USER_ID:
        await query.edit_message_text("У вас нет прав для выполнения этой команды.")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT match_id FROM matches")
        match_ids_data = cursor.fetchall()

        if not match_ids_data:
            await query.edit_message_text("В базе данных нет зарегистрированных матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        match_id_messages = ["⚔️ \\*Список ID матчей\\*\\:\\n"]
        current_message_content = match_id_messages[0]

        for match_id_tuple in match_ids_data:
            match_id = match_id_tuple[0]
            line = f"\\`{match_id}\\`\\n"

            if len(current_message_content) + len(line) > 4000:
                match_id_messages.append(line)
                current_message_content = line
            else:
                current_message_content += line
            match_id_messages[-1] = current_message_content # Исправлено на match_id_messages

        for msg in match_id_messages:
            await context.bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в show_all_match_ids: {e}")
        await query.edit_message_text("Произошла ошибка при получении списка ID матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в show_all_match_ids: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await query.edit_message_text("Произошла непредвиденная ошибка при получении списка ID матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def admin_reset_game(update_obj: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('DELETE FROM matches')
        cur.execute('DELETE FROM tournament_players')
        cur.execute('DELETE FROM tournaments')

        cur.execute('UPDATE players SET secret_code = NULL, state = ?, is_ready = 0, wins = 0, losses = 0, tournament_id = NULL, current_round = 0, is_eliminated = 0 WHERE user_id != ?', (START, ADMIN_USER_ID))

        cur.execute('UPDATE players SET state = ?, wins = 0, losses = 0, tournament_id = NULL, current_round = 0, is_eliminated = 0 WHERE user_id = ?', (START, ADMIN_USER_ID))

        conn.commit()
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text("Игра полностью сброшена\\. Все турниры, матчи и прогресс игроков \\(кроме админа\\) удалены\\.", parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text("Игра полностью сброшена\\. Все турниры, матчи и прогресс игроков \\(кроме админа\\) удалены\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_reset_game: {e}")
        if update_obj.callback_query:
            await update_obj.callback_query.message.edit_text("Произошла ошибка при сбросе игры\\.", parse_mode=ParseMode.MARKDOWN_V2)
        elif update_obj.message:
            await update_obj.message.reply_text("Произошла ошибка при сбросе игры\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- User-facing commands ---
@update_activity_decorator
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username_raw = update.effective_user.username or update.effective_user.first_name
    username = username_raw if username_raw else f"Игрок {user_id}" # Handle None username

    # Добавлена функция update_user_info
    update_user_info(user_id, update.effective_user.username, update.effective_user.first_name, update.effective_user.last_name)

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state, tournament_id FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()

        if player_data:
            player_state, tournament_id = player_data
            escaped_player_state = escape_markdown(player_state, version=2)

            msg = f"Привет, {escape_markdown(username, version=2)}\\! Вы уже зарегистрированы\\. Ваше текущее состояние\\: \\*{escaped_player_state}\\*\\.\\n"

            if tournament_id:
                cur.execute('SELECT status, current_round, tournament_name FROM tournaments WHERE tournament_id = ?', (tournament_id,))
                tournament_status_data = cur.fetchone()
                if tournament_status_data:
                    tournament_status, tournament_round, tournament_name = tournament_status_data
                    if tournament_status == 'registration':
                        # Добавлено отображение количества зарегистрированных игроков
                        cur.execute("SELECT COUNT(user_id) FROM tournament_players WHERE tournament_id = ?", (tournament_id,))
                        registered_count = cur.fetchone()[0]
                        cur.execute("SELECT min_players FROM tournaments WHERE tournament_id = ?", (tournament_id,))
                        min_players = cur.fetchone()[0]
                        msg += f"Вы зарегистрированы на турнир '{escape_markdown(tournament_name, version=2)}' и ждете его начала\\. Зарегистрировано\\: \\*{registered_count}/{min_players}\\* игроков\\. Используйте /ready чтобы подтвердить участие\\.\\n"
                    elif tournament_status == 'active':
                        msg += f"Турнир '{escape_markdown(tournament_name, version=2)}' активен, текущий раунд\\: \\*{tournament_round}\\*\\.\\n"
                        cur.execute('SELECT match_id FROM matches WHERE (player1_id = ? OR player2_id = ?) AND status = \'active\'', (user_id, user_id))
                        active_match = cur.fetchone()
                        if active_match:
                            msg += "У вас есть активный матч\\. Продолжайте игру\\!\\n"
                        else:
                            cur.execute('SELECT is_eliminated, is_bye, current_round FROM tournament_players WHERE user_id = ? AND tournament_id = ?', (user_id, tournament_id))
                            tp_data = cur.fetchone()
                            if tp_data:
                                is_eliminated, is_bye, player_current_round = tp_data
                                if is_eliminated:
                                    msg += "Вы выбыли из турнира\\. 😔\\n"
                                elif is_bye:
                                    # Уточнение для BYE игрока
                                    if player_current_round > tournament_round: # Если BYE игрок уже в следующем раунде
                                        msg += f"Вы получили свободный проход в раунде \\*{tournament_round}\\* и перешли в раунд \\*{player_current_round}\\*\\. Пожалуйста, придумайте и введите новый {CODE_LENGTH}\\-значный секретный код для следующего раунда\\."
                                    else: # Если BYE игрок еще не перешел в следующий раунд (т.е. только что получил BYE в текущем раунде)
                                        msg += f"Вы получили свободный проход в этом раунде \\(\\*\\{tournament_round}\\*\\)\\. Ожидайте следующего раунда\\. Для продолжения, пожалуйста, придумайте и введите новый {CODE_LENGTH}\\-значный секретный код\\."
                                else:
                                    msg += "Ожидаете своего оппонента в текущем раунде\\. Мы ищем вам пару\\.\\."
                            else:
                                msg += "Что-то пошло не так с вашим участием в турнире\\. Пожалуйста, свяжитесь с администратором\\."
                    elif tournament_status == 'completed':
                        msg += f"Турнир '{escape_markdown(tournament_name, version=2)}' завершен\\. Используйте /tournament_info для получения результатов\\."
                else:
                    msg += "Информация о турнире не найдена\\. Возможно, он был удален или завершен\\."
            else:
                msg += f"Для участия в турнире 'Crack the Code' введите свой {CODE_LENGTH}\\-значный секретный код командой /set_code 12345 или просто отправьте его\\. "
                msg += f"Затем используйте команду /ready\\."

            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
        else:
            cur.execute('INSERT INTO players (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)',
                        (user_id, username_raw, update.effective_user.first_name, update.effective_user.last_name)) # Store raw username
            conn.commit()
            
            # Обновленный текст сообщения для команды /start
            welcome_message = (
                f"Привет, @{escape_markdown(username_raw, version=2)}\\! Добро пожаловать в игру 'Crack the Code'\\! 🎉\\n\\n"
                f"Суть игры\\: Каждый игрок загадывает \\*уникальный {CODE_LENGTH}\\-значный код\\* \\(все цифры разные, не начинается с нуля\\)\\.\\n"
                f"Ваша задача \\- \\*отгадать код соперника\\*\\, делая предположения по одной цифре и её позиции\\.\\n"
                f"Для участия в турнире введите свой {CODE_LENGTH}\\-значный секретный код или просто отправьте его\\.\\n\\n"
                f"Правила игры\\: /rules"
            )
            await update.message.reply_text(welcome_message, parse_mode=ParseMode.MARKDOWN_V2)
            cur.execute('UPDATE players SET state = ? WHERE user_id = ?', (SET_CODE, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в start: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в start: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Define the raw rules text with placeholders
    # Все специальные символы в статическом тексте должны быть экранированы вручную
    rules_text_content = (
        f"\\*💥 Правила игры 'Crack the Code'\\! 💥\\*\\n\\n"
        f"Добро пожаловать в захватывающий мир 'Crack the Code'\\! Это игра на логику и дедукцию, где ваша цель — взломать секретный код оппонента, используя подсказки бота\\.\\n\\n"
        f"1\\. \\*Ваш секретный код\\*\\: Каждый игрок загадывает \\*уникальный {CODE_LENGTH}\\-значный код\\* \\(все цифры разные, не начинается с нуля\\)\\.\\n"
        f"1\\.1\\. Игроку на ход даёётся 15 минут, если за это время вы не успели сделать ход, то он передаётся вашему оппоненту\\.\\n"
        f"Ваша задача \\- \\*отгадать код соперника\\*\\, делая предположения по одной цифре и её позиции\\.\\n"
        f"Для участия в турнире введите свой {CODE_LENGTH}\\-значный секретный код или просто отправьте его\\.\\n\\n"
        f"2\\. \\*Как угадывать\\*\\: В свой ход вы делаете предположение о коде соперника\\. Используйте формат\\: \\`цифра позиция\\`\\. Например, чтобы угадать цифру 7 на 3\\-й позиции, отправьте \\`7 3\\`\\. Позиции нумеруются от 1 до {CODE_LENGTH}\\.\\n"
        f"3\\. \\*Подсказки бота\\*\\: После каждого вашего предположения бот даст одну из следующих подсказок\\:\\n"
        f"   \\\\- ✅ \\*Верно\\!\\*\\: Цифра \\*угаданная\\_цифра\\* на позиции \\*позиция\\* правильная\\. Эта цифра фиксируется в вашем прогрессе\\.\\n"
        f"   \\\\- ↔️ \\*Не на месте\\*\\: Цифра \\*угаданная\\_цифра\\* есть в коде соперника, но находится на \\*другой позиции\\*\\.\\n"
        f"   \\\\- ❌ \\*Нет в коде\\*\\: Цифры \\*угаданная\\_цифра\\* нет в коде соперника вообще\\.\\n"
        f"4\\. \\*Прогресс\\*\\: Угаданные на своих местах цифры отображаются в вашем прогрессе\\. Ваша цель \\- заполнить все 5 позиций\\.\\n"
        f"5\\. \\*Победа в матче\\*\\: Вы побеждаете в матче, как только угадаете все 5 цифр кода соперника\\!\\n"
        f"6\\. \\*Турнир\\*\\: В турнире вы будете сражаться с другими игроками на выбывание\\. Проигравший выбывает, а победитель продвигается в следующий раунд\\.\\n"
        f"7\\. \\*Цель турнира\\*\\: Стать единственным выжившим и занять 1 место\\!\\n"
        f"8\\. \\*Выход из турнира\\*\\: Если вы хотите досрочно выйти из турнира, используйте команду \\`/exit\\`\\. Обратите внимание, что это приведет к вашему поражению в текущем матче \\(если он активен\\) и выбыванию из турнира\\. После ввода \\`/exit\\` потребуется подтверждение с помощью команд \\`/yes\\` или \\`/no\\`\\.\\n"
        f"9\\. \\*Начало игры\\*\\: Чтобы начать, используйте команду /start, затем установите свой код и нажмите /ready для регистрации в турнире\\."
    )

    await update.message.reply_text(rules_text_content, parse_mode=ParseMode.MARKDOWN_V2)

@update_activity_decorator
async def set_code(update: Update, context: ContextTypes.DEFAULT_TYPE, direct_code: str = None):
    user_id = update.effective_user.id
    username_raw = update.effective_user.username or update.effective_user.first_name
    username = username_raw if username_raw else f"Игрок {user_id}" # Handle None username
    secret_code_from_input = None # Renamed to avoid conflict with DB secret_code

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state, secret_code FROM players WHERE user_id = ?', (user_id,)) # Fetch secret_code as well
        player_data = cur.fetchone()
        current_state = player_data[0] if player_data else START
        current_secret_code_in_db = player_data[1] if player_data else None # Get current secret code from DB

        if direct_code:
            secret_code_from_input = direct_code
        elif context.args and len(context.args[0]) == CODE_LENGTH and context.args[0].isdigit():
            secret_code_from_input = context.args[0]
        else:
            # If no code provided, and not in a state that expects a code, inform user
            if current_state not in [SET_CODE, SET_NEW_ROUND_CODE, START] and current_secret_code_in_db is not None:
                if not (update.message and update.message.web_app_data):
                    await update.message.reply_text("Вы уже установили код или находитесь в активном матче/ожидании\\. Если вы хотите изменить код, дождитесь завершения турнира или свяжитесь с администратором\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
            else: # Prompt for code if it's the right state or code is null
                if not (update.message and update.message.web_app_data):
                    await update.message.reply_text(f"Пожалуйста, введите {CODE_LENGTH}\\-значный секретный код \\(все цифры должны быть уникальными\\)\\., например\\: \\`/set_code 12345\\` или просто отправьте {CODE_LENGTH} цифр\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return


        # Common code validation
        if not (secret_code_from_input.isdigit() and len(secret_code_from_input) == CODE_LENGTH and len(set(secret_code_from_input)) == CODE_LENGTH and secret_code_from_input[0] != '0'):
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text(f"Код должен состоять из {CODE_LENGTH} \\*разных цифр\\* и не начинаться с нуля\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Logic for setting the code
        # If the current code in DB is NULL, or state is SET_CODE, START, or SET_NEW_ROUND_CODE
        if current_secret_code_in_db is None or current_state in [SET_CODE, START, SET_NEW_ROUND_CODE]:
            cur.execute('UPDATE players SET secret_code = ?, state = ? WHERE user_id = ?', (secret_code_from_input, WAITING_FOR_TOURNAMENT, user_id))
            conn.commit()

            # --- ИЗМЕНЕНО: Разные сообщения в зависимости от состояния ---
            if not (update.message and update.message.web_app_data): # Отправляем сообщение только если это не из WebApp
                if current_state in [START, SET_CODE]:
                    # Игрок только начинает или еще не зарегистрирован на турнир
                    await update.message.reply_text(f"Отлично\\! Ваш новый секретный код \\*{escape_markdown(secret_code_from_input, version=2)}\\* установлен\\. Нажмите /ready для регистрации в турнире\\.", parse_mode=ParseMode.MARKDOWN_V2)
                    logger.info(f"Игрок {username} ({user_id}) установил начальный код {secret_code_from_input}.")
                elif current_state in [SET_NEW_ROUND_CODE, WAITING_FOR_TOURNAMENT]:
                    # Игрок переходит на следующий раунд или уже ждет турнира
                    await update.message.reply_text(f"Отлично\\! Ваш новый секретный код \\*{escape_markdown(secret_code_from_input, version=2)}\\* установлен\\. Ожидайте, пока ваш оппонент установит свой код или турнир начнется\\.", parse_mode=ParseMode.MARKDOWN_V2)
                    logger.info(f"Игрок {username} ({user_id}) установил код {secret_code_from_input} для нового раунда.")
                else:
                    # Общий случай, если состояние не соответствует ожидаемым для установки кода
                    await update.message.reply_text(f"Ваш секретный код обновлен на \\*{escape_markdown(secret_code_from_input, version=2)}\\*\\. Ожидайте начала матча\\.", parse_mode=ParseMode.MARKDOWN_V2)
                    logger.info(f"Игрок {username} ({user_id}) обновил код на {secret_code_from_input}.")
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        else:
            # This 'else' should now only be hit if they have a code and are in IN_MATCH, or some other unexpected state
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы уже установили код или находитесь в активном матче/ожидании\\. Если вы хотите изменить код, дождитесь завершения турнира или свяжитесь с администратором\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в set_code: {e}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла ошибка при установке кода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в set_code: {e}\n{traceback.format_exc()}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла непредвиденная ошибка при установке кода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def ready(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username_raw = update.effective_user.username or update.effective_user.first_name
    username = username_raw if username_raw else f"Игрок {user_id}" # Handle None username

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT secret_code, state, tournament_id FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()

        if not player_data:
            await update.message.reply_text("Пожалуйста, сначала используйте команду /start, чтобы зарегистрироваться\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        secret_code, player_state, current_tournament_id = player_data

        if not secret_code:
            await update.message.reply_text("Прежде чем подтвердить готовность, пожалуйста, установите свой секретный код командой \\`/set_code XXXXX\\`\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Проверяем, есть ли активный турнир, куда можно зарегистрироваться (статус 'registration')
        cur.execute("SELECT tournament_id, tournament_name, min_players FROM tournaments WHERE status = 'registration' ORDER BY tournament_id DESC LIMIT 1")
        active_tournament = cur.fetchone()

        if active_tournament:
            tournament_id, tournament_name, min_players = active_tournament

            # Проверяем, зарегистрирован ли игрок уже в этом турнире
            cur.execute("SELECT user_id FROM tournament_players WHERE tournament_id = ? AND user_id = ?", (tournament_id, user_id))
            is_registered_in_this_tournament = cur.fetchone()

            if is_registered_in_this_tournament:
                 await update.message.reply_text(f"Вы уже зарегистрированы на турнир '{escape_markdown(tournament_name, version=2)}' \\(ID\\: {tournament_id}\\) и готовы к игре\\. Ожидайте начала\\.", parse_mode=ParseMode.MARKDOWN_V2)
                 return

            # Регистрируем игрока в турнире
            cur.execute('INSERT INTO tournament_players (tournament_id, user_id, current_round) VALUES (?, ?, 1)', (tournament_id, user_id))
            cur.execute('UPDATE players SET is_ready = 1, state = ?, tournament_id = ?, current_round = 1 WHERE user_id = ?',
                        (WAITING_FOR_TOURNAMENT, tournament_id, user_id))
            cur.execute('UPDATE tournaments SET players_count = players_count + 1 WHERE tournament_id = ?', (tournament_id,))
            conn.commit()

            # Получаем актуальное количество зарегистрированных игроков после добавления
            cur.execute("SELECT COUNT(user_id) FROM tournament_players WHERE tournament_id = ?", (tournament_id,))
            registered_players_count = cur.fetchone()[0]

            await update.message.reply_text(
                f"Вы успешно зарегистрированы на турнир '{escape_markdown(tournament_name, version=2)}' \\(ID\\: {tournament_id}\\) и готовы к игре\\! 🎉\\n"
                f"Сейчас зарегистрировано игроков\\: \\*{registered_players_count}/{min_players}\\*\\.\\n"
                f"Турнир начнется автоматически, как только наберется достаточно участников\\. Ожидайте начала матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Игрок {username} ({user_id}) зарегистрирован на турнир {tournament_id}.")

        else:
            # Нет активного турнира в стадии регистрации
            await update.message.reply_text(
                "В данный момент нет активных турниров для регистрации\\. Пожалуйста, дождитесь, пока администратор создаст новый турнир\\.", parse_mode=ParseMode.MARKDOWN_V2
            )

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в ready: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации на турнир\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в ready: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

# Эта функция теперь не используется для автоматического запуска, только для логирования
async def _check_and_start_tournament(tournament_id: int, bot):
    """
    Проверяет, все ли зарегистрированные игроки готовы.
    Эта функция теперь только логирует статус, но не запускает турнир автоматически.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(user_id) FROM tournament_players WHERE tournament_id = ?", (tournament_id,))
        registered_players_count = cur.fetchone()[0]

        cur.execute('''
            SELECT COUNT(tp.user_id)
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = ? AND p.is_ready = 1
        ''', (tournament_id,))
        ready_players_count = cur.fetchone()[0]

        cur.execute("SELECT status FROM tournaments WHERE tournament_id = ?", (tournament_id,))
        tournament_status = cur.fetchone()[0]

        logger.info(f"Турнир {tournament_id} в статусе '{tournament_status}'. Зарегистрировано: {registered_players_count}, Готовы: {ready_players_count}.")
        if tournament_status == 'registration' and registered_players_count >= 2 and registered_players_count == ready_players_count:
            logger.info(f"Турнир {tournament_id} готов к запуску администратором.")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в _check_and_start_tournament: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в _check_and_start_tournament: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def tournament_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT tournament_id, tournament_name, status, current_round, start_time, end_time, players_count, winner_id, min_players FROM tournaments ORDER BY tournament_id DESC LIMIT 1")
        tournament_data = cur.fetchone()

        if not tournament_data:
            await update.message.reply_text("Активных или завершенных турниров не найдено\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        tournament_id, tournament_name, status, current_round, start_time, end_time, players_count, winner_id, min_players = tournament_data

        info_message = f"🏆 \\*Информация о турнире ID {tournament_id} \\('{escape_markdown(tournament_name, version=2)}'\\)\\:*\\n"
        info_message += f"   \\*Статус\\*\\: \\*{escape_markdown(status.capitalize(), version=2)}\\*\\n"
        if status == 'registration':
            cur.execute("SELECT COUNT(*) FROM tournament_players WHERE tournament_id = ?", (tournament_id,))
            registered_players_count = cur.fetchone()[0]
            info_message += f"   \\*Зарегистрировано игроков\\*\\: {registered_players_count}/{min_players}\\n"
            info_message += "   Ожидается начало раундов\\.\\n"
        elif status == 'active':
            info_message += f"   \\*Текущий раунд\\*\\: \\*{current_round}\\*\\n"
            cur.execute("SELECT COUNT(*) FROM tournament_players WHERE tournament_id = ? AND is_eliminated = 0", (tournament_id,))
            remaining_players = cur.fetchone()[0]
            info_message += f"   \\*Осталось игроков\\*\\: {remaining_players}\\n"

            cur.execute("SELECT player1_id, player2_id FROM matches WHERE tournament_id = ? AND round_number = ? AND status = 'active'", (tournament_id, current_round))
            active_matches_data = cur.fetchall()
            if active_matches_data:
                info_message += "\\n   \\*Активные матчи текущего раунда\\*\\:\\n"
                for p1_id, p2_id in active_matches_data:
                    p1_name_raw = (await context.bot.get_chat(p1_id)).username or (await context.bot.get_chat(p1_id)).first_name
                    p1_name = p1_name_raw if p1_name_raw else f"Игрок {p1_id}" # Handle None username
                    p2_name_raw = (await context.bot.get_chat(p2_id)).username or (await context.bot.get_chat(p2_id)).first_name
                    p2_name = p2_name_raw if p2_name_raw else f"Игрок {p2_id}" # Handle None username
                    info_message += f"     \\\\- {escape_markdown(p1_name, version=2)} vs {escape_markdown(p2_name, version=2)}\\n"
            else:
                info_message += "\\n   Активных матчей в этом раунде нет\\. Ожидается переход в следующий раунд или завершение турнира\\."
                cur.execute('SELECT p.username, tp.user_id FROM tournament_players tp JOIN players p ON tp.user_id = p.user_id WHERE tp.tournament_id = ? AND tp.is_bye = 1 AND tp.current_round = ?', (tournament_id, current_round + 1))
                bye_players = cur.fetchall()
                if bye_players:
                    info_message += "\\n   \\*Игроки со свободным проходом \\(Bye\\)\\*\\:\\n"
                    for username_raw, user_id_bye in bye_players:
                        username_bye = username_raw if username_raw else f"Игрок {user_id_bye}" # Handle None username
                        info_message += f"     \\\\- {escape_markdown(username_bye, version=2)}\\n"


        elif status == 'completed':
            info_message += f"   \\*Завершено\\*\\: {escape_markdown(end_time.split('.')[0], version=2)}\\n"
            if winner_id:
                winner_name_raw = (await context.bot.get_chat(winner_id)).username or (await context.bot.get_chat(winner_id)).first_name
                winner_name = winner_name_raw if winner_name_raw else f"Игрок {winner_id}" # Handle None username
                info_message += f"   \\*Победитель\\*\\: \\*{escape_markdown(winner_name, version=2)}\\* 👑\\n"
                cur.execute("SELECT second_place_id, third_place_id FROM tournaments WHERE tournament_id = ?", (tournament_id,))
                second_place_id, third_place_id = cur.fetchone()
                if second_place_id:
                    second_name_raw = (await context.bot.get_chat(second_place_id)).username or (await context.bot.get_chat(second_place_id)).first_name
                    second_name = second_name_raw if second_name_raw else f"Игрок {second_place_id}" # Handle None username
                    info_message += f"   \\*2 место\\*\\: {escape_markdown(second_name, version=2)}\\n"
                if third_place_id:
                    third_name_raw = (await context.bot.get_chat(third_place_id)).username or (await context.bot.get_chat(third_place_id)).first_name
                    third_name = third_name_raw if third_name_raw else f"Игрок {third_place_id}" # Handle None username
                    info_message += f"   \\*3 место\\*\\: {escape_markdown(third_name, version=2)}\\n"
            else:
                info_message += "   Победитель не определен \\(турнир был отменен или завершен без явного победителя\\)\\."

        await update.message.reply_text(info_message, parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в tournament_info: {e}")
        await update.message.reply_text("Произошла ошибка при получении информации о турнире\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в tournament_info: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await update.message.reply_text("Произошла непредвиденная ошибка при получении информации о турнире\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- Визуализация турнирной сетки ---
async def generate_tournament_bracket_image(tournament_id: int, bot) -> io.BytesIO:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT players_count, current_round FROM tournaments WHERE tournament_id = ?", (tournament_id,))
    tour_info = cur.fetchone()
    if not tour_info:
        conn.close()
        return None
    
    total_players_registered = tour_info[0]
    current_tournament_round = tour_info[1]

    num_rounds = math.ceil(math.log2(max(2, total_players_registered)))
    if total_players_registered > 2**(num_rounds-1) and num_rounds > 0: # Уточнение для случаев, когда игроков больше, чем 2^(N-1) но меньше 2^N
        pass # num_rounds уже корректен
    elif total_players_registered == 0:
        num_rounds = 0 # Нет игроков, нет раундов

    cur.execute('''
        SELECT m.round_number, m.player1_id, p1.username, m.player2_id, p2.username, m.winner_id
        FROM matches m
        JOIN players p1 ON m.player1_id = p1.user_id
        JOIN players p2 ON m.player2_id = p2.user_id
        WHERE m.tournament_id = ?
        ORDER BY m.round_number, m.match_id
    ''', (tournament_id,))
    matches_data = cur.fetchall()

    cur.execute('''
        SELECT tp.user_id, p.username, tp.current_round
        FROM tournament_players tp
        JOIN players p ON tp.user_id = p.user_id
        WHERE tp.tournament_id = ? AND tp.is_bye = 1
    ''', (tournament_id,))
    bye_players_data = cur.fetchall()

    conn.close()

    player_height = 40
    player_width = 200
    padding_x = 20
    padding_y = 10
    round_spacing_x = 250
    match_spacing_y = 20

    # Расчет высоты изображения должен учитывать максимальное количество слотов в первом раунде
    # Это 2^num_rounds
    initial_slots = 2**num_rounds
    image_height = initial_slots * (player_height + match_spacing_y) + padding_y * 2
    image_width = num_rounds * round_spacing_x + padding_x * 2 + player_width

    img = Image.new('RGB', (image_width, image_height), color='white')
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("arial.ttf", 16)
    except IOError:
        logger.warning("Шрифт arial.ttf не найден. Используется шрифт по умолчанию.")
        font = ImageFont.load_default()

    player_positions = {}
    
    for r in range(1, num_rounds + 1):
        round_x_start = padding_x + (r - 1) * round_spacing_x
        
        draw.text((round_x_start, padding_y / 2), f"Раунд {r}", fill="black", font=font)

        current_round_matches = [m for m in matches_data if m[0] == r]
        
        if r == 1:
            # Для первого раунда отображаем всех зарегистрированных игроков
            cur_players = sqlite3.connect(DB_PATH).cursor()
            cur_players.execute('''
                SELECT tp.user_id, p.username, tp.is_eliminated, tp.is_bye
                FROM tournament_players tp
                JOIN players p ON tp.user_id = p.user_id
                WHERE tp.tournament_id = ?
                ORDER BY tp.user_id
            ''', (tournament_id,))
            initial_players_db = cur_players.fetchall()
            cur_players.connection.close()

            players_for_first_round_visual = []
            for p_id, p_name_raw, is_elim, is_bye in initial_players_db:
                # Ensure p_name is not None
                display_name = p_name_raw if p_name_raw else f"ID: {p_id}"
                players_for_first_round_visual.append({'id': p_id, 'name': display_name, 'is_eliminated': is_elim, 'is_bye': is_bye})

            # Сортируем для более логичного отображения
            players_for_first_round_visual.sort(key=lambda x: (x['is_eliminated'], x['is_bye'], x['name']))

            # Рассчитываем начальный y-offset для центрирования
            # Количество слотов в первом раунде (2^num_rounds)
            num_slots_in_first_round = 2**(num_rounds)
            total_visual_height_for_players = len(players_for_first_round_visual) * (player_height + match_spacing_y) - match_spacing_y
            
            # Если игроков меньше, чем слотов, центрируем их
            y_start_offset = (image_height - total_visual_height_for_players) / 2 if total_visual_height_for_players < image_height else padding_y

            current_y = y_start_offset
            for i, player_info in enumerate(players_for_first_round_visual):
                player_id = player_info['id']
                player_name = player_info['name'] # This is already handled for None
                is_eliminated = player_info['is_eliminated']
                is_bye = player_info['is_bye']

                player_y = current_y
                
                rect_color = "grey"
                text_color = "black"
                status_text = ""

                if is_eliminated:
                    text_color = "red"
                    status_text = " (Выбыл)"
                elif is_bye:
                    text_color = "blue"
                    status_text = " (BYE)"
                
                draw.rectangle([(round_x_start, player_y), (round_x_start + player_width, player_y + player_height)], outline=rect_color)
                draw.text((round_x_start + 5, player_y + 10), f"{player_name}{status_text}", fill=text_color, font=font)
                
                player_positions[player_id] = (round_x_start + player_width, player_y + player_height / 2)
                current_y += (player_height + match_spacing_y)

        else: # Для раундов > 1
            # Рассчитываем начальный y-offset для центрирования матчей в этом раунде
            num_matches_in_round = len(current_round_matches)
            # Учитываем BYE игроков, которые перешли в этот раунд
            bye_players_in_this_round = [p_id for p_id, p_name, p_round in bye_players_data if p_round == r]
            
            total_visual_height_for_round = (num_matches_in_round * 2 * (player_height + match_spacing_y)) + (len(bye_players_in_this_round) * (player_height + match_spacing_y))
            y_start_offset = (image_height - total_visual_height_for_round) / 2 if total_visual_height_for_round < image_height else padding_y

            current_y = y_start_offset
            
            # Сначала рисуем BYE игроков, которые перешли в этот раунд
            for bye_p_id in bye_players_in_this_round:
                cur_player_info = sqlite3.connect(DB_PATH).cursor()
                cur_player_info.execute('SELECT username FROM players WHERE user_id = ?', (bye_p_id,))
                bye_p_name_raw = cur_player_info.fetchone()[0]
                bye_p_name = bye_p_name_raw if bye_p_name_raw else f"ID: {bye_p_id}" # Handle None username
                cur_player_info.connection.close()

                draw.rectangle([(round_x_start, current_y), (round_x_start + player_width, current_y + player_height)], outline="blue")
                draw.text((round_x_start + 5, current_y + 10), f"{bye_p_name} (BYE)", fill="blue", font=font)
                player_positions[bye_p_id] = (round_x_start + player_width, current_y + player_height / 2)
                current_y += (player_height + match_spacing_y)

            # Затем рисуем матчи текущего раунда
            for i in range(0, len(current_round_matches)):
                match_round, p1_id, p1_name_raw, p2_id, p2_name_raw, winner_id = current_round_matches[i]
                p1_name = p1_name_raw if p1_name_raw else f"ID: {p1_id}" # Handle None username
                p2_name = p2_name_raw if p2_name_raw else f"ID: {p2_id}" # Handle None username
                
                p1_y_center = current_y + player_height / 2
                p2_y_center = current_y + player_height + match_spacing_y + player_height / 2

                # Рисуем линии от предыдущего раунда к текущему
                if p1_id in player_positions:
                    draw.line([player_positions[p1_id], (round_x_start, p1_y_center)], fill="black", width=2)
                
                if p2_id in player_positions:
                    draw.line([player_positions[p2_id], (round_x_start, p2_y_center)], fill="black", width=2)

                p1_text_color = "black"
                p2_text_color = "black"
                if winner_id:
                    if winner_id == p1_id:
                        p1_text_color = "green"
                        p2_text_color = "red"
                    else:
                        p1_text_color = "red"
                        p2_text_color = "green"

                draw.text((round_x_start + 5, p1_y_center - player_height / 2 + 10), p1_name, fill=p1_text_color, font=font)
                draw.text((round_x_start + 5, p2_y_center - player_height / 2 + 10), p2_name, fill=p2_text_color, font=font)
                
                draw.rectangle([(round_x_start, p1_y_center - player_height / 2), (round_x_start + player_width, p1_y_center + player_height / 2)], outline="grey")
                draw.rectangle([(round_x_start, p2_y_center - player_height / 2), (round_x_start + player_width, p2_y_center + player_height / 2)], outline="grey")

                match_center_y = (p1_y_center + p2_y_center) / 2
                draw.line([(round_x_start + player_width, p1_y_center), (round_x_start + player_width + padding_x, p1_y_center)], fill="black", width=2)
                draw.line([(round_x_start + player_width, p2_y_center), (round_x_start + player_width + padding_x, p2_y_center)], fill="black", width=2)
                draw.line([(round_x_start + player_width + padding_x, p1_y_center), (round_x_start + player_width + padding_x, p2_y_center)], fill="black", width=2)
                draw.line([(round_x_start + player_width + padding_x, match_center_y), (round_x_start + player_width + padding_x + round_spacing_x - player_width - padding_x, match_center_y)], fill="black", width=2)
                
                if winner_id:
                    player_positions[winner_id] = (round_x_start + player_width + round_spacing_x - player_width - padding_x, match_center_y)
                
                current_y += (player_height + match_spacing_y) * 2 + match_spacing_y
                
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)
    return img_byte_arr

# --- АДМИН ПАНЕЛЬ ---

@update_activity_decorator
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    keyboard = [
        [InlineKeyboardButton("Создать новый турнир", callback_data="admin_start_new_tournament_callback")],
        [InlineKeyboardButton("Отменить активный матч", callback_data="admin_cancel_match_callback")],
        [InlineKeyboardButton("Завершить текущий турнир", callback_data="admin_end_tournament_callback")],
        [InlineKeyboardButton("Показать статистику", callback_data="admin_stats_callback")],
        [InlineKeyboardButton("Сбросить игру", callback_data="admin_reset_game_callback")],
        [InlineKeyboardButton("Сделать рассылку", callback_data="admin_broadcast_callback")],
        [InlineKeyboardButton("Принудительный статус регистрации", callback_data="admin_force_registration_status")],
        [InlineKeyboardButton("Принудительный статус активных матчей", callback_data="admin_force_active_matches")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Добро пожаловать в админ панель\\! Выберите действие\\:", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)


# --- АДМИН КОМАНДЫ (измененные/новые) ---

@update_activity_decorator
async def start_new_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_to_reply = update.effective_message # Используем effective_message

    if user_id != ADMIN_USER_ID:
        await message_to_reply.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT tournament_id, status FROM tournaments WHERE status IN ('registration', 'active')")
        active_tournament = cur.fetchone()

        if active_tournament:
            tour_id, tour_status = active_tournament
            msg = f"Уже есть активный турнир \\(\\*ID\\: {tour_id}\\*, статус\\: \\*{escape_markdown(tour_status, version=2)}\\*\\)\\. Завершите его, прежде чем начинать новый\\."
            # Если это callback_query и сообщение совпадает с effective_message, редактируем его
            if update.callback_query and update.callback_query.message == message_to_reply:
                await update.callback_query.message.edit_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
            else: # Иначе просто отвечаем
                await message_to_reply.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Эта строка была причиной ошибки, теперь использует effective_message
        await message_to_reply.reply_text("Введите название турнира и минимальное количество игроков через пробел \\(например, \\`8 Весенний турнир\\`\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
        cur.execute('UPDATE players SET state = ? WHERE user_id = ?', (AWAITING_TOURNAMENT_DETAILS, user_id))
        conn.commit()
        logger.info(f"Администратор {user_id} переведен в состояние AWAITING_TOURNAMENT_DETAILS.")

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при создании нового турнира: {e}")
        await message_to_reply.reply_text("Произошла ошибка при создании нового турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка при создании нового турнира: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await message_to_reply.reply_text("Произошла непредвиденная ошибка при создании нового турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# Функция begin_tournament теперь не вызывается из админ-кнопки "Запустить турнир",
# так как запуск автоматический. Оставляем ее, если она нужна для других целей.
# Если нет, то ее можно удалить.
@update_activity_decorator
async def begin_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    await update.message.reply_text("Турнир теперь запускается автоматически, когда набирается достаточно игроков\\. Эта команда больше не нужна для обычного запуска\\.", parse_mode=ParseMode.MARKDOWN_V2)

# --- Exit game command ---
@update_activity_decorator
async def exit_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state, tournament_id FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()

        if not player_data:
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы не зарегистрированы в игре\\. Используйте /start, чтобы начать\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        player_state, tournament_id = player_data

        if player_state in [IN_MATCH, WAITING_FOR_TOURNAMENT, AWAITING_EXIT_CONFIRMATION]:
            # Store the current state in user_data for "нет" option
            context.user_data['previous_state_before_exit'] = player_state
            await update_user_state(user_id, AWAITING_EXIT_CONFIRMATION)
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Ты точно хочешь выйти из турнира\\? Если ты уверен, введи \\`/yes\\`\\. Если ты передумал, введи \\`/no\\`\\.", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы не участвуете в активном турнире или матче, чтобы выйти\\. Используйте /start для начала игры\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в exit_game: {e}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла ошибка при попытке выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в exit_game: {e}\n{traceback.format_exc()}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла непредвиденная ошибка при попытке выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def confirm_exit_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state, tournament_id FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()
        current_state, tournament_id = player_data if player_data else (START, None)

        if current_state == AWAITING_EXIT_CONFIRMATION:
            await _process_exit(update, context)
        else:
            await update.message.reply_text("Неожиданная команда\\. Используйте /exit, чтобы начать процесс выхода из турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в confirm_exit_yes: {e}")
        await update.message.reply_text("Произошла ошибка при подтверждении выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в confirm_exit_yes: {e}\n{traceback.format_exc()}")
        await update.message.reply_text("Произошла непредвиденная ошибка при подтверждении выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def confirm_exit_no(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()
        current_state = player_data[0] if player_data else START

        if current_state == AWAITING_EXIT_CONFIRMATION:
            previous_state = context.user_data.pop('previous_state_before_exit', START)
            await update_user_state(user_id, previous_state)
            await update.message.reply_text("Выход отменен\\. Вы можете продолжить игру\\.", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await update.message.reply_text("Неожиданная команда\\. Вы не находитесь в процессе выхода из турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в confirm_exit_no: {e}")
        await update.message.reply_text("Произошла ошибка при отмене выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в confirm_exit_no: {e}\n{traceback.format_exc()}")
        await update.message.reply_text("Произошла непредвиденная ошибка при отмене выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()


async def _process_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute('SELECT tournament_id FROM players WHERE user_id = ?', (user_id,))
        tournament_id_data = cur.fetchone()
        tournament_id = tournament_id_data[0] if tournament_id_data else None

        if not tournament_id:
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы не участвуете в турнире\\.", parse_mode=ParseMode.MARKDOWN_V2)
            await update_user_state(user_id, START) # Reset state
            return

        # Check for active match
        cur.execute('SELECT match_id, player1_id, player2_id FROM matches WHERE (player1_id = ? OR player2_id = ?) AND status = \'active\'', (user_id, user_id))
        active_match = cur.fetchone()

        if active_match:
            match_id, player1_id, player2_id = active_match
            opponent_id = player2_id if user_id == player1_id else player1_id

            # End the match, opponent wins
            await end_match(match_id, opponent_id, context.bot)
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы успешно вышли из турнира\\. Ваш оппонент победил в текущем матче\\.", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Игрок {user_id} вышел из матча {match_id}. Оппонент {opponent_id} победил.")
        else:
            # If not in an active match, just eliminate from tournament
            if not (update.message and update.message.web_app_data):
                await update.message.reply_text("Вы успешно вышли из турнира\\. Игрок был удален из турнира\\.", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Игрок {user_id} вышел из турнира {tournament_id} (не в активном матче).")

        # Mark player as eliminated in tournament_players table
        cur.execute('UPDATE tournament_players SET is_eliminated = 1 WHERE user_id = ? AND tournament_id = ?', (user_id, tournament_id))
        # Reset player state and tournament info in players table
        cur.execute('UPDATE players SET state = ?, tournament_id = NULL, current_round = 0, is_eliminated = 1, secret_code = NULL WHERE user_id = ?', (START, user_id))
        conn.commit()

        # Check tournament status to potentially advance rounds or end tournament
        await check_tournament_status(tournament_id, context.bot)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в _process_exit: {e}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла ошибка при обработке выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в _process_exit: {e}\n{traceback.format_exc()}")
        if not (update.message and update.message.web_app_data):
            await update.message.reply_text("Произошла непредвиденная ошибка при обработке выхода\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- НОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ АДМИНА ---

async def _send_player_readiness_info_to_admin(context: ContextTypes.DEFAULT_TYPE, tournament_id: int):
    """Отправляет админу информацию о готовности игроков для регистрации, обновляя предыдущее сообщение."""
    if ADMIN_USER_ID is None:
        logger.warning("ADMIN_USER_ID не установлен. Невозможно отправить информацию о готовности игроков.")
        return

    last_message_id = _get_admin_message_id(ADMIN_USER_ID, 'tournament_status')
    logger.info(f"Retrieved last_tournament_status_msg_id: {last_message_id}")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT tournament_name, min_players FROM tournaments WHERE tournament_id = ? AND status = 'registration'", (tournament_id,))
        active_tournament = cur.fetchone()

        response_message = "📊 \\*Статус регистрации турнира\\*\\:\\n\\n"

        if active_tournament:
            tournament_name, min_players = active_tournament
            
            # Получаем всех игроков и их статус регистрации на этот турнир
            cur.execute('''
                SELECT p.user_id, p.username, tp.user_id IS NOT NULL AS is_registered
                FROM players p
                LEFT JOIN tournament_players tp ON p.user_id = tp.user_id AND tp.tournament_id = ?
                ORDER BY p.username
            ''', (tournament_id,))
            all_players_data = cur.fetchall()

            registered_count = sum(1 for p_id, p_name, is_reg in all_players_data if is_reg)

            response_message += (
                f"\\*Турнир\\*\\: '{escape_markdown(tournament_name, version=2)}' \\(ID\\: \\`{tournament_id}\\`\\)\\n"
                f"Зарегистрировано\\: \\*{registered_count}/{min_players}\\* игроков\\.\\n"
                f"Ожидаем {min_players - registered_count} игроков для старта\\.\\n\\n"
                f"\\*Список игроков\\*\\:\\n"
            )

            if not all_players_data:
                response_message += "  Нет зарегистрированных игроков в системе\\."
            else:
                for p_id, p_username_raw, is_registered in all_players_data:
                    p_username = p_username_raw if p_username_raw else f"ID: {p_id}"
                    status_text = "да" if is_registered else "нет"
                    response_message += f"  @{escape_markdown(p_username, version=2)} \\({status_text}\\)\\n"
        else:
            response_message += "В данный момент нет активных турниров для регистрации\\."
        
        try:
            if last_message_id:
                logger.info(f"Attempting to edit tournament status message {last_message_id} for admin {ADMIN_USER_ID}.")
                await context.bot.edit_message_text(
                    chat_id=ADMIN_USER_ID,
                    message_id=last_message_id,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Successfully edited tournament status message {last_message_id} for admin {ADMIN_USER_ID}.")
            else:
                logger.info(f"No previous tournament status message ID found for admin {ADMIN_USER_ID}. Sending new message.")
                sent_message = await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Sent new tournament status message {sent_message.message_id} to admin {ADMIN_USER_ID}. Storing ID.")
                _set_admin_message_id(ADMIN_USER_ID, 'tournament_status', sent_message.message_id)
        except BadRequest as e: # Catch BadRequest specifically
            if "Message is not modified" in str(e):
                logger.info(f"Message {last_message_id} for admin {ADMIN_USER_ID} was not modified. Skipping new message.")
            else:
                logger.warning(f"Failed to edit tournament status message {last_message_id} for admin {ADMIN_USER_ID}: {e}. Sending new message and updating ID.")
                sent_message = await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Sent new tournament status message {sent_message.message_id} to admin {ADMIN_USER_ID} after edit failure. Storing new ID.")
                _set_admin_message_id(ADMIN_USER_ID, 'tournament_status', sent_message.message_id)
        except Exception as e:
            logger.error(f"Unexpected error during admin tournament status message handling: {e}\n{traceback.format_exc()}")
            await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Произошла непредвиденная ошибка при отправке отчета о статусе турнира администратору\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

async def _send_active_matches_info_to_admin(context: ContextTypes.DEFAULT_TYPE, tournament_id: int):
    """Отправляет администратору информацию обо всех активных матчах, обновляя предыдущее сообщение."""
    if ADMIN_USER_ID is None:
        logger.warning("ADMIN_USER_ID не установлен. Невозможно отправить информацию об активных матчах.")
        return

    last_message_id = _get_admin_message_id(ADMIN_USER_ID, 'active_matches')
    logger.info(f"Retrieved last_active_matches_msg_id: {last_message_id}")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Выбираем все активные матчи для данного турнира
        cur.execute('''
            SELECT m.match_id, m.player1_id, p1.username, m.player2_id, p2.username, 
                   m.current_player_turn, m.player1_cracked_code, m.player2_cracked_code,
                   m.tournament_id, m.round_number
            FROM matches m
            JOIN players p1 ON m.player1_id = p1.user_id
            JOIN players p2 ON m.player2_id = p2.user_id
            WHERE m.status = 'active' AND m.tournament_id = ?
        ''', (tournament_id,))
        active_matches = cur.fetchall()

        response_message = "⚔️ \\*Активные матчи\\*\\:\\n\\n"
        if not active_matches:
            response_message += "В данный момент активных матчей нет\\."
        else:
            for match_data in active_matches:
                match_id, p1_id, p1_username_raw, p2_id, p2_username_raw, \
                current_turn_id, p1_cracked_code_json, p2_cracked_code_json, \
                tournament_id, round_number = match_data

                p1_username = p1_username_raw if p1_username_raw else f"Игрок {p1_id}"
                p2_username = p2_username_raw if p2_username_raw else f"Игрок {p2_id}"

                current_turn_username_raw = (await context.bot.get_chat(current_turn_id)).username or (await context.bot.get_chat(current_turn_id)).first_name
                current_turn_username = current_turn_username_raw if current_turn_username_raw else f"Игрок {current_turn_id}"

                p1_cracked_code = " ".join(json.loads(p1_cracked_code_json))
                p2_cracked_code = " ".join(json.loads(p2_cracked_code_json))

                match_info = (
                    f"\\*Матч ID\\*\\: \\`{match_id}\\` \\(Турнир\\: {tournament_id}, Раунд\\: {round_number}\\)\\n"
                    f"  @{escape_markdown(p1_username, version=2)} \\(Код\\: \\`{escape_markdown(p1_cracked_code, version=2)}\\`\\) vs @{escape_markdown(p2_username, version=2)} \\(Код\\: \\`{escape_markdown(p2_cracked_code, version=2)}\\`\\)\\n"
                    f"  \\*Ход\\*\\: @{escape_markdown(current_turn_username, version=2)}\\n"
                )
                response_message += match_info + "\\n"

        try:
            if last_message_id:
                logger.info(f"Attempting to edit active matches message {last_message_id} for admin {ADMIN_USER_ID}.")
                await context.bot.edit_message_text(
                    chat_id=ADMIN_USER_ID,
                    message_id=last_message_id,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Successfully edited active matches message {last_message_id} for admin {ADMIN_USER_ID}.")
            else:
                logger.info(f"No previous active matches message ID found for admin {ADMIN_USER_ID}. Sending new message.")
                sent_message = await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Sent new active matches message {sent_message.message_id} to admin {ADMIN_USER_ID} after edit failure. Storing new ID.")
                _set_admin_message_id(ADMIN_USER_ID, 'active_matches', sent_message.message_id)
        except BadRequest as e: # Catch BadRequest specifically
            if "Message is not modified" in str(e):
                logger.info(f"Message {last_message_id} for admin {ADMIN_USER_ID} was not modified. Skipping new message.")
            else:
                logger.warning(f"Failed to edit active matches message {last_message_id} for admin {ADMIN_USER_ID}: {e}. Sending new message and updating ID.")
                sent_message = await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=response_message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Sent new active matches message {sent_message.message_id} to admin {ADMIN_USER_ID} after edit failure. Storing new ID.")
                _set_admin_message_id(ADMIN_USER_ID, 'active_matches', sent_message.message_id)
        except Exception as e:
            logger.error(f"Unexpected error during admin active matches message handling: {e}\n{traceback.format_exc()}")
            await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Произошла непредвиденная ошибка при отправке отчета об активных матчах администратору\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- НОВЫЕ ФУНКЦИИ ДЛЯ ПРИНУДИТЕЛЬНОГО ВЫЗОВА ОТЧЕТОВ ---
@update_activity_decorator
async def force_registration_status_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.callback_query.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    await update.callback_query.answer("Запрашиваю статус регистрации турнира...")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT tournament_id FROM tournaments WHERE status = 'registration' ORDER BY tournament_id DESC LIMIT 1")
        latest_tournament_id_data = cur.fetchone()
        
        if latest_tournament_id_data:
            latest_tournament_id = latest_tournament_id_data[0]
            await _send_player_readiness_info_to_admin(context, latest_tournament_id)
            await _clear_admin_message(ADMIN_USER_ID, 'active_matches', context) # Очищаем сообщение об активных матчах
        else:
            await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Нет активных турниров в стадии регистрации для отображения статуса\\.", parse_mode=ParseMode.MARKDOWN_V2)
            await _clear_admin_message(ADMIN_USER_ID, 'tournament_status', context) # Очищаем сообщение о статусе регистрации
            await _clear_admin_message(ADMIN_USER_ID, 'active_matches', context) # Очищаем сообщение об активных матчах
    except Exception as e:
        logger.error(f"Ошибка при принудительном запросе статуса регистрации: {e}\n{traceback.format_exc()}")
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Произошла непредвиденная ошибка при получении принудительного статуса регистрации\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

@update_activity_decorator
async def force_active_matches_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.callback_query.message.reply_text("У вас нет прав администратора\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    await update.callback_query.answer("Запрашиваю статус активных матчей...")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT tournament_id FROM tournaments WHERE status = 'active' ORDER BY tournament_id DESC LIMIT 1")
        latest_tournament_id_data = cur.fetchone()

        if latest_tournament_id_data:
            latest_tournament_id = latest_tournament_id_data[0]
            await _send_active_matches_info_to_admin(context, latest_tournament_id)
            await _clear_admin_message(ADMIN_USER_ID, 'tournament_status', context) # Очищаем сообщение о статусе регистрации
        else:
            await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Нет активных турниров для отображения матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
            await _clear_admin_message(ADMIN_USER_ID, 'active_matches', context) # Очищаем сообщение об активных матчах
            await _clear_admin_message(ADMIN_USER_ID, 'tournament_status', context) # Очищаем сообщение о статусе регистрации
    except Exception as e:
        logger.error(f"Ошибка при принудительном запросе активных матчей: {e}\n{traceback.format_exc()}")
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text="Произошла непредвиденная ошибка при получении принудительного статуса активных матчей\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- ПРОВЕРКА ТАЙМАУТОВ и СТАТУСА ТУРНИРА ---
async def check_game_status_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Running job \"check_game_status_job\"")
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        current_time = datetime.datetime.now()

        # 1. Обработка игроков в состоянии WAIT_READY (если они были зарегистрированы, но не стали READY для турнира)
        cur.execute('SELECT user_id, last_activity FROM players WHERE state = ?', (WAIT_READY,))
        ready_players = cur.fetchall()

        for user_id, last_activity_str in ready_players:
            last_activity = datetime.datetime.strptime(last_activity_str, '%Y-%m-%d %H:%M:%S')
            if (current_time - last_activity).total_seconds() > WAIT_READY_TIMEOUT_SECONDS:
                await context.bot.send_message(chat_id=user_id, text="Ваша сессия ожидания оппонента истекла\\. Пожалуйста, используйте /ready снова, чтобы продолжить поиск\\.", parse_mode=ParseMode.MARKDOWN_V2)
                cur.execute('UPDATE players SET state = ?, is_ready = 0 WHERE user_id = ?', (SET_CODE, user_id))
                conn.commit()
                logger.info(f"Игрок {user_id} таймаут в WAIT_READY.")
        
        # 2. Обработка игроков в состоянии IN_MATCH (таймаут на ход)
        cur.execute('SELECT match_id, player1_id, player2_id, current_player_turn, tournament_id, round_number FROM matches WHERE status = \'active\'')
        active_matches_for_timeout_check = cur.fetchall() # Renamed to avoid conflict with active_matches for admin report

        for match_id, player1_id, player2_id, current_player_turn, tournament_id_match, round_number_match in active_matches_for_timeout_check:
            
            cur.execute('SELECT last_activity FROM players WHERE user_id = ?', (current_player_turn,))
            current_player_last_activity_str_data = cur.fetchone()
            if current_player_last_activity_str_data:
                current_player_last_activity_str = current_player_last_activity_str_data[0]
                current_player_last_activity = datetime.datetime.strptime(current_player_last_activity_str, '%Y-%m-%d %H:%M:%S')
                
                time_since_last_activity = (current_time - current_player_last_activity).total_seconds()
                logger.info(f"Match {match_id}: Player {current_player_turn} last activity: {current_player_last_activity_str}. Time since last activity: {time_since_last_activity} seconds.")


                if time_since_last_activity > IN_MATCH_TIMEOUT_SECONDS:
                    opponent_id = player1_id if current_player_turn == player2_id else player1_id
                    
                    await context.bot.send_message(chat_id=current_player_turn, 
                                                   text="Вы не сделали ход вовремя\\. Ваш ход был пропущен\\. Ход переходит вашему оппоненту\\.", 
                                                   parse_mode=ParseMode.MARKDOWN_V2)
                    
                    await context.bot.send_message(chat_id=opponent_id, 
                                                   text="Ваш оппонент не сделал ход вовремя\\. Теперь ваш ход\\!", 
                                                   parse_mode=ParseMode.MARKDOWN_V2)
                    
                    cur.execute('UPDATE matches SET current_player_turn = ? WHERE match_id = ?', (opponent_id, match_id))
                    
                    new_last_activity_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute('UPDATE players SET last_activity = ? WHERE user_id = ?', (new_last_activity_time, opponent_id))
                    conn.commit()

                    logger.info(f"Ход в матче {match_id} передан игроку {opponent_id} из-за таймаута игрока {current_player_turn}.")
                else:
                    logger.info(f"Игрок {current_player_turn} в матче {match_id} еще не таймаут.")
            else:
                logger.warning(f"Не найдена активность для игрока {current_player_turn} в матче {match_id}. Пропускаем проверку таймаута.")


        # 3. Проверка статуса активных турниров (для продвижения раундов)
        cur.execute("SELECT tournament_id FROM tournaments WHERE status = 'active'")
        active_tournaments = cur.fetchall()
        for (tour_id,) in active_tournaments:
            await check_tournament_status(tour_id, context.bot)

        # 4. Проверка турниров на автоматический запуск (статус 'registration')
        cur.execute("SELECT tournament_id, tournament_name, min_players FROM tournaments WHERE status = 'registration'")
        registration_tournaments = cur.fetchall()
        for tournament_id, tournament_name, min_players in registration_tournaments:
            cur.execute("SELECT COUNT(user_id) FROM tournament_players WHERE tournament_id = ?", (tournament_id,))
            registered_players_count = cur.fetchone()[0]

            logger.info(f"Турнир '{tournament_name}' (ID: {tournament_id}): зарегистрировано {registered_players_count}/{min_players} игроков.")

            if registered_players_count >= min_players:
                logger.info(f"Турнир '{tournament_name}' (ID: {tournament_id}) достиг минимального количества игроков. Запускаем!")
                await _begin_tournament_logic(tournament_id, context.bot)
            else:
                logger.info(f"Турнир '{tournament_name}' (ID: {tournament_id}): недостаточно игроков для старта.")

        # --- НОВЫЕ УСЛОВНЫЕ АДМИН-ОТЧЕТЫ ---
        # Получаем информацию о последнем турнире (регистрация или активный)
        cur.execute("SELECT tournament_id, status FROM tournaments WHERE status IN ('registration', 'active') ORDER BY tournament_id DESC LIMIT 1")
        latest_tournament_info = cur.fetchone()

        if latest_tournament_info:
            latest_tour_id, latest_tour_status = latest_tournament_info
            if latest_tour_status == 'registration':
                await _send_player_readiness_info_to_admin(context, latest_tour_id)
                await _clear_admin_message(ADMIN_USER_ID, 'active_matches', context)
            elif latest_tour_status == 'active':
                await _send_active_matches_info_to_admin(context, latest_tour_id)
                await _clear_admin_message(ADMIN_USER_ID, 'tournament_status', context)
        else:
            # Если нет активных или регистрирующихся турниров, очищаем оба сообщения
            await _clear_admin_message(ADMIN_USER_ID, 'tournament_status', context)
            await _clear_admin_message(ADMIN_USER_ID, 'active_matches', context)
        # --- КОНЕЦ НОВЫХ УСЛОВНЫХ АДМИН-ОТЧЕТОВ ---

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в check_game_status_job: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в check_game_status_job: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()

# --- General message handlers / wrappers ---

async def handle_guess_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text.strip()
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()
        player_state = player_data[0] if player_data else None
        conn.close()

        if player_state == IN_MATCH:
            parts = message_text.split()
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and len(parts[0]) == 1:
                context.args = parts 
                await guess(update, context)
                return True
    except Exception as e:
        logger.error(f"Ошибка в handle_guess_direct: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
    finally:
        if conn:
            conn.close()
    return False

async def handle_set_code_direct_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT state FROM players WHERE user_id = ?', (user_id,))
    player_data = cur.fetchone()
    player_state = player_data[0] if player_data else None
    conn.close()

    # Allow setting code if in START, SET_CODE, WAITING_FOR_TOURNAMENT, or SET_NEW_ROUND_CODE
    if player_state in [SET_CODE, WAITING_FOR_TOURNAMENT, START, SET_NEW_ROUND_CODE]:
        secret_code = message_text
        await set_code(update, context, direct_code=secret_code)
    else:
        await handle_text_message(update, context) # Fallback to general text handler

async def handle_guess_direct_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    processed = await handle_guess_direct(update, context)
    if not processed:
        await handle_text_message(update, context)

async def admin_text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT state FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()
        player_state = player_data[0] if player_data else None 
        conn.close()

        if is_admin(user_id):
            logger.info(f"Администратор {user_id} в состоянии {player_state}. Текст: {update.message.text}") # Добавлено логирование
            if player_state == AWAITING_BROADCAST_MESSAGE:
                await admin_send_broadcast(update, context)
                return
            elif context.user_data.get('awaiting_match_id'):
                await admin_cancelmatch(update, context)
                return
            elif context.user_data.get('awaiting_tournament_id_to_end'):
                await admin_end_tournament_by_id(update, context)
                return
            elif player_state == AWAITING_TOURNAMENT_DETAILS: # Обработка деталей турнира
                await admin_create_tournament_details(update, context)
                return

        await handle_text_message(update, context) 

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в admin_text_input_handler: {e}")
        await update.message.reply_text("Произошла ошибка при обработке вашего запроса\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в admin_text_input_handler: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await update.message.reply_text("Произошла непредвиденная ошибка при обработке вашего запроса\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Общий обработчик текстовых сообщений от пользователей.
    Определяет текущее состояние пользователя и обрабатывает сообщение
    соответствующим образом.
    """
    user_id = update.effective_user.id
    user_text = update.message.text

    logger.info(f"Получено текстовое сообщение от {user_id}: {user_text}")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Получаем состояние пользователя
        cursor.execute("SELECT state, tournament_id FROM players WHERE user_id = ?", (user_id,))
        user_data = cursor.fetchone()
        
        current_state = user_data[0] if user_data else START # Если пользователя нет, считаем его в START
        player_tournament_id = user_data[1] if user_data else None

        # Определяем, является ли пользователь администратором
        is_admin_user = (user_id == ADMIN_USER_ID)

        # Логика обработки в зависимости от состояния и роли
        if is_admin_user:
            logger.info(f"Администратор {user_id} в состоянии {current_state}. Текст: {user_text}")
            if current_state == AWAITING_BROADCAST_MESSAGE:
                await admin_send_broadcast(update, context)
            elif current_state == AWAITING_TOURNAMENT_DETAILS: # Обработка ввода деталей турнира
                await admin_create_tournament_details(update, context)
            else:
                await update.message.reply_text("Админ\\: Ваше сообщение принято, но не обработано в текущем состоянии\\.", parse_mode=ParseMode.MARKDOWN_V2)

        else: # Обычный игрок
            logger.info(f"Игрок {user_id} в состоянии {current_state}. Текст: {user_text}")
            if current_state == AWAITING_EXIT_CONFIRMATION:
                # В этом состоянии ожидаем только команды /yes или /no
                await update.message.reply_text("Пожалуйста, используйте \\`/yes\\` для подтверждения или \\`/no\\` для отмены\\.", parse_mode=ParseMode.MARKDOWN_V2)
            elif current_state in [SET_CODE, SET_NEW_ROUND_CODE]: # Handle both initial code and new round code
                if len(user_text) == CODE_LENGTH and user_text.isdigit():
                    await set_code(update, context, direct_code=user_text)
                else:
                    await update.message.reply_text(f"Пожалуйста, введите корректный {CODE_LENGTH}\\-значный код\\.", parse_mode=ParseMode.MARKDOWN_V2)
            elif current_state == IN_MATCH:
                await update.message.reply_text("Вы находитесь в матче\\. Пожалуйста, используйте формат \\`цифра позиция\\` для угадывания\\.", parse_mode=ParseMode.MARKDOWN_V2)
            elif current_state == WAITING_FOR_TOURNAMENT:
                # Уточняем сообщение для WAITING_FOR_TOURNAMENT
                if player_tournament_id:
                    cursor.execute("SELECT status FROM tournaments WHERE tournament_id = ?", (player_tournament_id,))
                    tournament_status_data = cursor.fetchone()
                    tournament_status = tournament_status_data[0] if tournament_status_data else None

                    if tournament_status == 'active':
                        cursor.execute('SELECT is_eliminated, is_bye FROM tournament_players WHERE user_id = ? AND tournament_id = ?', (user_id, player_tournament_id))
                        tp_data = cursor.fetchone()
                        if tp_data:
                            is_eliminated, is_bye = tp_data
                            if is_eliminated:
                                await update.message.reply_text("Вы выбыли из турнира\\. Используйте /start для начала новой игры\\.", parse_mode=ParseMode.MARKDOWN_V2)
                            elif is_bye:
                                await update.message.reply_text(f"Вы получили свободный проход в этом раунде\\. Пожалуйста, придумайте и введите новый {CODE_LENGTH}\\-значный секретный код для следующего раунда\\.", parse_mode=ParseMode.MARKDOWN_V2)
                            else:
                                await update.message.reply_text("Вы ожидаете начала следующего матча в турнире\\. Пожалуйста, дождитесь, пока бот найдет вам оппонента\\.", parse_mode=ParseMode.MARKDOWN_V2)
                        else:
                            await update.message.reply_text("Вы ожидаете начала турнира\\. Пожалуйста, дождитесь, пока администратор запустит его или используйте /ready для регистрации\\.", parse_mode=ParseMode.MARKDOWN_V2)
                    elif tournament_status == 'registration':
                        await update.message.reply_text("Вы зарегистрированы на турнир и ожидаете его начала\\. Пожалуйста, дождитесь, пока наберется достаточно игроков\\.", parse_mode=ParseMode.MARKDOWN_V2)
                    else:
                        await update.message.reply_text("Вы ожидаете начала турнира\\. Пожалуйста, дождитесь, пока администратор запустит его или используйте /ready для регистрации\\.", parse_mode=ParseMode.MARKDOWN_V2)
                else:
                    await update.message.reply_text("Вы ожидаете начала турнира\\. Пожалуйста, дождитесь, пока администратор запустит его или используйте /ready для регистрации\\.", parse_mode=ParseMode.MARKDOWN_V2)
            else:
                await update.message.reply_text("Извините, я не понял вашего сообщения\\. Возможно, вы находитесь не в том состоянии\\. Используйте /start для начала игры\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в handle_text_message: {e}")
        await update.message.reply_text("Произошла ошибка при обработке вашего запроса\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в handle_text_message: {e}\n{traceback.format_exc()}") # Добавлено логирование трейсбэка
        await update.message.reply_text("Произошла непредвиденная ошибка при обработке вашего запроса\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- НОВАЯ ФУНКЦИЯ: tournament_bracket (обработчик команды) ---
@update_activity_decorator
async def tournament_bracket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Попытка найти последний активный или завершенный турнир
        cur.execute("SELECT tournament_id, tournament_name FROM tournaments WHERE status IN ('active', 'completed') ORDER BY tournament_id DESC LIMIT 1")
        tournament_data = cur.fetchone()

        if not tournament_data:
            await update.message.reply_text("Активных или завершенных турниров для отображения сетки не найдено\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        tournament_id, tournament_name = tournament_data
        
        await update.message.reply_text(f"Генерирую турнирную сетку для турнира '{escape_markdown(tournament_name, version=2)}' \\(ID\\: {tournament_id}\\)\\. Это может занять несколько секунд\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)

        image_buffer = await generate_tournament_bracket_image(tournament_id, context.bot)

        if image_buffer:
            image_buffer.seek(0) # Перемещаем указатель в начало буфера
            await context.bot.send_photo(chat_id=user_id, photo=InputFile(image_buffer, filename=f'tournament_bracket_{tournament_id}.png'),
                                         caption=f"Турнирная сетка для турнира '{escape_markdown(tournament_name, version=2)}' \\(ID\\: {tournament_id}\\)", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Турнирная сетка для турнира {tournament_id} отправлена пользователю {user_id}.")
        else:
            await update.message.reply_text("Не удалось сгенерировать турнирную сетку\\. Возможно, нет данных для отображения или произошла ошибка\\.", parse_mode=ParseMode.MARKDOWN_V2)

    except sqlite3.Error as e:
        logger.error(f"Ошибка БД в tournament_bracket: {e}")
        await update.message.reply_text("Произошла ошибка при получении данных для турнирной сетки\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Неизвестная ошибка в tournament_bracket: {e}\n{traceback.format_exc()}")
        await update.message.reply_text("Произошла непредвиденная ошибка при генерации турнирной сетки\\. Пожалуйста, попробуйте еще раз\\.", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        if conn:
            conn.close()

# --- НОВЫЕ ФУНКЦИИ ДЛЯ WEB APP ---
async def open_webapp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Открывает Web App с кнопкой и передает game_id (chat_instance)."""
    user_id = update.effective_user.id
    # Используем chat_id сообщения, из которого открывается Web App, как game_id
    # В приватном чате это будет user_id, в группе - group_chat_id
    game_instance_id = update.effective_chat.id
    
    # Формируем URL с параметром game_id
    # Убедитесь, что WEB_APP_URL не содержит Markdown форматирования
    webapp_url_with_param = f"{WEB_APP_URL.strip('[]()')}?game_id={game_instance_id}"

    keyboard = [[InlineKeyboardButton("Открыть Web App", web_app=WebAppInfo(url=webapp_url_with_param))]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Нажмите кнопку ниже, чтобы открыть Web App:", reply_markup=reply_markup)
    logger.info(f"Пользователь {user_id} открыл Web App с game_id={game_instance_id}.")

async def _send_game_state_update_to_webapp(user_id: int, context: ContextTypes.DEFAULT_TYPE, game_id_from_webapp: str):
    """
    Формирует и отправляет текущее состояние игры в Web App пользователя.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Fetch player's current state and tournament info
        cur.execute('SELECT state, tournament_id, secret_code FROM players WHERE user_id = ?', (user_id,))
        player_data = cur.fetchone()
        current_state = player_data[0] if player_data else 'no_game'
        player_tournament_id = player_data[1] if player_data else None
        player_secret_code = player_data[2] if player_data else None

        game_state = {
            'game_id': game_id_from_webapp,
            'status': current_state,
            'your_turn': False,
            'opponent_name': 'N/A',
            'your_progress': '-----',
            'opponent_progress': '-----',
            'log': [], # Initialize empty log for now, can populate later
            'winner_name': 'N/A'
        }

        if current_state == IN_MATCH:
            cur.execute('''
                SELECT match_id, player1_id, player2_id, current_player_turn,
                       player1_cracked_code, player2_cracked_code, winner_id
                FROM matches
                WHERE (player1_id = ? OR player2_id = ?) AND status = 'active'
            ''', (user_id, user_id))
            match_info = cur.fetchone()

            if match_info:
                match_id, p1_id, p2_id, current_turn, cracked_p1_json, cracked_p2_json, winner_id_match = match_info
                opponent_id = p2_id if user_id == p1_id else p1_id

                opponent_chat = await context.bot.get_chat(opponent_id)
                opponent_name = opponent_chat.username or opponent_chat.first_name or f"Игрок {opponent_id}"

                game_state['your_turn'] = (user_id == current_turn)
                game_state['opponent_name'] = opponent_name
                game_state['your_progress'] = " ".join(json.loads(cracked_p1_json) if user_id == p1_id else json.loads(cracked_p2_json))
                game_state['opponent_progress'] = " ".join(json.loads(cracked_p2_json) if user_id == p1_id else json.loads(cracked_p1_json))
                game_state['status'] = 'in_match' # Explicitly set status for WebApp

                # Для лога, пока нет отдельной таблицы, можно добавить текущий статус
                game_state['log'].append({
                    'type': 'status',
                    'text': f"Ваш прогресс: {game_state['your_progress']}, Прогресс оппонента: {game_state['opponent_progress']}"
                })

            else: # No active match, but player state is IN_MATCH (could be a stale state)
                game_state['status'] = 'no_game' # Or a more specific error state

        elif current_state == WAITING_FOR_TOURNAMENT:
            game_state['status'] = 'waiting_for_tournament'
        elif current_state == SET_CODE or current_state == SET_NEW_ROUND_CODE:
            game_state['status'] = 'waiting_for_code'
        elif current_state == START:
            game_state['status'] = 'start'

        # If a winner is determined (e.g., after end_match)
        if current_state == START and player_tournament_id: # Check if player was in a completed tournament
            cur.execute('SELECT winner_id FROM tournaments WHERE tournament_id = ? AND status = \'completed\'', (player_tournament_id,))
            completed_tour_winner_data = cur.fetchone()
            if completed_tour_winner_data and completed_tour_winner_data[0] == user_id:
                game_state['status'] = 'game_over'
                winner_chat = await context.bot.get_chat(user_id)
                game_state['winner_name'] = winner_chat.username or winner_chat.first_name or f"Игрок {user_id}"
                game_state['log'].append({'type': 'message', 'text': 'Вы победили в турнире!'})
            elif completed_tour_winner_data: # Tournament completed, but user is not winner
                game_state['status'] = 'game_over'
                game_state['log'].append({'type': 'message', 'text': 'Турнир завершен.'})

        # Send the game state to the Web App
        await context.bot.send_message(
            chat_id=user_id,
            text=json.dumps({'action': 'game_state_update', 'game_state': game_state}),
            parse_mode=ParseMode.HTML # Или PLAIN_TEXT, если хотите, чтобы Web App парсил
        )
        logger.info(f"Sent game state update to Web App for user {user_id}: {game_state['status']}")

    except sqlite3.Error as e:
        logger.error(f"DB Error in _send_game_state_update_to_webapp for user {user_id}: {e}")
        await context.bot.send_message(chat_id=user_id, text=json.dumps({'action': 'message', 'text': 'Ошибка базы данных при обновлении Web App.'}), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Unexpected error in _send_game_state_update_to_webapp for user {user_id}: {e}\n{traceback.format_exc()}")
        await context.bot.send_message(chat_id=user_id, text=json.dumps({'action': 'message', 'text': 'Непредвиденная ошибка при обновлении Web App.'}), parse_mode=ParseMode.HTML)
    finally:
        if conn:
            conn.close()

async def handle_web_app_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает данные, полученные из Web App."""
    if not update.message or not update.message.web_app_data:
        logger.error(f"handle_web_app_data вызван без update.message или update.message.web_app_data. Update: {update}")
        return

    user_id = update.effective_user.id
    data_str = update.message.web_app_data.data
    logger.info(f"Получены данные из Web App от {user_id}: {data_str}")

    try:
        data = json.loads(data_str)
        action = data.get('action')
        game_id_from_webapp = data.get('game_id') # Получаем game_id из Web App

        # Создаем фиктивное обновление для передачи в существующие обработчики
        # Это необходимо, потому что оригинальные функции ожидают объект Update
        # с определенной структурой (например, update.message.text).
        # Для WebApp данных, update.message.text не существует, поэтому
        # мы имитируем его, если это необходимо для вызова.
        mock_update = Update(update_id=update.update_id,
                             message=update.message) # Передаем оригинальное сообщение WebApp

        if action == 'get_game_status':
            await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)

        elif action == 'make_guess':
            guess_string = data.get('guess_string')
            if guess_string:
                # Разбираем строку на цифру и позицию
                parts = guess_string.split()
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    context.args = parts # Устанавливаем context.args для функции guess
                    await guess(mock_update, context) # Вызываем вашу функцию guess
                    await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)
                else:
                    await context.bot.send_message(chat_id=user_id, text=json.dumps({'action': 'message', 'text': 'Неверный формат догадки. Используйте "цифра позиция".'}), parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id=user_id, text=json.dumps({'action': 'message', 'text': 'Отсутствует строка догадки.'}), parse_mode=ParseMode.HTML)


        elif action == 'set_code':
            code = data.get('code')
            if code:
                # Для set_code, нам нужно имитировать update.message.text
                mock_update.message.text = code
                context.args = [code] # Устанавливаем context.args для set_code
                await set_code(mock_update, context, direct_code=code)
                await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)
            else:
                await context.bot.send_message(chat_id=user_id, text=json.dumps({'action': 'message', 'text': 'Отсутствует код для установки.'}), parse_mode=ParseMode.HTML)

        elif action == 'exit_game':
            await exit_game(mock_update, context)
            await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)

        elif action == 'ready':
            await ready(mock_update, context)
            await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)

        elif action == 'start_game':
            await start(mock_update, context)
            await _send_game_state_update_to_webapp(user_id, context, game_id_from_webapp)

        else:
            await context.bot.send_message(
                chat_id=user_id,
                text=json.dumps({'action': 'message', 'text': f'Неизвестное действие: {action}'}),
                parse_mode=ParseMode.HTML
            )

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from Web App: {data_str}")
        await context.bot.send_message(
            chat_id=user_id,
            text=json.dumps({'action': 'message', 'text': 'Ошибка: Неверный формат данных от Web App.'}),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error processing Web App data for user {user_id}: {e}\n{traceback.format_exc()}")
        await context.bot.send_message(
            chat_id=user_id,
            text=json.dumps({'action': 'message', 'text': 'Произошла ошибка при обработке вашего запроса.'}),
            parse_mode=ParseMode.HTML
        )

# --- ГЛАВНАЯ ФУНКЦИЯ ---

async def configure_bot_commands(application: Application):
    """Настраивает команды бота в Telegram."""
    await application.bot.set_my_commands([
        BotCommand("start", "Начать игру 'Crack the Code'"),
        BotCommand("rules", "Правила игры"),
        BotCommand("set_code", "Установить свой секретный код (5 цифр)"),
        BotCommand("ready", "Зарегистрироваться на турнир"),
        BotCommand("guess", "Сделать предположение (в матче)"),
        BotCommand("tournament_info", "Информация о текущем турнире"),
        BotCommand("tournament_bracket", "Показать турнирную сетку"),
        BotCommand("exit", "Выйти из турнира"),
        BotCommand("yes", "Подтвердить выход из турнира"),
        BotCommand("no", "Отменить выход из турнира"),
        BotCommand("admin", "Панель администратора (только для админа)"),
        BotCommand("open_webapp", "Открыть интерфейс Web App для игры"), # НОВАЯ КОМАНДА
    ])
    logger.info("Команды бота настроены.")

def main():
    logger.info("Бот запущен!")
    init_db()

    application = (ApplicationBuilder()
                   .token(BOT_TOKEN)
                   .post_init(configure_bot_commands)
                   .build())
    job_queue: JobQueue = application.job_queue

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("rules", rules))
    application.add_handler(CommandHandler("set_code", set_code))
    application.add_handler(CommandHandler("ready", ready))
    application.add_handler(CommandHandler("guess", guess))
    application.add_handler(CommandHandler("exit", exit_game))
    application.add_handler(CommandHandler("yes", confirm_exit_yes))
    application.add_handler(CommandHandler("no", confirm_exit_no))
    application.add_handler(CommandHandler("admin", admin_menu))
    application.add_handler(CommandHandler("tournament_info", tournament_info))
    application.add_handler(CommandHandler("tournament_bracket", tournament_bracket))
    application.add_handler(CommandHandler("admin_stats", admin_stats))

    # --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ WEB APP ---
    application.add_handler(CommandHandler("open_webapp", open_webapp_command)) # Команда для открытия Web App
    # Использование кастомного фильтра
    application.add_handler(MessageHandler(web_app_data_filter_instance, handle_web_app_data)) 
    # --- КОНЕЦ НОВЫХ ОБРАБОТЧИКОВ ---

    # Обработчики для инлайн-кнопок админ-панели
    application.add_handler(CallbackQueryHandler(start_new_tournament, pattern="admin_start_new_tournament_callback"))
    application.add_handler(CallbackQueryHandler(admin_cancelmatch_prompt, pattern="admin_cancel_match_callback"))
    application.add_handler(CallbackQueryHandler(admin_end_tournament_prompt, pattern="admin_end_tournament_callback"))
    application.add_handler(CallbackQueryHandler(admin_stats, pattern="admin_stats_callback"))
    application.add_handler(CallbackQueryHandler(admin_reset_game, pattern="admin_reset_game_callback"))
    application.add_handler(CallbackQueryHandler(admin_broadcast_prompt, pattern="admin_broadcast_callback"))
    application.add_handler(CallbackQueryHandler(force_registration_status_report, pattern="admin_force_registration_status"))
    application.add_handler(CallbackQueryHandler(force_active_matches_report, pattern="admin_force_active_matches"))

    # Обработчики для инлайн-кнопок из статистики (остаются)
    application.add_handler(CallbackQueryHandler(show_all_users, pattern=SHOW_ALL_USERS_CALLBACK))
    application.add_handler(CallbackQueryHandler(show_all_match_ids, pattern=SHOW_ALL_MATCH_IDS_CALLBACK))

    # --- ИЗМЕНЕННЫЕ ОБРАБОТЧИКИ ТЕКСТОВЫХ СООБЩЕНИЙ ДЛЯ ОБХОДА ОШИБКИ С '&' ---
    application.add_handler(MessageHandler(filter_text_no_command_and_5_digits, handle_set_code_direct_wrapper))
    application.add_handler(MessageHandler(filter_text_no_command_and_digit_space_digit, handle_guess_direct_wrapper))
    application.add_handler(MessageHandler(filter_text_no_command, admin_text_input_handler))
    # --- КОНЕЦ ИЗМЕНЕННЫХ ОБРАБОТЧИКОВ ---
    
    job_queue.run_repeating(check_game_status_job, interval=CHECK_INTERVAL_SECONDS, first=5)
    logger.info("Scheduler started")
    logger.info("Application started")

    application.run_polling()

if __name__ == "__main__":
    try:
        with open('config.txt', 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:
                raise ValueError("Файл config.txt должен содержать токен бота на первой строке и ID админа на второй.")
            BOT_TOKEN = lines[0].strip()
            # Устанавливаем глобальную переменную ADMIN_USER_ID
            ADMIN_USER_ID = int(lines[1].strip())
    except FileNotFoundError:
        logger.error("Файл 'config.txt' не найден. Пожалуйста, создайте его и добавьте токен бота и ID админа.")
        exit()
    except ValueError as e:
        logger.error(f"Ошибка чтения config.txt: {e}. Убедитесь, что ID админа является числом.")
        exit()
    except Exception as e:
        logger.error(f"Произошла ошибка при загрузке конфигурации: {e}")
        exit()
    
    main()
import os, logging, datetime as dt, calendar, json
from collections import defaultdict
from dotenv import load_dotenv
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.error import TelegramError
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update, InputFile
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler,ConversationHandler, ContextTypes, filters
)
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
import io
import math
import numpy as np
from matplotlib.ticker import MaxNLocator

# --- КОНФИГ ---
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATE_FMT = "%d.%m.%Y"
ADMINS = {"5144039813", "476179186"}  # ID администраторов
SELLERS = ["Сергей", "Наталия", "Людмила", "Мария"]
ADMIN_CHAT_IDS = [5144039813, 476179186]
SHEET_REPORT = "Дневные отчёты"
SHEET_SUPPLIERS = "Поставщики"
SHEET_EXPENSES = "Расходы"
SHEET_LOG = "Логи"
SHEET_SHIFTS = "Смены"
SHEET_DEBTS = "Долги"
SHEET_SALARIES = "Зарплаты"
SHEET_PLAN_FACT = "ПланФактНаЗавтра" 
SHEET_PLANNING_SCHEDULE = "ПланированиеПоставщиков"
SHEET_INVENTORY = "Остаток магазина"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
def push_nav(context, target):
    stack = context.user_data.get('nav_stack', [])
    stack.append(target)
    context.user_data['nav_stack'] = stack

def pop_nav(context):
    stack = context.user_data.get('nav_stack', [])
    if stack:
        stack.pop()
    context.user_data['nav_stack'] = stack
    return stack[-1] if stack else "main_menu"

def now(): return dt.datetime.now().strftime("%d.%m.%Y %H:%M")
def sdate(d=None): 
    d = d or dt.date.today()
    return d.strftime(DATE_FMT)

def pdate(s):
    """Парсит дату из строк в формате ДД.ММ.ГГГГ, не вызывая ошибок на других строках."""
    if not isinstance(s, str):
        return None
    try:
        return dt.datetime.strptime(s, DATE_FMT).date()
    except ValueError:
        return None # Просто возвращаем None, если формат не совпал
    
def week_range(date=None):
    date = date or dt.date.today()
    start = date - dt.timedelta(days=date.weekday())
    end = start + dt.timedelta(days=6)
    return start, end
    
def delete_plan_by_row_index(row_index: int) -> bool:
    """Находит и удаляет строку в листе ПланФактНаЗавтра по ее номеру."""
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        ws.delete_rows(row_index)
        logging.info(f"Запись о плане в строке {row_index} успешно удалена.")
        return True
    except Exception as e:
        logging.error(f"Ошибка удаления записи о плане в строке {row_index}: {e}")
        return False

def month_range(date=None):
    date = date or dt.date.today()
    start = dt.date(date.year, date.month, 1)
    end = dt.date(date.year, date.month + 1, 1) - dt.timedelta(days=1)
    return start, end

def parse_float(value):
    """Преобразует строку с запятой в десятичном числе в float."""
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value.replace(',', '.').strip())
    except (ValueError, TypeError):
        return 0.0

# --- И ЭТУ ФУНКЦИЮ ТОЖЕ ЗАМЕНИТЕ ---
def get_planning_details_for_date(context: ContextTypes.DEFAULT_TYPE, report_date: dt.date):
    """Собирает данные из ПланФакт для отчета на ЗАДАННУЮ ДАТУ, используя кэш."""
    report_date_str = sdate(report_date)
    
    rows = get_cached_sheet_data(context, SHEET_PLAN_FACT)
    if rows is None:
        logging.error("Не удалось получить данные из листа ПланФактНаЗавтра")
        return "", 0, 0, 0

    details, total_cash, total_card = [], 0, 0
    for i, row in enumerate(rows):
        if not (row and len(row) > 3 and row[0]):
            continue

        if row[0].strip() == report_date_str:
            try:
                supplier = row[1]
                amount_str = row[2]
                pay_type = row[3]
                amount = float(amount_str.strip().replace(',', '.'))
                
                details.append(f"- {supplier}: {amount:.2f}₴ ({pay_type})")
                if 'налич' in pay_type.lower():
                    total_cash += amount
                elif 'карт' in pay_type.lower():
                    total_card += amount
            except (ValueError, IndexError) as e:
                logging.error(f"!!! Не удалось обработать строку #{i+2} в листе '{SHEET_PLAN_FACT}': {row}. Ошибка: {e}")
                continue
    
    if not details:
        return "", 0, 0, 0

    report_text = "\n\n<b>📋 План оплат на " + report_date_str + ":</b>\n" + "\n".join(details)
    total_amount = total_cash + total_card
    return report_text, total_cash, total_card, total_amount
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
def get_debts_for_date(context: ContextTypes.DEFAULT_TYPE, report_date: dt.date):
    """Собирает данные о долгах на заданную дату, используя кэш."""
    rows = get_cached_sheet_data(context, SHEET_DEBTS)
    if rows is None:
        logging.error("Не удалось получить данные из листа Долги для get_debts_for_date")
        return 0, []

    report_date_str = sdate(report_date)
    total = 0
    suppliers = []
    for row in rows:
        # Проверяем, что долг не погашен (столбец G, индекс 6) и срок совпадает
        if len(row) > 6 and row[6].strip().lower() != "да" and row[5].strip() == report_date_str:
            try:
                amount = parse_float(row[4])  # Остаток
                total += amount
                suppliers.append((row[1], amount))
            except (ValueError, IndexError):
                continue
    return total, suppliers
def clear_plan_for_date(date_to_clear_str: str):
    """Очищает записи в листе ПланФактНаЗавтра для указанной даты."""
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        all_rows = ws.get_all_values()
        
        rows_to_delete_indices = []
        for i, row in enumerate(all_rows[1:], start=2):
            if row and row[0] == date_to_clear_str:
                rows_to_delete_indices.append(i)
        
        if rows_to_delete_indices:
            for index in sorted(rows_to_delete_indices, reverse=True):
                ws.delete_rows(index)
            logging.info(f"Очищены планы для даты: {date_to_clear_str}")

    except Exception as e:
        logging.error(f"Ошибка очистки планов для даты {date_to_clear_str}: {e}")


def get_cached_sheet_data(context: ContextTypes.DEFAULT_TYPE, sheet_name: str, cache_duration_seconds: int = 60, force_update: bool = False) -> list | None:
    """Получает данные из листа, используя кэш, с возможностью принудительного обновления."""
    if not GSHEET: return None
    
    now = dt.datetime.now()
    cache = context.bot_data.setdefault('sheets_cache', {})
    
    # Если не требуется принудительное обновление, проверяем кэш
    if not force_update and sheet_name in cache:
        cached_data, timestamp = cache[sheet_name]
        if (now - timestamp).total_seconds() < cache_duration_seconds:
            logging.info(f"Данные для '{sheet_name}' взяты из кэша.")
            return list(cached_data)
            
    # Читаем из таблицы, если кэш устарел, его нет или требуется обновление
    try:
        logging.info(f"Читаем данные для '{sheet_name}' из Google Sheets (обновляем кэш).")
        ws = GSHEET.worksheet(sheet_name)
        data = ws.get_all_values()[1:]
        
        cache[sheet_name] = (data, now)
        context.bot_data['sheets_cache'] = cache
        
        return list(data)
    except Exception as e:
        logging.error(f"Не удалось прочитать или кэшировать лист '{sheet_name}': {e}")
        return None
    
# --- GOOGLE SHEETS ---
def get_gsheet():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        # Проверяем, запущена ли программа на Railway (через переменную окружения)
        if 'GOOGLE_CREDENTIALS_JSON' in os.environ:
            # Читаем учетные данные из переменной окружения
            creds_json_str = os.environ.get('GOOGLE_CREDENTIALS_JSON')
            creds_dict = json.loads(creds_json_str)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            # Иначе используем локальный файл (для тестов на вашем компьютере)
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)

        gc = gspread.authorize(creds)
        sh = gc.open("Магазин: Финансы")
        logging.info("Таблица Google Sheets успешно открыта")
        
        required_sheets = {
            SHEET_REPORT: ['Дата', 'Продавец', 'Наличные', 'Терминал', 'Общая сумма', 'Остаток наличных', 'На завтра (долги)', 'На завтра (план)', 'Комментарий', 'Остаток в сейфе'],
            SHEET_SALARIES: ['Дата', 'Продавец', 'Тип', 'Сумма', 'Комментарий'],
            SHEET_SUPPLIERS: ['Дата', 'Поставщик', 'Сумма прихода', 'Возврат/списание', 'К оплате', 'Сумма после наценки', 'Тип оплаты', 'Оплачено', 'Долг', 'Срок долга'],
            SHEET_EXPENSES: ['Дата', 'Сумма', 'Категория/Комментарий', 'Продавец'],
            SHEET_LOG: ['Время', 'Telegram', 'Имя', 'Действие', 'Комментарий'],
            SHEET_SHIFTS: ['Дата', 'Продавец 1', 'Продавец 2'],
            SHEET_DEBTS: ['Дата', 'Поставщик', 'Сумма', 'Оплачено', 'Остаток', 'Срок погашения', 'Погашено', 'Тип оплаты'],
            SHEET_PLANNING_SCHEDULE: ["День недели", "Поставщик"],
            SHEET_PLAN_FACT: ["Дата", "Поставщик", "Сумма", "Тип оплаты", "Кто заполнил", "Статус"],
            "Переучеты": ["Дата", "Расчетная сумма", "Фактическая сумма", "Разница", "Комментарий", "Кто внёс"],
        }
        
        existing_titles = [ws.title for ws in sh.worksheets()]
        for sheet_name, headers in required_sheets.items():
            if sheet_name not in existing_titles:
                try:
                    ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))
                    ws.append_row(headers)
                    logging.info(f"Создан лист: {sheet_name}")
                except gspread.exceptions.APIError as e:
                    if "already exists" in str(e):
                        logging.warning(f"Лист '{sheet_name}' уже существует.")
                    else: raise e
        return sh
        
    except Exception as e:
        logging.critical(f"Критическая ошибка подключения к Google Таблицам: {e}")
        return None
    
    except Exception as e:
        logging.critical(f"Критическая ошибка подключения к Google Таблицам: {e}")
        class DummyWorksheet:
            def append_row(self, row): 
                logging.warning(f"Заглушка: append_row({row})")
            def get_all_values(self): 
                logging.warning("Заглушка: get_all_values()")
                return []
            def update(self, *args, **kwargs): 
                logging.warning(f"Заглушка: update({args}, {kwargs})")
            def find(self, *args, **kwargs):
                return None
            def update_cell(self, *args, **kwargs):
                pass
            def row_values(self, row):
                return []
            def col_values(self, col):
                return []
        class DummySpreadsheet:
            def worksheet(self, title):
                logging.warning(f"Заглушка: запрошен лист '{title}'")
                return DummyWorksheet()
        return DummySpreadsheet()

GSHEET = get_gsheet()

def log_action(user, name, action, comment=""):
    try:
        ws = GSHEET.worksheet(SHEET_LOG)
        ws.append_row([now(), str(user.id), name, action, comment])
    except Exception as e:
        logging.error(f"Ошибка логирования: {e}")

def get_suppliers_for_day(day_of_week: str):
    """Получает список всех поставщиков на заданный день недели из таблицы 'длинного' формата."""
    try:
        ws = GSHEET.worksheet("ПланированиеПоставщиков")
        rows = ws.get_all_values()[1:]  # Пропускаем заголовок
        
        suppliers_for_day = []
        
        
        for row in rows:
            # <<< И ЭТО ТОЖЕ ДОБАВЬ >>>

            # row[0] - День недели, row[1] - Поставщик
            if row and row[0].strip().lower() == day_of_week:
                if len(row) > 1 and row[1].strip():
                    suppliers_for_day.append(row[1].strip())
                    
        print(f"--- РЕЗУЛЬТАТ: Найденные поставщики: {suppliers_for_day} ---")
        return suppliers_for_day
        
    except gspread.exceptions.WorksheetNotFound:
        logging.error("Критическая ошибка: лист 'ПланированиеПоставщиков' не найден!")
        return []
    except Exception as e:
        logging.error(f"Ошибка получения поставщиков на день '{day_of_week}': {e}")
        return []

def save_fact(date, supplier, amount, pay_type, sheet):
    ws = sheet.worksheet("ПланФакт")
    ws.append_row([date, supplier, amount, pay_type])
    
def get_unplanned_suppliers(date, all_suppliers, sheet):
    ws = sheet.worksheet("ПланФакт")
    rows = ws.get_all_values()
    planned = [row[1] for row in rows if row[0] == date]
    return [x for x in all_suppliers if x not in planned]

def month_buttons(start_date, end_date):
    # prev/next month с учётом смены года
    prev_month = start_date.month - 1 or 12
    prev_year = start_date.year if start_date.month > 1 else start_date.year - 1
    next_month = start_date.month + 1 if start_date.month < 12 else 1
    next_year = start_date.year if start_date.month < 12 else start_date.year + 1

    prev_start = dt.date(prev_year, prev_month, 1)
    prev_end = dt.date(prev_year, prev_month, calendar.monthrange(prev_year, prev_month)[1])
    next_start = dt.date(next_year, next_month, 1)
    next_end = dt.date(next_year, next_month, calendar.monthrange(next_year, next_month)[1])
    curr_start, curr_end = month_range()  # Текущий месяц

    return [
        [
            InlineKeyboardButton("◀️ Пред. месяц", callback_data=f"report_month_{sdate(prev_start)}_{sdate(prev_end)}"),
            InlineKeyboardButton("Текущий месяц", callback_data=f"report_month_{sdate(curr_start)}_{sdate(curr_end)}"),
            InlineKeyboardButton("След. месяц ▶️", callback_data=f"report_month_{sdate(next_start)}_{sdate(next_end)}"),
        ],
        [InlineKeyboardButton("💸 Детально расходы", callback_data=f"details_exp_{sdate(start_date)}_{sdate(end_date)}")],
        [InlineKeyboardButton("📦 Детально накладные", callback_data=f"details_sup_{sdate(start_date)}_{sdate(end_date)}")],
        [InlineKeyboardButton("📖 Просмотр детальных отчетов", callback_data=f"detail_report_nav_{sdate(start_date)}_{sdate(end_date)}_0")],
        [InlineKeyboardButton("🔙 К отчетам", callback_data="view_reports_menu")]
    ]

# <<< НАЧАЛО: НОВЫЙ КОД ДЛЯ ДОБАВЛЕНИЯ >>>

# --- ФУНКЦИИ ДЛЯ ПЛАНИРОВАНИЯ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
def get_planned_suppliers(date_str: str):
    """
    Получает поставщиков, которые уже были спланированы на заданную дату, 
    вместе с деталями плана и номерами их строк.
    """
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        rows = ws.get_all_values()[1:]
        planned_suppliers_data = []
        for i, row in enumerate(rows, start=2):
            if row and len(row) >= 4 and row[0] == date_str:
                planned_suppliers_data.append({
                    "supplier": row[1].strip(),
                    "amount": row[2],
                    "pay_type": row[3],
                    "row_index": i
                })
        return planned_suppliers_data
    except Exception as e:
        logging.error(f"Ошибка получения спланированных поставщиков на '{date_str}': {e}")
        return []
        
def save_plan_fact(date_str, supplier, amount, pay_type, user_name):
    """Сохраняет одну запись о плане на завтра со статусом 'Ожидается'."""
    if not GSHEET: return
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        ws.append_row([date_str, supplier, amount, pay_type, user_name, "Ожидается"])
        logging.info(f"План на {date_str} для '{supplier}' сохранен.")
    except Exception as e:
        logging.error(f"Ошибка сохранения ПланФакт: {e}")
        
def get_tomorrow_planning_details():
    """Собирает данные из ПланФакт для отчета и возвращает форматированную строку."""
    tomorrow_str = (dt.date.today() + dt.timedelta(days=1)).strftime(DATE_FMT)
    try:
        ws = GSHEET.worksheet("ПланФактНаЗавтра")
        rows = ws.get_all_values()[1:]
        
        details = []
        total_cash = 0
        total_card = 0
        
        for row in rows:
            if row and row[0] == tomorrow_str:
                # row -> ['16.06.2025', 'Factor', '1500', 'Наличные', 'Женя']
                supplier = row[1]
                amount = float(row[2].replace(',', '.'))
                pay_type = row[3]
                
                details.append(f"- {supplier}: {amount:.2f}₴ ({pay_type})")
                
                if pay_type.lower() == 'наличные':
                    total_cash += amount
                elif pay_type.lower() == 'карта':
                    total_card += amount
        
        if not details:
            return "", 0, 0 # Если планов нет

        report_text = "\n\n<b>📋 План оплат на завтра:</b>\n" + "\n".join(details)
        
        return report_text, total_cash, total_card

    except Exception as e:
        logging.error(f"Ошибка получения деталей планирования на завтра: {e}")
        return "", 0, 0

def clear_planning_sheet():
    """Очищает лист ПланФактНаЗавтра после сдачи отчета."""
    try:
        ws = GSHEET.worksheet("ПланФактНаЗавтра")
        # Удаляем все строки кроме первой (заголовка)
        ws.delete_rows(2, len(ws.get_all_values()))
        logging.info("Лист 'ПланФактНаЗавтра' очищен.")
    except Exception as e:
        logging.error(f"Ошибка очистки листа ПланФактНаЗавтра: {e}")

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
def update_invoice_in_sheet(row_index: int, field_to_update: str, new_value):
    """Обновляет одно поле в строке накладной в листе Поставщики."""
    try:
        ws = GSHEET.worksheet(SHEET_SUPPLIERS)
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Добавляем все нужные поля ---
        column_map = {
            'amount_income': 3, 'writeoff': 4, 'to_pay': 5, 'markup_amount': 6,
            'pay_type': 7, 'paid_status': 8, 'debt_amount': 9, 'due_date': 10, 
            'comment': 11
        }
        col_to_update = column_map.get(field_to_update)
        if not col_to_update:
            logging.error(f"Попытка обновить неизвестное поле накладной: {field_to_update}")
            return False
        
        ws.update_cell(row_index, col_to_update, str(new_value))
        logging.info(f"Накладная в строке {row_index} обновлена. Поле: {field_to_update}, значение: {new_value}")
        return True
    except Exception as e:
        logging.error(f"Ошибка обновления ячейки накладной ({row_index}, {col_to_update}): {e}")
        return False


def week_buttons(start_date, end_date):
    prev_start = start_date - dt.timedelta(days=7)
    prev_end = end_date - dt.timedelta(days=7)
    next_start = start_date + dt.timedelta(days=7)
    next_end = end_date + dt.timedelta(days=7)
    curr_start, curr_end = week_range()

    return [
        [
            InlineKeyboardButton("◀️ Пред. неделя", callback_data=f"report_week_{sdate(prev_start)}_{sdate(prev_end)}"),
            InlineKeyboardButton("Текущая", callback_data=f"report_week_current"),
            InlineKeyboardButton("След. неделя ▶️", callback_data=f"report_week_{sdate(next_start)}_{sdate(next_end)}"),
        ],
        # --- ВОЗВРАЩАЕМ КНОПКУ СЮДА ---
        [InlineKeyboardButton("📖 Навигация по дням", callback_data=f"detail_report_nav_{sdate(start_date)}_{sdate(end_date)}_0")],
        [
            InlineKeyboardButton("💸 Расходы", callback_data=f"choose_date_exp_{sdate(start_date)}_{sdate(end_date)}"),
            InlineKeyboardButton("📦 Накладные", callback_data=f"choose_date_sup_{sdate(start_date)}_{sdate(end_date)}")
        ],
        [InlineKeyboardButton("🔙 К отчетам", callback_data="view_reports_menu")]
    ]

# --- И ЭТУ ФУНКЦИЮ ТОЖЕ ЗАМЕНИТЕ ---
def month_buttons(start_date, end_date):
    prev_month_date = start_date - dt.timedelta(days=1)
    prev_start, _ = month_range(prev_month_date)
    next_month_date = end_date + dt.timedelta(days=1)
    next_start, _ = month_range(next_month_date)
    curr_start, _ = month_range()

    return [
        [
            InlineKeyboardButton("◀️ Пред. месяц", callback_data=f"report_month_{sdate(prev_start)}_{sdate(month_range(prev_start)[1])}"),
            InlineKeyboardButton("Текущий", callback_data=f"report_month_{sdate(curr_start)}_{sdate(month_range()[1])}"),
            InlineKeyboardButton("След. месяц ▶️", callback_data=f"report_month_{sdate(next_start)}_{sdate(month_range(next_start)[1])}"),
        ],
        # --- И СЮДА ТОЖЕ ---
        [InlineKeyboardButton("📖 Навигация по дням", callback_data=f"detail_report_nav_{sdate(start_date)}_{sdate(end_date)}_0")],
        [
            InlineKeyboardButton("💸 Расходы", callback_data=f"choose_date_exp_{sdate(start_date)}_{sdate(end_date)}"),
            InlineKeyboardButton("📦 Накладные", callback_data=f"choose_date_sup_{sdate(start_date)}_{sdate(end_date)}")
        ],
        [InlineKeyboardButton("🔙 К отчетам", callback_data="view_reports_menu")]
    ]
def add_inventory_operation(op_type, amount, comment, user):
    ws = GSHEET.worksheet("Остаток магазина")
    ws.append_row([sdate(), op_type, amount, comment, user])

# --- И ЭТУ ФУНКЦИЮ ТОЖЕ ЗАМЕНИТЕ ---
def get_inventory_balance():
    ws = GSHEET.worksheet("Остаток магазина")
    rows = ws.get_all_values()[1:]
    balance = 0
    for row in rows:
        try:
            op_type = row[1]
            # Сумма операции (может быть пустой для переучета)
            amount = float(row[2].replace(',', '.')) if len(row) > 2 and row[2] else 0

            if op_type == "Старт":
                balance = amount
            elif op_type == "Приход":
                balance += amount
            elif op_type == "Корректировка":
                balance += amount
            elif op_type in ["Продажа", "Списание"]:
                balance -= amount
            # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Новая логика для Переучета ---
            elif op_type == "Переучет":
                # Устанавливаем баланс равным сумме, указанной в строке переучета
                balance = amount
        except (ValueError, IndexError):
            continue
    return balance

def get_debts_page(debts, page=0, page_size=10):
    total = len(debts)
    start = page * page_size
    end = start + page_size
    page_debts = debts[start:end]
    return page_debts, total

def debts_message_and_keyboard(debts, page, page_size):
    page_debts, total = get_debts_page(debts, page, page_size)
    msg = ""
    for idx, debt in enumerate(page_debts, start=1 + page * page_size):
        msg += f"{idx}. {debt['name']} — {debt['amount']}₴ — {debt['date']}\n"
    i
    # Кнопки
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"debts_page_{page-1}"))
    if (page + 1) * page_size < total:
        buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"debts_page_{page+1}"))
    
    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None
    
    return msg or "Записей пока нет.", keyboard

def add_salary_record(seller, salary_type, amount, comment):
    ws = GSHEET.worksheet(SHEET_SALARIES)
    ws.append_row([sdate(), seller, salary_type, amount, comment])

def build_debts_history_keyboard(rows, page=0, per_page=10):
    # rows — это все строки из таблицы долгов (без заголовка)
    paged_rows = rows[::-1][page*per_page:(page+1)*per_page]  # последние 10, новые сверху
    kb = []
    text = "<b>📜 История долгов (стр. {}/{}):</b>\n\n".format(page+1, (len(rows)+per_page-1)//per_page)
    for i, row in enumerate(paged_rows, 1):
        num = (page*per_page)+i
        status = "✅" if row[6].strip().lower() == "да" else "🟠"
        text += (
            f"<b>#{num} {status} {row[1]}</b>\n"
            f"   • Дата: {row[0]}\n"
            f"   • Сумма: <b>{float(row[2]):.2f}₴</b>\n"
            f"   • Срок: {row[5]}\n"
            f"   • Оплачен?: {row[6]}\n"
            "─────────────\n"
        )
    # Кнопки "Подробнее #1", "Подробнее #2", ...
    # Навигация (стрелки)
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"debts_history_{page-1}"))
    if (page+1)*per_page < len(rows):
        nav_row.append(InlineKeyboardButton("➡️ Ещё", callback_data=f"debts_history_{page+1}"))
    if nav_row:
        kb.append(nav_row)
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")])
    return text, InlineKeyboardMarkup(kb)


# --- ОСТАТОК МАГАЗИНА, ПЕРЕУЧЕТЫ И СЕЙФ ---
def add_safe_operation(op_type, amount, comment, user):
    ws = GSHEET.worksheet("Сейф")
    ws.append_row([sdate(), op_type, amount, comment, user])

# --- ДОБАВЬТЕ ЭТОТ БЛОК НОВЫХ ФУНКЦИЙ ---

def get_sellers_comparison_data(context: ContextTypes.DEFAULT_TYPE, sellers_list: list, days_period: int = 30):
    """Собирает данные для сравнения средних продаж продавцов по дням недели."""
    today = dt.date.today()
    start_date = today - dt.timedelta(days=days_period)
    
    reports = get_cached_sheet_data(context, SHEET_REPORT)
    if not reports:
        return None

    # Структура: { 'Людмила': {'понедельник': [50283, 48000], 'вторник': [41000]}, 'Мария': {...} }
    sales_data = {seller: defaultdict(list) for seller in sellers_list}

    for row in reports:
        try:
            report_date = pdate(row[0])
            report_seller = row[1]
            total_sales = float(row[4].replace(',', '.'))
            
            if report_seller in sellers_list and start_date <= report_date <= today:
                dow_name = DAYS_OF_WEEK_RU[report_date.weekday()]
                sales_data[report_seller][dow_name].append(total_sales)
        except (ValueError, IndexError, TypeError):
            continue
    
    # Считаем среднее
    avg_stats = {seller: {} for seller in sellers_list}
    for seller, dow_sales in sales_data.items():
        for day_name, sales_list in dow_sales.items():
            avg_stats[seller][day_name] = sum(sales_list) / len(sales_list) if sales_list else 0
            
    return avg_stats

def generate_comparison_chart(stats_data: dict) -> io.BytesIO:
    """Генерирует сгруппированный график для сравнения продавцов."""
    sellers = list(stats_data.keys())
    days = DAYS_OF_WEEK_RU
    
    x = np.arange(len(days))  # the label locations
    width = 0.35  # the width of the bars
    multiplier = 0

    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(12, 7), layout='constrained')

    for seller, dow_avg_sales in stats_data.items():
        sales = [dow_avg_sales.get(day, 0) for day in days]
        offset = width * multiplier
        rects = ax.bar(x + offset, sales, width, label=seller, alpha=0.8)
        ax.bar_label(rects, padding=3, fmt='%.0f')
        multiplier += 1

    ax.set_ylabel('Средняя сумма продаж, ₴')
    ax.set_title('Сравнение средних продаж по дням недели')
    ax.set_xticks(x + width / (len(sellers) / 2) - width/2 , days) # Центрируем подписи
    ax.legend(loc='upper left', ncols=len(sellers))
    ax.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def get_safe_balance(context: ContextTypes.DEFAULT_TYPE):
    """Считает баланс сейфа, используя кэшированные данные."""
    rows = get_cached_sheet_data(context, "Сейф")
    if rows is None:
        logging.error("Не удалось получить данные для расчета баланса сейфа.")
        return 0

    balance = 0
    for row in rows:
        try:
            op_type = row[1]
            amount = float(row[2].replace(',', '.')) if row[2] else 0
            if op_type == "Пополнение":
                balance += amount
            elif op_type in ["Снятие", "Зарплата", "Расход"]:
                balance -= amount
        except (ValueError, IndexError):
            continue
    return balance

def build_edit_invoice_keyboard(invoice_data: list, selected_fields: dict, row_index: int):
    """Строит клавиатуру для режима редактирования накладной."""
    fields = {
        'amount_income': "Сумма прихода", 'writeoff': "Возврат/списание",
        'markup_amount': "Сумма после наценки", 'pay_type': "Тип оплаты",
        'due_date': "Дата долга", 'comment': "Комментарий"
    }
    
    kb = []
    for field_key, field_name in fields.items():
        # Не показывать кнопку "Дата долга", если тип оплаты - не "Долг"
        current_pay_type = selected_fields.get('pay_type', invoice_data[6])
        if field_key == 'due_date' and current_pay_type != "Долг":
            continue
            
        icon = "✅" if field_key in selected_fields else "❌"
        kb.append([InlineKeyboardButton(f"{icon} {field_name}", callback_data=f"edit_invoice_toggle_{row_index}_{field_key}")])

    kb.append([
        InlineKeyboardButton("💾 Сохранить изменения", callback_data=f"edit_invoice_save_{row_index}"),
        InlineKeyboardButton("🚫 Отмена", callback_data=f"edit_invoice_cancel_{row_index}")
    ])
    return InlineKeyboardMarkup(kb)


def update_plan_in_sheet(row_num: int, field: str, new_value) -> bool:
    """Простая функция для обновления одной ячейки в ПланФакт. Возвращает True/False."""
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        col_map = {'amount': 3, 'pay_type': 4}
        col_num = col_map.get(field)
        if not col_num:
            logging.error(f"Попытка обновить неизвестное поле: {field}")
            return False
        ws.update_cell(row_num, col_num, str(new_value))
        logging.info(f"План в строке {row_num} обновлен. Поле: {field}, новое значение: {new_value}")
        return True
    except Exception as e:
        logging.error(f"Ошибка обновления ячейки ({row_num}, {col_num}): {e}")
        return False

# --- ДОБАВЬТЕ ЭТУ НОВУЮ ФУНКЦИЮ ---
def get_todays_actual_invoices():
    """Эффективно получает словарь с фактическими данными накладных за сегодня."""
    if not GSHEET: return {}
    try:
        today_str = sdate()
        ws = GSHEET.worksheet(SHEET_SUPPLIERS)
        rows = ws.get_all_values()[1:]
        
        actual_data = {}
        for row in rows:
            # [Дата, Поставщик, ..., К оплате, ..., Тип оплаты]
            if len(row) > 6 and row[0].strip() == today_str:
                supplier_name = row[1].strip()
                actual_amount = row[4]
                actual_pay_type = row[6]
                actual_data[supplier_name] = {'amount': actual_amount, 'pay_type': actual_pay_type}
        return actual_data
    except Exception as e:
        logging.error(f"Ошибка получения фактических накладных за сегодня: {e}")
        return {}
#Управление зарплатами
# <<< НАЧАЛО БЛОКА ДЛЯ ВСТАВКИ: УПРАВЛЕНИЕ ЗАРПЛАТАМИ >>>

def get_current_payroll_period():
    """Определяет начальную и конечную дату текущего зарплатного периода."""
    today = dt.date.today()
    # Зарплата выплачивается 24-го числа
    if today.day <= 24:
        # Период с 25-го числа прошлого месяца по 24-е текущего
        end_date = dt.date(today.year, today.month, 24)
        start_date = end_date - dt.timedelta(days=end_date.day-1) # 1-е число
        start_date = dt.date(start_date.year, start_date.month -1 if start_date.month > 1 else 12, 25)

    else: # today.day > 24
        # Период с 25-го текущего месяца по 24-е следующего
        start_date = dt.date(today.year, today.month, 25)
        try:
            end_date = dt.date(today.year, today.month + 1, 24)
        except ValueError: # Переход через год
            end_date = dt.date(today.year + 1, 1, 24)
            
    return start_date, end_date

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
def calculate_accrued_bonus(seller_name: str, all_reports=None, all_salaries=None):
    """
    Считает остаток бонуса к выплате по формуле: (Все начисления) - (Все выплаты).
    """
    start_period, end_period = get_current_payroll_period()
    
    # 1. Если данные не переданы, читаем их из таблицы
    if all_reports is None:
        try:
            ws_reports = GSHEET.worksheet(SHEET_REPORT)
            all_reports = ws_reports.get_all_values()[1:]
        except Exception as e:
            logging.error(f"Не удалось прочитать лист отчетов для расчета бонуса: {e}")
            all_reports = []
    
    if all_salaries is None:
        try:
            ws_salaries = GSHEET.worksheet(SHEET_SALARIES)
            all_salaries = ws_salaries.get_all_values()[1:]
        except Exception as e:
            logging.error(f"Не удалось прочитать лист зарплат для расчета бонуса: {e}")
            all_salaries = []

    # 2. Считаем ОБЩУЮ СУММУ всех НАЧИСЛЕННЫХ бонусов за период
    total_accrued = 0
    bonus_days = []
    for row in all_reports:
        try:
            report_date = pdate(row[0])
            report_seller = row[1]
            total_sales = float(row[4].replace(',', '.'))
            
            if start_period <= report_date <= end_period and report_seller == seller_name:
                if bonus > 0:
                    total_accrued += bonus
                    bonus_days.append({'date': sdate(report_date), 'sales': total_sales, 'bonus': bonus})
        except (ValueError, IndexError, TypeError):
            continue
                   
    # 3. Считаем ОБЩУЮ СУММУ всех ВЫПЛАЧЕННЫХ бонусов за этот же период
    total_paid_out = 0
    period_str = f"за период {sdate(start_period)}-{sdate(end_period)}"
    for row in all_salaries:
        try:
            # Ищем запись о выплате бонуса именно за этот период
            if row[1] == seller_name and "Выплата бонуса" in row[2] and period_str in row[4]:
                paid_amount = float(row[3].replace(',', '.'))
                total_paid_out += paid_amount
        except (ValueError, IndexError):
            continue
    
    # 4. Вычисляем остаток к выплате
    bonus_to_pay = total_accrued - total_paid_out
    
    # Округляем до 2 знаков после запятой и не позволяем уйти в минус
    bonus_to_pay = max(0, round(bonus_to_pay, 2))

    return bonus_to_pay, bonus_days
async def staff_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает оптимизированное меню управления персоналом."""
    query = update.callback_query
    await query.answer("Загружаю данные...") # Даем понять пользователю, что идет работа

    sellers_to_check = ["Людмила", "Мария"]
    kb = []
    
    try:
        # ОДИН РАЗ читаем все необходимые данные
        ws_reports = GSHEET.worksheet(SHEET_REPORT)
        all_reports = ws_reports.get_all_values()[1:]
        
        ws_salaries = GSHEET.worksheet(SHEET_SALARIES)
        all_salaries = ws_salaries.get_all_values()[1:]

        for seller in sellers_to_check:
            # Передаем уже загруженные данные в функцию
            bonus, _ = calculate_accrued_bonus(seller, all_reports, all_salaries)
            btn_text = f"{seller} (Бонус к выплате: {bonus:.2f}₴)"
            kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_salary_{seller}")])

    except Exception as e:
        await query.message.edit_text(f"❌ Не удалось загрузить данные для расчета: {e}")
        return

    kb.append([InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")])
    await query.message.edit_text("<b>Управление персоналом</b>\n\nВыберите продавца для просмотра деталей по зарплате:",
                                  parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# --- ДОБАВЬТЕ ВЕСЬ ЭТОТ БЛОК НОВЫХ ФУНКЦИЙ ---

async def edit_invoice_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает сессию редактирования накладной."""
    query = update.callback_query
    await query.answer()
    
    row_index = int(query.data.split('_')[-1])
    
    # Инициализируем состояние редактирования
    context.user_data['edit_invoice'] = {
        'row_index': row_index,
        'selected_fields': {}, # Поля, отмеченные галочкой
        'new_values': {} # Новые значения для этих полей
    }
    
    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    invoice_data = all_invoices[row_index - 2]
    
    kb = build_edit_invoice_keyboard(invoice_data, {}, row_index)
    await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля, которые хотите изменить, и нажмите 'Сохранить'.",
                                  parse_mode=ParseMode.HTML, reply_markup=kb)

async def edit_invoice_toggle_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключает (✅/❌) поле для редактирования."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    row_index = int(parts[3])
    field_key = "_".join(parts[4:])
    
    edit_state = context.user_data.get('edit_invoice', {})
    if edit_state.get('row_index') != row_index: # Проверка, что мы в той же сессии
        return

    # Переключаем поле в словаре
    if field_key in edit_state['selected_fields']:
        del edit_state['selected_fields'][field_key]
    else:
        edit_state['selected_fields'][field_key] = None # Просто помечаем, что оно выбрано

    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    invoice_data = all_invoices[row_index - 2]
    kb = build_edit_invoice_keyboard(invoice_data, edit_state['selected_fields'], row_index)
    await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля, которые хотите изменить, и нажмите 'Сохранить'.",
                                  parse_mode=ParseMode.HTML, reply_markup=kb)


# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def execute_invoice_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполняет сохранение и ВСЕ необходимые пересчеты, включая удаление старых долгов."""
    query = update.callback_query
    await query.answer("Обрабатываю изменения...", show_alert=False)

    edit_state = context.user_data.get('edit_invoice', {})
    row_index = edit_state.get('row_index')
    new_values = edit_state.get('new_values', {})

    if not row_index:
        await query.message.edit_text("❌ Ошибка: сессия редактирования утеряна.")
        return

    # 1. Получаем старые данные из кэша ДО изменений
    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    old_row = all_invoices[row_index - 2]
    old_to_pay = float(old_row[4].replace(',', '.'))
    old_markup = float(old_row[5].replace(',', '.'))
    old_pay_type = old_row[6]
    original_date = old_row[0]
    original_supplier = old_row[1]
    
    # 2. Применяем прямые изменения в таблице "Поставщики"
    for field, new_value in new_values.items():
        update_invoice_in_sheet(row_index, field, new_value)
    
    # 3. Принудительно сбрасываем кэш, чтобы прочитать новые данные
    if 'sheets_cache' in context.bot_data:
        context.bot_data['sheets_cache'].pop(SHEET_SUPPLIERS, None)
    
    all_invoices_new = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    new_row = all_invoices_new[row_index - 2]
    
    # 3.1. Пересчитываем и обновляем "К оплате"
    new_income = float(new_row[2].replace(',', '.'))
    new_writeoff = float(new_row[3].replace(',', '.'))
    new_to_pay = new_income - new_writeoff
    update_invoice_in_sheet(row_index, 'to_pay', f"{new_to_pay:.2f}")

    # 4. Корректируем связанные операции
    who = query.from_user.first_name
    comment_prefix = f"Корректировка по накл. от {original_date} ({original_supplier})"
    new_pay_type = new_row[6]
    
    # 4.1. Корректировка остатка магазина
    new_markup = float(new_row[5].replace(',', '.'))
    markup_diff = new_markup - old_markup
    if abs(markup_diff) > 0.01:
        add_inventory_operation("Корректировка", markup_diff, comment_prefix, who)

    # 4.2. Корректировка сейфа
    old_spent_from_safe = 0 if old_pay_type == "Долг" else old_to_pay
    new_spent_from_safe = 0 if new_pay_type == "Долг" else new_to_pay
    safe_diff = new_spent_from_safe - old_spent_from_safe
    if abs(safe_diff) > 0.01:
        op_type = "Расход" if safe_diff > 0 else "Пополнение"
        add_safe_operation(op_type, abs(safe_diff), comment_prefix, who)
        if 'sheets_cache' in context.bot_data:
            context.bot_data['sheets_cache'].pop("Сейф", None)

    # 5. Обновляем лист "Долги"
    ws_debts = GSHEET.worksheet(SHEET_DEBTS)
    # Принудительно читаем свежие данные, так как могли быть изменения
    debts_rows = get_cached_sheet_data(context, SHEET_DEBTS, force_update=True) 
    found_debt_row_index = -1
    for i, debt_row in enumerate(debts_rows):
        if debt_row[0] == original_date and debt_row[1] == original_supplier:
            found_debt_row_index = i + 2
            break
            
    # Сценарий 1: Теперь это долг (а раньше не был, или был, но сумма изменилась)
    if new_pay_type == "Долг":
        due_date = new_row[9] if len(new_row) > 9 and new_row[9] else ""
        if found_debt_row_index != -1:
            logging.info(f"Обновляем существующий долг в строке {found_debt_row_index}")
            ws_debts.update_cell(found_debt_row_index, 3, new_to_pay)
            current_paid = float(ws_debts.cell(found_debt_row_index, 4).value.replace(',', '.'))
            new_balance = new_to_pay - current_paid
            ws_debts.update_cell(found_debt_row_index, 5, new_balance)
            if 'due_date' in new_values:
                ws_debts.update_cell(found_debt_row_index, 6, new_values['due_date'])
        else:
            logging.info("Создаем новую запись о долге.")
            ws_debts.append_row([original_date, original_supplier, new_to_pay, 0, new_to_pay, due_date, "Нет", "Наличные"])
    
    # Сценарий 2: Это больше не долг (а раньше был)
    elif old_pay_type == "Долг" and new_pay_type != "Долг":
        if found_debt_row_index != -1:
            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Полностью удаляем строку вместо пометки ---
            logging.info(f"Удаляем старую запись о долге из строки {found_debt_row_index}")
            ws_debts.delete_rows(found_debt_row_index)

    # 6. Обновляем главную таблицу "Поставщики" финальными статусами
    if new_pay_type != "Долг":
        update_invoice_in_sheet(row_index, 'due_date', "") # Очищаем срок долга

    final_paid_status = "Да" if new_pay_type != "Долг" else "Нет"
    final_debt_amount = new_to_pay if new_pay_type == "Долг" else 0
    update_invoice_in_sheet(row_index, 'paid_status', final_paid_status)
    update_invoice_in_sheet(row_index, 'debt_amount', f"{final_debt_amount:.2f}")

    # Финальные действия
    context.user_data.pop('edit_invoice', None)
    await query.message.edit_text("✅ Накладная успешно обновлена! Все связанные данные пересчитаны.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Продолжить просмотр", callback_data=f"edit_invoice_cancel_{row_index}")]]))


async def show_seller_salary_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детализацию бонусов и кнопку для просмотра истории."""
    query = update.callback_query
    seller_name = query.data.split('_', 2)[2]
    await query.answer()

    bonus_to_pay, bonus_days = calculate_accrued_bonus(seller_name)
    start_period, end_period = get_current_payroll_period()

    msg = f"<b>Детализация бонусов для {seller_name}</b>\n"
    msg += f"<i>Период: {sdate(start_period)} - {sdate(end_period)}</i>\n\n"

    if not bonus_days:
        msg += "Начислений бонусов в этом периоде нет."
    else:
        for day in bonus_days:
            msg += f" • {day['date']}: +{day['bonus']:.2f}₴ (от продаж {day['sales']:.2f}₴)\n"
    
    msg += f"\n<b>Итого к выплате: {bonus_to_pay:.2f}₴</b>"
    
    kb = []
    if bonus_to_pay > 0:
        kb.append([InlineKeyboardButton(f"✅ Выплатить {bonus_to_pay:.2f}₴", callback_data=f"confirm_payout_{seller_name}_{bonus_to_pay}")])
    
    # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Добавляем новую кнопку ---
    kb.append([InlineKeyboardButton(f"📜 История всех выплат", callback_data=f"salary_history_{seller_name}_0")])
    
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="staff_management")])
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

async def show_sellers_comparison(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает сравнение статистики продавцов."""
    query = update.callback_query
    await query.message.edit_text("⏳ Собираю данные для сравнения и рисую график...")

    sellers_to_compare = ["Людмила", "Мария"]
    comparison_data = get_sellers_comparison_data(context, sellers_to_compare)

    if not comparison_data:
        await query.message.edit_text("Недостаточно данных для сравнения.", 
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="seller_stats")]]))
        return
        
    image_buffer = generate_comparison_chart(comparison_data)
    
    msg = "<b>🏆 Сравнение продавцов</b> (средние продажи за день недели)\n\n"
    for day in DAYS_OF_WEEK_RU:
        msg += f"<b>{day.capitalize()}:</b>\n"
        for seller in sellers_to_compare:
            avg_sale = comparison_data.get(seller, {}).get(day, 0)
            msg += f"  - <i>{seller}:</i> {avg_sale:.2f}₴\n"

    await query.message.delete()
    await context.bot.send_photo(
        chat_id=query.message.chat_id,
        photo=image_buffer,
        caption=msg,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="seller_stats")]])
    )


async def confirm_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает подтверждение выплаты зарплаты."""
    query = update.callback_query
    _, _, seller_name, amount_str = query.data.split('_')
    amount = float(amount_str)
    await query.answer()
    
    text = (f"❗️<b>Подтвердите действие</b>❗️\n\n"
            f"Вы уверены, что хотите выплатить бонус продавцу <b>{seller_name}</b> "
            f"в размере <b>{amount:.2f}₴</b>?\n\n"
            f"Это действие нельзя отменить.")
    
    kb = [[
        InlineKeyboardButton("✅ Да, выплатить", callback_data=f"execute_payout_{seller_name}_{amount}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"view_salary_{seller_name}")
    ]]
    await query.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---

async def safe_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # ИСПРАВЛЕНИЕ: Передаем 'context' в функцию
    bal = get_safe_balance(context)
    
    await query.message.edit_text(
        f"💵 Остаток в сейфе: <b>{bal:.2f}₴</b>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stock_safe_menu")]])
    )
async def execute_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполняет выплату и записывает данные."""
    query = update.callback_query
    _, _, seller_name, amount_str = query.data.split('_')
    amount = float(amount_str)
    await query.answer()

    start_period, end_period = get_current_payroll_period()
    period_str = f"за период {sdate(start_period)}-{sdate(end_period)}"
    
    try:
        ws = GSHEET.worksheet(SHEET_SALARIES)
        ws.append_row([sdate(), seller_name, "Выплата бонуса", amount, period_str])
        
        # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
        msg = f"✅ Бонус в размере {amount:.2f}₴ для {seller_name} успешно выплачен и записан в историю."
        kb = [[InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")]]
        await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(kb))

    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка записи выплаты: {e}")



# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_salary_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает страничный просмотр истории ТОЛЬКО ВЫПЛАЧЕННЫХ БОНУСОВ для продавца."""
    query = update.callback_query
    
    try:
        _, _, seller_name, page_str = query.data.split('_')
        page = int(page_str)
    except (ValueError, IndexError):
        await query.answer("Ошибка в данных для навигации.", show_alert=True)
        return
        
    await query.answer()

    try:
        ws = GSHEET.worksheet(SHEET_SALARIES)
        all_rows = ws.get_all_values()[1:]
        
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Фильтруем записи по типу "Выплата бонуса" ---
        seller_rows = [
            row for row in all_rows 
            if len(row) > 2 and row[1] == seller_name and row[2] == "Выплата бонуса"
        ]
        seller_rows.reverse() # Показываем самые новые записи сначала
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка получения истории зарплат: {e}")
        return

    per_page = 5 # Уменьшим количество на странице для лучшей читаемости
    total_records = len(seller_rows)
    total_pages = math.ceil(total_records / per_page) if total_records > 0 else 1
    page = max(0, min(page, total_pages - 1))

    start_index = page * per_page
    end_index = start_index + per_page
    page_records = seller_rows[start_index:end_index]

    msg = f"<b>📜 История выплат бонусов для {seller_name}</b>\n(Стр. {page + 1}/{total_pages})\n"

    if not page_records:
        msg += "\n<i>Записей о выплаченных бонусах не найдено.</i>"
    else:
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Новый красивый формат вывода ---
        for row in page_records:
            date = row[0] if len(row) > 0 else ""
            amount = row[3] if len(row) > 3 else "0"
            comment = row[4] if len(row) > 4 else "" # Комментарий содержит период
            
            msg += "\n──────────────────\n"
            msg += f"🗓 <i>{date}</i>\n"
            msg += f"💰 <b>Сумма:</b> {amount}₴\n"
            msg += f"📋 <b>Детали:</b> Выплата бонуса {comment}\n"

    kb_nav = []
    if page > 0:
        kb_nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"salary_history_{seller_name}_{page - 1}"))
    if (page + 1) < total_pages:
        kb_nav.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"salary_history_{seller_name}_{page + 1}"))

    kb = []
    if kb_nav:
        kb.append(kb_nav)
    
    kb.append([InlineKeyboardButton("🔙 К деталям продавца", callback_data=f"view_salary_{seller_name}")])

    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# Функция для обновления данных у поставщика
def update_supplier_payment(supplier_name, amount, user_name, debt_closed, debt_id=None):
    ws_sup = GSHEET.worksheet(SHEET_SUPPLIERS)
    try:
        # Если есть debt_id, ищем по уникальному номеру, иначе — по имени
        if debt_id:
            cell = ws_sup.find(debt_id)
            if not cell:
                return
            row_num = cell.row
        else:
            # Ищем по имени (первая найденная строка)
            cell = ws_sup.find(supplier_name)
            if not cell:
                return
            row_num = cell.row

        row = ws_sup.row_values(row_num)
        debt_col = 9  # "Долг" — 9-я колонка (проверь у себя!)
        paid_col = 8  # "Оплачено" — 8-я колонка
        hist_col = 13 # "История погашений" — 13-я колонка
        who_col = 12  # "Кто внёс" — 12-я колонка

        # История погашений
        old_history = ws_sup.cell(row_num, hist_col).value or ""
        new_history = old_history + f"{sdate()}: {amount:.2f}₴ ({user_name}); "
        ws_sup.update_cell(row_num, hist_col, new_history)
        # Обновим долг
        old_debt = float(row[debt_col - 1]) if len(row) >= debt_col and row[debt_col - 1] else 0
        new_debt = old_debt - amount
        ws_sup.update_cell(row_num, debt_col, max(new_debt, 0))
        # Оплачено — если долг закрыт
        if debt_closed or new_debt <= 0:
            ws_sup.update_cell(row_num, paid_col, f"Да ({float(row[4]):.2f})")
        # Кто погасил
        ws_sup.update_cell(row_num, who_col, user_name)
    except Exception as e:
        logging.error(f"Ошибка обновления поставщика при погашении долга: {e}")


async def search_debts_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['search_debt'] = {}
    await query.message.edit_text("🔎 Введите СУММА или ИМЯ или ДАТУ поставщика для поиска:", reply_markup=back_kb())

# --- ДОБАВЬТЕ ЭТУ ФУНКЦИЮ В ВАШ КОД ---
async def show_report(update: Update, context: ContextTypes.DEFAULT_TYPE, start_date, end_date):
    """Показывает отчет за период, используя кэшированные данные."""
    # Определяем, откуда пришел вызов, чтобы правильно ответить
    if hasattr(update, "callback_query") and update.callback_query:
        query = update.callback_query
        await query.answer("Загружаю отчет...")
        msg_func = query.message.edit_text
    else:
        msg_func = update.message.reply_text

    # Используем кэширование для всех трех листов
    report_rows = get_cached_sheet_data(context, SHEET_REPORT)
    exp_rows = get_cached_sheet_data(context, SHEET_EXPENSES)
    sup_rows = get_cached_sheet_data(context, SHEET_SUPPLIERS)

    # Проверяем, что все данные загрузились
    if report_rows is None or exp_rows is None or sup_rows is None:
        await msg_func("❌ Ошибка чтения данных из одной из таблиц. Попробуйте позже.")
        return

    # --- Дальнейшая логика обработки данных ---
    days = defaultdict(list)
    for row in report_rows:
        try:
            row_date = pdate(row[0])
            if row_date and start_date <= row_date <= end_date:
                days[sdate(row_date)].append(row)
        except (ValueError, IndexError):
            continue

    expenses_by_day = defaultdict(float)
    for row in exp_rows:
        try:
            exp_date = pdate(row[0])
            if exp_date and start_date <= exp_date <= end_date and len(row) > 1 and row[1]:
                expenses_by_day[sdate(exp_date)] += float(row[1].replace(',', '.'))
        except (ValueError, IndexError):
            continue

    suppliers_by_day = defaultdict(float)
    for row in sup_rows:
        try:
            sup_date = pdate(row[0])
            if len(row) > 2 and row[2] and sup_date and start_date <= sup_date <= end_date:
                suppliers_by_day[sdate(sup_date)] += float(row[2].replace(',', '.'))
        except (ValueError, IndexError):
            continue

    response = f"📊 Отчет за период: {sdate(start_date)} — {sdate(end_date)}\n\n"
    total_cash = total_terminal = total_expenses = total_suppliers = 0

    if not days:
        response += "<i>За выбранный период нет сданных смен.</i>"
    else:
        for day, day_rows in sorted(days.items(), key=lambda item: pdate(item[0])):
            for r in day_rows:
                cash = float(r[2].replace(',', '.')) if len(r) > 2 and r[2] else 0
                terminal = float(r[3].replace(',', '.')) if len(r) > 3 and r[3] else 0
                seller = r[1]
                response += f"📅 <b>{day}</b> ({seller})\n   💵 {cash:.2f}₴ | 💳 {terminal:.2f}₴\n"
                exp = expenses_by_day.get(day, 0)
                sup = suppliers_by_day.get(day, 0)
                response += f"   💸 Расходы: {exp:.2f}₴ | 📦 Поставщики: {sup:.2f}₴\n"
                response += "─────────────\n"
                total_cash += cash
                total_terminal += terminal
                total_expenses += exp
                total_suppliers += sup

    response += (
        f"\n<b>Итого за период:</b>\n"
        f"💵 Наличные: {total_cash:.2f}₴\n"
        f"💳 Карта: {total_terminal:.2f}₴\n"
        f"💸 Расходы: {total_expenses:.2f}₴\n"
        f"📦 Поставщики: {total_suppliers:.2f}₴\n"
        f"🏦 Чистая прибыль: <b>{(total_cash + total_terminal) - (total_expenses + total_suppliers):.2f}₴</b>"
    )

    kb = week_buttons(start_date, end_date) if (end_date - start_date).days <= 7 else month_buttons(start_date, end_date)

    try:
        await msg_func(response, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass 
        else:
            raise

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_daily_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Формирует и показывает красивую и полную оперативную панель на текущий день."""
    query = update.callback_query
    await query.answer("Собираю оперативную сводку...")

    today_str = sdate()
    today = pdate(today_str)

    # --- 1. Загружаем все необходимые данные из кэша ---
    all_data = {
        sheet: get_cached_sheet_data(context, sheet)
        for sheet in [SHEET_SHIFTS, SHEET_PLAN_FACT, SHEET_SUPPLIERS, SHEET_DEBTS, SHEET_EXPENSES, SHEET_INVENTORY]
    }
    if any(data is None for data in all_data.values()):
        await query.message.edit_text("❌ Не удалось загрузить все данные из таблиц. Попробуйте позже.")
        return

    # --- 2. Обрабатываем данные и готовим переменные ---
    # Кто на смене
    on_shift_today = "Не указано"
    for row in all_data[SHEET_SHIFTS]:
        if row and row[0] == today_str:
            on_shift_today = ", ".join([seller for seller in row[1:] if seller])
            break
    
    # План/Факт по поставщикам
    todays_plans = {row[1].strip(): row for row in all_data[SHEET_PLAN_FACT] if row and row[0] == today_str}
    todays_invoices_suppliers = {row[1].strip() for row in all_data[SHEET_SUPPLIERS] if row and row[0] == today_str}
    
    arrived_suppliers_text = []
    pending_suppliers_data = []
    
    for planned_supplier, plan_data in todays_plans.items():
        planned_amount = plan_data[2]
        planned_pay_type = plan_data[3]
        planned_pay_type_human = "Наличные" if 'налич' in planned_pay_type.lower() else "Карта" if 'карт' in planned_pay_type.lower() else "Долг"

        if planned_supplier in todays_invoices_suppliers:
            actual_invoice = next((inv for inv in all_data[SHEET_SUPPLIERS] if inv[0] == today_str and inv[1].strip() == planned_supplier), None)
            actual_amount_str = actual_invoice[4].replace(',', '.') if actual_invoice and len(actual_invoice) > 4 else "0"
            actual_amount = f"{float(actual_amount_str):.2f}"
            actual_pay_type = actual_invoice[6] if actual_invoice and len(actual_invoice) > 6 else planned_pay_type_human
            arrived_suppliers_text.append(f"  ✅ {planned_supplier} (факт: {actual_amount}₴) (план: {planned_amount}₴ - {planned_pay_type_human})")
        else:
            pending_suppliers_data.append(plan_data)
            arrived_suppliers_text.append(f"  ⌛️ {planned_supplier} (план: {planned_amount}₴ - {planned_pay_type_human})")

    # Финансовые потребности
    total_planned_cash = sum(float(row[2].replace(',', '.')) for row in todays_plans.values() if 'налич' in row[3].lower())
    needed_cash_remaining = sum(float(data[2].replace(',', '.')) for data in pending_suppliers_data if 'налич' in data[3].lower())
    
    # Критические долги на сегодня
    critical_debts_text = []
    for row in all_data[SHEET_DEBTS]:
        if len(row) > 6 and row[6].strip().lower() != 'да' and pdate(row[5]) == today:
            critical_debts_text.append(f"  • {row[1]} - {float(row[4].replace(',', '.')):.2f}₴")

    # Активность за день
    invoice_count_today = len(todays_invoices_suppliers)
    
    expenses_list = [f"  • {float(row[1].replace(',', '.')):.2f}₴ - {row[2]}" for row in all_data[SHEET_EXPENSES] if row and row[0] == today_str and len(row) > 2 and row[1]]
    writeoffs_list = [f"  • {float(row[2].replace(',', '.')):.2f}₴ - {row[3]}" for row in all_data[SHEET_INVENTORY] if row and row[0] == today_str and row[1] == "Списание" and len(row) > 3 and row[2]]

    # --- 3. Собираем красивое сообщение ---
    msg = f"<b>☀️ Оперативная сводка на {today_str}</b>\n"
    msg += f"<b>👤 На смене:</b> {on_shift_today}\n"
    msg += "──────────────────\n"
    msg += "<b>🚚 Прибытие товаров (План/Факт)</b>\n"
    msg += "\n".join(arrived_suppliers_text) if arrived_suppliers_text else "<i>Нет запланированных прибытий.</i>"
    
    msg += "\n──────────────────\n<b>💰 Финансы на сегодня:</b>\n"
    msg += f"  • 💵 ОБЩИЙ план наличными на сегодня: {total_planned_cash:.2f}₴\n"
    msg += f"  • 💵 Осталось оплатить поставщикам: {needed_cash_remaining:.2f}₴\n"
    if critical_debts_text:
        msg += "❗️ <b>Критические долги (срок сегодня):</b>\n" + "\n".join(critical_debts_text)
    
    msg += "\n──────────────────\n<b>📊 Активность за день:</b>\n"
    msg += f"  • 🧾 Добавлено накладных: {invoice_count_today}\n"
    msg += "  • 💸 Расходы по кассе:\n" + ("\n".join(expenses_list) if expenses_list else "    <i>(нет)</i>\n")
    msg += "  • 🗑️ Списания с остатка:\n" + ("\n".join(writeoffs_list) if writeoffs_list else "    <i>(нет)</i>")
    
    # --- 4. Отправляем сообщение ---
    kb = [[InlineKeyboardButton("🔄 Обновить", callback_data="daily_summary")],
          [InlineKeyboardButton("🔙 Назад в меню Финансы", callback_data="finance_menu")]]
    
    try:
        await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer("Нет новых данных для обновления.")
        else:
            logging.error(f"Ошибка при обновлении сводки: {e}")
            await query.answer(f"❌ Ошибка: {e}", show_alert=True)
            
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def ask_for_invoice_edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Единственная функция, которая задает вопросы при редактировании накладной."""
    query = update.callback_query
    message = query.message if query else update.message
    
    if query:
        await query.answer()

    edit_state = context.user_data.get('edit_invoice', {})
    row_index = edit_state.get('row_index')
    
    # Если мы только начинаем задавать вопросы, формируем список полей
    if 'fields_to_edit_list' not in edit_state:
        fields_to_edit = list(edit_state.get('selected_fields', {}).keys())
        # Правильная последовательность запросов
        field_order = ['amount_income', 'writeoff', 'markup_amount', 'comment', 'pay_type', 'due_date']
        edit_state['fields_to_edit_list'] = [f for f in field_order if f in fields_to_edit]
        edit_state['current_field_index'] = 0

    fields_to_edit = edit_state.get('fields_to_edit_list', [])
    current_index = edit_state.get('current_field_index', 0)

    # Если все вопросы заданы, переходим к подтверждению
    if current_index >= len(fields_to_edit):
        await show_invoice_edit_confirmation(update, context)
        return

    current_field = fields_to_edit[current_index]
    edit_state['step'] = f"editing_{current_field}"
    
    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    old_data_row = all_invoices[row_index - 2]
    
    column_map = {'amount_income': 2, 'writeoff': 3, 'markup_amount': 5, 'pay_type': 6, 'due_date': 9, 'comment': 10}
    old_value = old_data_row[column_map.get(current_field)] if len(old_data_row) > column_map.get(current_field, 99) else ""

    prompts = {
        'amount_income': f"💰 Введите новую сумму прихода (текущая: {old_value}₴):",
        'writeoff': f"↩️ Введите новую сумму возврата (текущая: {old_value}₴):",
        'markup_amount': f"🧾 Введите новую сумму после наценки (текущая: {old_value}₴):",
        'pay_type': f"💳 Выберите новый тип оплаты (текущий: {old_value}):",
        'due_date': f"📅 Введите новую дату долга (текущая: {old_value}):",
        'comment': f"📝 Введите новый комментарий (текущий: '{old_value}'):"
    }
    prompt_text = prompts.get(current_field, "Введите новое значение:")
    
    kb = None
    if current_field == 'pay_type':
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💵 Наличные", callback_data="invoice_edit_value_Наличные")],
            [InlineKeyboardButton("💳 Карта", callback_data="invoice_edit_value_Карта")],
            [InlineKeyboardButton("📆 Долг", callback_data="invoice_edit_value_Долг")]])
    
    if query:
        await message.edit_text(prompt_text, reply_markup=kb)
    else:
        await message.reply_text(prompt_text, reply_markup=kb)
        
async def repay_debt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = parse_float(update.message.text)
        repay_data = context.user_data['repay']
        debt_idx = repay_data['debt_idx']
        who_paid = update.effective_user.first_name

        # 1. Получаем выбранный долг из context.user_data['repay_debts']
        repay_debts = context.user_data.get('repay_debts', [])
        if debt_idx >= len(repay_debts):
            await update.message.reply_text("❌ Долг не найден")
            return

        debt = repay_debts[debt_idx]
        date, supplier_name, total = debt[0], debt[1], float(debt[2])

        ws_debts = GSHEET.worksheet(SHEET_DEBTS)
        rows = ws_debts.get_all_values()
        # 2. Ищем нужный долг по дате+поставщик+сумма (а не просто индекс)
        row_idx = None
        for i, row in enumerate(rows[1:], start=2):
            if row[0] == date and row[1] == supplier_name and abs(float(row[2]) - total) < 0.01:
                row_idx = i
                break
        if not row_idx:
            await update.message.reply_text("❌ Долг не найден в таблице!")
            return

        paid = float(ws_debts.cell(row_idx, 4).value or 0)
        balance = float(ws_debts.cell(row_idx, 5).value or 0)
        new_paid = paid + amount
        new_balance = balance - amount

        ws_debts.update_cell(row_idx, 4, new_paid)
        ws_debts.update_cell(row_idx, 5, new_balance)

        debt_closed = False
        if new_balance <= 0.01:
            ws_debts.update_cell(row_idx, 7, "Да")
            debt_closed = True

        # === ОБНОВЛЯЕМ СТАТУС В ПОСТАВЩИКАХ ===
        ws_sup = GSHEET.worksheet(SHEET_SUPPLIERS)
        sup_rows = ws_sup.get_all_values()
        found_idx = None
        for idx, row in enumerate(sup_rows[1:], start=2):
            sup_date = row[0]
            sup_name = row[1]
            sup_debt = float(row[8]) if len(row) > 8 and row[8] else 0
            if sup_date == date and sup_name == supplier_name and abs(sup_debt - balance) < 0.01:
                found_idx = idx
                break

        if found_idx:
            ws_sup.update_cell(found_idx, 7+1, f"Да ({new_paid:.2f})")
            ws_sup.update_cell(found_idx, 8+1, 0)
            old_hist = ws_sup.cell(found_idx, 13).value or ""
            ws_sup.update_cell(
                found_idx, 13, 
                (old_hist or "") + f"{sdate()}: {amount:.2f}₴ ({who_paid}); "
            )
        else:
            logging.warning("Связанный долг в 'Поставщиках' не найден")

        await update.message.reply_text(
            f"✅ Долг погашен на сумму {amount:.2f}₴\nОстаток: {new_balance:.2f}₴"
            + ("\n\n📗 Статус в 'Поставщиках' обновлён!" if debt_closed else "")
        )
        context.user_data.pop('repay', None)
        await view_debts(update, context)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка погашения долга: {str(e)}")


# далее стандартная логика handle_planning_amount и handle_planning_paytype
# <<< НАЧАЛО: НОВЫЙ КОД ДЛЯ ДОБАВЛЕНИЯ >>>

# --- ЛОГИКА ПЛАНИРОВАНИЯ ---
DAYS_OF_WEEK_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]

# 1. Нажатие на кнопку "Планирование"
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def start_planning(update: Update, context: ContextTypes.DEFAULT_TYPE, target_date: dt.date = None):
    """Показывает обновленное, чистое и компактное меню планирования."""
    query = update.callback_query
    if query:
        await query.answer()

    today = dt.date.today()
    if target_date is None:
        target_date = today + dt.timedelta(days=1)

    target_date_str = sdate(target_date)
    day_of_week_name = DAYS_OF_WEEK_RU[target_date.weekday()]
    
    # Расширенный период планирования
    days_until_next_sunday = (6 - today.weekday()) + 7
    end_of_planning_period = today + dt.timedelta(days=days_until_next_sunday)
    
    # --- Получение данных ---
    scheduled_today = get_suppliers_for_day(day_of_week_name)
    planned_data = get_planned_suppliers(target_date_str)
    planned_names = {item['supplier'] for item in planned_data}
    unplanned_scheduled = [s for s in scheduled_today if s not in planned_names]

    # --- Строим сообщение и клавиатуру ---
    # 1. Новый чистый заголовок
    header_text = f"🗓️  <b>ПЛАНИРОВАНИЕ НА {day_of_week_name.upper()}, {target_date_str}</b>"

    kb = []
    # 2. Навигация
    nav_row = []
    prev_day = target_date - dt.timedelta(days=1)
    if prev_day > today:
        nav_row.append(InlineKeyboardButton("◀️ Пред. день", callback_data=f"plan_nav_{sdate(prev_day)}"))
    
    next_day = target_date + dt.timedelta(days=1)
    if next_day <= end_of_planning_period:
        nav_row.append(InlineKeyboardButton("След. день ▶️", callback_data=f"plan_nav_{sdate(next_day)}"))
    
    if nav_row:
        kb.append(nav_row)
    
    # 3. Обновленный блок "Уже запланировано" с компактными кнопками
    kb.append([InlineKeyboardButton("--- ✏️ Уже запланировано ---", callback_data="noop")])
    if not planned_data:
        kb.append([InlineKeyboardButton("(пусто)", callback_data="noop")])
    else:
        for item in planned_data:
            # Текст кнопки стал короче, чтобы влезать в одну строку
            btn_text = f"{item['supplier']} ({item['amount']}₴, {item['pay_type']})"
            kb.append([
                InlineKeyboardButton(btn_text, callback_data=f"edit_plan_{item['row_index']}"),
                InlineKeyboardButton("❌", callback_data=f"plan_delete_{item['row_index']}_{target_date_str}")
            ])

    # 4. Блок "Добавить по графику" с новым эмодзи
    kb.append([InlineKeyboardButton("--- 🚚 Добавить по графику ---", callback_data="noop")])
    if not unplanned_scheduled:
        kb.append([InlineKeyboardButton("(все добавлены)", callback_data="noop")])
    else:
        for supplier in unplanned_scheduled:
            kb.append([InlineKeyboardButton(f"➕ {supplier}", callback_data=f"plan_sup_{target_date_str}_{supplier}")])

    kb.append([InlineKeyboardButton("📝 Внеплановый поставщик", callback_data=f"plan_sup_{target_date_str}_other")])
    kb.append([InlineKeyboardButton("🔙 В меню", callback_data="suppliers_menu")])

    if query:
        await query.message.edit_text(
            header_text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.HTML
        )
# --- ДОБАВЬТЕ ЭТИ ДВЕ НОВЫЕ ФУНКЦИИ ---

async def show_invoices_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает новый экран-список накладных за выбранный день.
    """
    query = update.callback_query
    await query.answer()

    try:
        # Формат: invoices_list_ДАТА
        # или старый формат: details_sup_ДАТА_ДАТА_СТАРТ...
        parts = query.data.split('_')
        date_str = parts[2]
        date_obj = pdate(date_str)
        # Сохраняем контекст для кнопки "Назад", чтобы знать, откуда пришли
        context.user_data['invoices_back_context'] = query.data
    except (ValueError, IndexError):
        await query.message.edit_text("❌ Ошибка в данных для показа накладных.")
        return

    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    if all_invoices is None:
        await query.message.edit_text("❌ Не удалось загрузить данные о накладных.")
        return

    # Находим все накладные и их реальные номера строк за нужный день
    day_invoices_with_index = [
        (i + 2, row) for i, row in enumerate(all_invoices)
        if len(row) > 0 and pdate(row[0]) == date_obj
    ]

    # Сохраняем номера строк для пагинации в детальном виде
    context.user_data['day_invoice_rows'] = [item[0] for item in day_invoices_with_index]
    
    msg = f"📦 <b>Накладные за {date_str}</b>\n\nВыберите накладную для детального просмотра:"
    kb = []
    if not day_invoices_with_index:
        msg = f"📦 <b>Накладные за {date_str}</b>\n\nЗа этот день накладных не найдено."
    else:
        for i, (row_num, invoice_data) in enumerate(day_invoices_with_index):
            supplier = invoice_data[1] if len(invoice_data) > 1 else "N/A"
            to_pay = invoice_data[4] if len(invoice_data) > 4 else "0"
            pay_type = invoice_data[6] if len(invoice_data) > 6 else "N/A"
            btn_text = f"{i+1}. {supplier} - {to_pay}₴ ({pay_type})"
            # Создаем callback для перехода к детальному просмотру
            kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_single_invoice_{date_str}_{i}")])
    
    # Кнопка "Назад" вернет нас в то меню, откуда мы пришли
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="suppliers_menu")])
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))


# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_single_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает детальный вид ОДНОЙ накладной с пагинацией и кнопкой редактирования.
    """
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split('_')
        date_str, current_index = parts[3], int(parts[4])
    except (ValueError, IndexError):
        await query.message.edit_text("❌ Ошибка навигации по накладным.")
        return

    day_invoice_rows_indices = context.user_data.get('day_invoice_rows', [])
    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)

    if not day_invoice_rows_indices or all_invoices is None:
        await query.message.edit_text("❌ Данные о накладных устарели, вернитесь назад и попробуйте снова.")
        return
        
    total_invoices = len(day_invoice_rows_indices)
    current_index = max(0, min(current_index, total_invoices - 1))
    
    target_row_num = day_invoice_rows_indices[current_index]
    invoice_data = all_invoices[target_row_num - 2]

    # --- Форматируем красивое сообщение (как и раньше) ---
    supplier = invoice_data[1] if len(invoice_data) > 1 else "???"
    amount_income = float(invoice_data[2].replace(',', '.')) if len(invoice_data) > 2 and invoice_data[2] else 0
    writeoff = float(invoice_data[3].replace(',', '.')) if len(invoice_data) > 3 and invoice_data[3] else 0
    to_pay = float(invoice_data[4].replace(',', '.')) if len(invoice_data) > 4 and invoice_data[4] else 0
    markup_amount = float(invoice_data[5].replace(',', '.')) if len(invoice_data) > 5 and invoice_data[5] else 0
    pay_type = invoice_data[6] if len(invoice_data) > 6 else "???"
    due_date = invoice_data[9] if len(invoice_data) > 9 else ""
    comment = invoice_data[10] if len(invoice_data) > 10 else ""

    msg = f"🧾 <b>Детали накладной ({current_index + 1}/{total_invoices})</b> за {date_str}\n\n"
    msg += f"<b>Поставщик:</b> {supplier}\n"
    if writeoff > 0:
        msg += f"  • Сумма прихода: {amount_income:.2f}₴\n"
        msg += f"  • Возврат/списание: {writeoff:.2f}₴\n"
    msg += f"  • <b>К оплате:</b> {to_pay:.2f}₴\n"
    msg += f"  • <b>Сумма после наценки:</b> {markup_amount:.2f}₴\n"
    msg += f"  • <b>Тип оплаты:</b> {pay_type}\n"
    if pay_type == "Долг" and due_date:
        msg += f"     <i>(Срок погашения: {due_date})</i>\n"
    if comment:
        msg += f"  • <b>Комментарий:</b> {comment}\n"

    # --- Клавиатура с пагинацией и кнопкой "Редактировать" ---
    kb_nav = []
    if current_index > 0:
        kb_nav.append(InlineKeyboardButton("◀️ Пред.", callback_data=f"view_single_invoice_{date_str}_{current_index - 1}"))
    if current_index < total_invoices - 1:
        kb_nav.append(InlineKeyboardButton("След. ▶️", callback_data=f"view_single_invoice_{date_str}_{current_index + 1}"))
    
    kb = []
    if kb_nav: kb.append(kb_nav)
    
    # Добавляем кнопку редактирования, передавая номер строки в таблице
    kb.append([InlineKeyboardButton(f"✏️ Редактировать ({supplier})", callback_data=f"edit_invoice_start_{target_row_num}")])
    kb.append([InlineKeyboardButton("🔙 К списку накладных", callback_data=f"invoices_list_{date_str}")])
    
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    
# 2. Выбор поставщика из списка или ввод нового
async def handle_planning_supplier_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Новый формат: plan_sup_ДАТА_ИмяПоставщика или plan_sup_ДАТА_other
    parts = query.data.split('_', 3)
    target_date_str = parts[2]
    supplier_name = parts[3]
    
    # Сохраняем дату в состояние планирования
    context.user_data['planning'] = {
        'date': target_date_str
    }
    
    if supplier_name == "other":
        context.user_data['planning']['step'] = 'other_supplier_name'
        await query.message.edit_text(
            f"✍️ Введите имя внепланового поставщика на {target_date_str}:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="planning")]])
        )
    else:
        # Сохраняем выбранного поставщика и переходим к вводу суммы
        context.user_data['planning']['supplier'] = supplier_name
        context.user_data['planning']['step'] = 'amount'
        await query.message.edit_text(
            f"💰 Введите примерную сумму для <b>{supplier_name}</b> на {target_date_str} (в гривнах):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="planning")]]),
            parse_mode=ParseMode.HTML
        )
        
async def handle_planning_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(',', '.'))
        context.user_data['planning']['amount'] = amount
        context.user_data['planning']['step'] = 'payment_type'
        
        kb = [
            [InlineKeyboardButton("💵 Наличные", callback_data="plan_pay_Наличные")],
            [InlineKeyboardButton("💳 Карта", callback_data="plan_pay_Карта")],
            [InlineKeyboardButton("📆 Долг", callback_data="plan_pay_Долг")],
            [InlineKeyboardButton("🔙 Назад (к выбору поставщика)", callback_data="planning")]
        ]
        
        await update.message.reply_text(
            "💳 Выберите тип оплаты:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы. Введите число.")
    except Exception as e:
        await update.message.reply_text(f"❌ Произошла ошибка: {e}")

# 4. Выбор типа оплаты и сохранение
async def handle_planning_pay_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    pay_type = query.data.split('_', 2)[2]
    
    planning_data = context.user_data['planning']
    supplier = planning_data['supplier']
    amount = planning_data['amount']
    user_name = update.effective_user.first_name
    # Используем дату, сохраненную на предыдущем шаге
    target_date_str = planning_data['date']
    
    # Сохраняем в таблицу
    save_plan_fact(target_date_str, supplier, amount, pay_type, user_name)
    
    await query.message.edit_text(
        f"✅ План для <b>{supplier}</b> на <b>{target_date_str}</b> на сумму <b>{amount:.2f}₴</b> ({pay_type}) сохранен!\n\n"
        "Хотите спланировать следующего поставщика?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Продолжить планирование", callback_data="planning")],
            [InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")],
        ])
    )
    context.user_data.pop('planning', None)
    
# --- ПЕРЕУЧЕТ ---


async def start_revision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс переучета."""
    query = update.callback_query
    await query.answer()

    # Сначала получаем расчетный баланс из нашей существующей функции
    calculated_balance = get_inventory_balance()
    
    # Сохраняем состояние
    context.user_data['revision'] = {
        'step': 'actual_amount',
        'calculated': calculated_balance
    }
    
    msg = (f"🧮 <b>Проведение переучета</b>\n\n"
           f"Расчетный остаток товара в магазине: <b>{calculated_balance:.2f}₴</b>\n\n"
           f"Пожалуйста, введите фактический остаток товара (по результатам подсчета):")

    await query.message.edit_text(msg, parse_mode=ParseMode.HTML)


async def handle_revision_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод фактического остатка и запрашивает комментарий."""
    try:
        actual_amount = float(update.message.text.replace(',', '.'))
        context.user_data['revision']['actual'] = actual_amount
        context.user_data['revision']['step'] = 'comment'
        
        await update.message.reply_text("📝 Теперь введите комментарий к переучету (например, 'Плановый переучет за июнь'):")

    except (ValueError, KeyError):
        await update.message.reply_text("❌ Ошибка. Пожалуйста, введите сумму числом.")
        return


async def save_revision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет результаты переучета и выводит итог."""
    comment = update.message.text
    revision_data = context.user_data.get('revision', {})
    
    calculated = revision_data.get('calculated')
    actual = revision_data.get('actual')
    user = update.effective_user.first_name
    
    if calculated is None or actual is None:
        await update.message.reply_text("❌ Произошла ошибка, данные утеряны. Начните заново.")
        context.user_data.pop('revision', None)
        return

    # Используем вашу существующую функцию для записи данных
    add_revision(calculated, actual, comment, user)
    
    difference = actual - calculated
    diff_text = f"Излишек: +{difference:.2f}₴" if difference > 0 else f"Недостача: {difference:.2f}₴"
    if abs(difference) < 0.01:
        diff_text = "Расхождений нет"

    msg = (f"✅ <b>Переучет завершен!</b>\n\n"
           f"<b>Расчетный остаток:</b> {calculated:.2f}₴\n"
           f"<b>Фактический остаток:</b> {actual:.2f}₴\n"
           f"<b>Результат:</b> {diff_text}\n\n"
           f"<i>Комментарий: {comment}</i>")

    kb = [[InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")]]
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    context.user_data.pop('revision', None)
    


async def show_invoice_edit_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает экран с подтверждением изменений 'Было/Станет'."""
    query = update.callback_query
    message = query.message if query else update.message
    
    if query:
        await query.answer()

    edit_state = context.user_data.get('edit_invoice', {})
    row_index = edit_state.get('row_index')
    new_values = edit_state.get('new_values')

    if not row_index or not new_values:
        await message.reply_text("❌ Ошибка: данные для редактирования утеряны.")
        return

    all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    old_data_row = all_invoices[row_index - 2]
    
    field_names = {
        'amount_income': "Сумма прихода", 'writeoff': "Возврат/списание",
        'markup_amount': "Сумма после наценки", 'pay_type': "Тип оплаты",
        'due_date': "Дата долга", 'comment': "Комментарий"
    }
    column_map = {'amount_income': 2, 'writeoff': 3, 'markup_amount': 5, 'pay_type': 6, 'due_date': 9, 'comment': 10}

    msg = "<b>❗️Подтвердите изменения:</b>\n"
    for field, new_value in new_values.items():
        col_index = column_map.get(field)
        old_value = old_data_row[col_index] if len(old_data_row) > col_index else ""
        msg += f"\n<u>{field_names.get(field, field)}</u>:\n"
        msg += f"  • Было: <code>{old_value}</code>\n"
        msg += f"  • Станет: <b>{new_value}</b>"

    kb = [[
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"execute_invoice_edit_{row_index}"),
        InlineKeyboardButton("🚫 Отмена", callback_data=f"edit_invoice_cancel_{row_index}")
    ]]
    
    # Редактируем или отправляем новое сообщение
    if query:
        await message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        
async def view_current_debts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(context.user_data.get('current_debts_page', 0))
    ws = GSHEET.worksheet(SHEET_DEBTS)
    rows = ws.get_all_values()[1:]
    debts = [row for row in rows if len(row) >= 7 and row[6].strip().lower() != "да"]

    per_page = 7
    total_pages = max(1, math.ceil(len(debts) / per_page))
    start = page * per_page
    end = start + per_page
    page_debts = debts[start:end] # Это правильный срез

    if not page_debts:
        await query.message.edit_text("🎉 Нет текущих долгов!", reply_markup=debts_menu_kb())
        return

    response = "<b>📋 Текущие долги:</b>\n\n"
    # Изменить этот цикл
    for i, row in enumerate(page_debts): # Итерируемся по page_debts
        # Индекс для пользователя будет смещен с учетом страницы
        display_idx = start + i + 1
        status = "✅" if row[6].strip().lower() == "да" else "🟠"
        response += (
            f"<b>#{display_idx} {status} {row[1]}</b>\n"
            f"    • Дата: {row[0]}\n"
            f"    • Сумма: <b>{parse_float(row[2]):.2f}₴</b>\n"
            f"    • Остаток: <b>{parse_float(row[4]):.2f}₴</b>\n"
            f"    • Срок: {row[5]}\n"
            f"    • Статус: {row[6]}\n"
            "─────────────\n"
        )
    kb = []
    if page > 0:
        kb.append(InlineKeyboardButton("⬅️ Назад", callback_data="current_debts_prev"))
    if page < total_pages - 1:
        kb.append(InlineKeyboardButton("➡️ Вперед", callback_data="current_debts_next"))
    kb = [kb] if kb else []
    kb.append([InlineKeyboardButton("🔙 Долги", callback_data="debts_menu")])

    await query.message.edit_text(response, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    context.user_data['current_debts_page'] = page


def get_week_debts(start, end):
    ws = GSHEET.worksheet(SHEET_DEBTS)
    rows = ws.get_all_values()[1:]
    debts = []
    for row in rows:
        if len(row) < 7:
            continue
        try:
            d = pdate(row[0])
        except Exception:
            continue
        if start <= d <= end:
            debts.append(row)
    return debts

def week_buttons_for_debts(start, end):
    prev = start - dt.timedelta(days=7)
    next = start + dt.timedelta(days=7)
    curr_start, curr_end = week_range()
    return [
        [
            InlineKeyboardButton("◀️ Пред. неделя", callback_data=f"debts_week_{sdate(prev)}_{sdate(prev + dt.timedelta(days=6))}"),
            InlineKeyboardButton("Текущая неделя", callback_data=f"debts_week_{sdate(curr_start)}_{sdate(curr_end)}"),
            InlineKeyboardButton("След. неделя ▶️", callback_data=f"debts_week_{sdate(next)}_{sdate(next + dt.timedelta(days=6))}")
        ],
        [InlineKeyboardButton("🔍 Поиск долга", callback_data="debts_search")],
        [InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")]
    ]

async def show_debts_history(update: Update, context: ContextTypes.DEFAULT_TYPE, start=None, end=None):
    query = update.callback_query
    await query.answer()

    if not start or not end:
        start, end = week_range()

    debts = get_week_debts(start, end)
    text = f"<b>История долгов за {sdate(start)} — {sdate(end)}:</b>\n\n"
    if not debts:
        text += "Нет долгов за этот период."
    else:
        for i, row in enumerate(debts):
            try:
                # Заменяем запятые на точки
                amount = parse_float(row[2])
                paid = parse_float(row[3])
            except ValueError:
                amount = 0.0
                paid = 0.0
            
            status = "✅ Закрыт" if row[6].strip().lower() == "да" else "❌ Открыт"
            history = row[7] if len(row) > 7 else ""
            text += (f"{i+1}. <b>{row[1]}</b> | {row[0]} | {amount:.2f}₴ | {status}\n"
                     f"   Оплачено: {paid:.2f}₴ | Срок: {row[5]}\n")
            if history:
                hist_lines = history.replace(';', '\n     •').strip()
                text += f"   История: \n     •{hist_lines}\n"
            text += "────────────\n"

    kb = week_buttons_for_debts(start, end)
    await query.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))


# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
def add_revision(calc_sum, fact_sum, comment, user):
    """
    Записывает данные о переучете в лист "Переучеты" и КОРРЕКТНО
    обновляет баланс в листе "Остаток магазина".
    """
    # 1. Запись в архив переучетов - эта часть у вас работала правильно.
    ws_revisions = GSHEET.worksheet("Переучеты")
    diff = fact_sum - calc_sum
    ws_revisions.append_row([sdate(), calc_sum, fact_sum, diff, comment, user])
    
    # 2. Корректировка баланса в "Остаток магазина" - здесь была ошибка.
    ws_inv = GSHEET.worksheet("Остаток магазина")
    # Создаем новую строку, где в столбце "Сумма" (третий столбец)
    # мы НЕ пишем старый остаток, а в столбце "Комментарий" (четвертый столбец)
    # мы записываем фактическую сумму, как вы и просили.
    # Это эффективно сбрасывает баланс до fact_sum.
    
    # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Тип операции "Переучет" говорит функции get_inventory_balance,
    # что нужно взять сумму из 4-го столбца и установить ее как новый баланс.
    # Поэтому 3-й столбец (сумма операции) мы оставляем пустым.
    # В 4-й столбец (комментарий) мы записываем фактическую сумму для get_inventory_balance.
    # Ваш комментарий к переучету будет в листе "Переучеты".
    
    # Мы сделаем еще лучше: запишем фактическую сумму в столбец "Сумма",
    # а в комментарий - пояснение. И обновим get_inventory_balance.
    ws_inv.append_row([sdate(), "Переучет", fact_sum, f"Новый остаток: {fact_sum}", user])
def is_date(string):
    try:
        dt.datetime.strptime(string, "%d.%m.%Y")
        return True
    except:
        return False
    
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
def stock_safe_kb(is_admin=False):
    kb = [
        [InlineKeyboardButton("📦 Остаток магазина", callback_data="inventory_balance")],
        [InlineKeyboardButton("💵 Остаток в сейфе", callback_data="safe_balance")],
        # Размещаем обе истории в одном ряду
        [
            InlineKeyboardButton("🧾 История остатка", callback_data="inventory_history"),
            InlineKeyboardButton("🧾 История сейфа", callback_data="safe_history")
        ],
        [
            InlineKeyboardButton("➕ Положить в сейф", callback_data="safe_deposit"),
            InlineKeyboardButton("➖ Снять из сейфа", callback_data="safe_withdraw")
        ],
        [InlineKeyboardButton("➖ Добавить списание с остатка", callback_data="add_inventory_expense")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(kb)

def get_tomorrow_debts():
    ws = GSHEET.worksheet(SHEET_DEBTS)
    rows = ws.get_all_values()[1:]
    tomorrow = (dt.date.today() + dt.timedelta(days=1)).strftime(DATE_FMT)
    total = 0
    suppliers = []
    for row in rows:
        if len(row) >= 6 and row[5] != "Да" and row[4]:  # Не погашен
            # row[5] — срок
            if row[5] == tomorrow:
                amount = parse_float(row[4])
                total += amount
                suppliers.append((row[1], amount))
    return total, suppliers


# --- FAQ ---
FAQ = [
    ("📝 Как сдать смену?", "Нажмите «➕ Новый отчёт», выберите себя, заполните суммы и расходы, следуйте подсказкам."),
    ("💸 Как добавить расход?", "В главном меню выберите «💰 Добавить расход», укажите сумму и комментарий."),
    ("📦 Как добавить поставщика?", "Выберите «📦 Добавить поставщика» и следуйте шагам. Для отсрочек укажите дату."),
    ("📆 Как посмотреть смены?", "Выберите «🗓 График смен»."),
    ("❓ Возникли вопросы?", "Пишите администратору — Наталия или Женя.")
]

# --- КЛАВИАТУРЫ ---
def main_kb(is_admin=False):
    kb = [
        [InlineKeyboardButton("💼 Работа с остатком и сейфом", callback_data="stock_safe_menu")],
        [InlineKeyboardButton("📊 Финансы", callback_data="finance_menu")],
        [InlineKeyboardButton("👥 Персонал", callback_data="staff_menu")],
        [InlineKeyboardButton("📦 Поставщики", callback_data="suppliers_menu"),
         InlineKeyboardButton("🏦 Долги", callback_data="debts_menu")],
        [InlineKeyboardButton("📈 Аналитика", callback_data="analytics_menu")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")]
    ]
    if is_admin:
        kb.append([InlineKeyboardButton("🔐 Админ-панель", callback_data="admin_panel")])
    kb.append([InlineKeyboardButton("❌ Закрыть", callback_data="close")])
    return InlineKeyboardMarkup(kb)

def finance_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Сдать смену", callback_data="add_report")],
        [InlineKeyboardButton("📋 Просмотр отчётов", callback_data="view_reports_menu")],
        [InlineKeyboardButton("📊 Ежедневная сводка", callback_data="daily_summary")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])

def reports_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 За сегодня", callback_data="report_today")],
        [InlineKeyboardButton("📅 За вчера", callback_data="report_yesterday")],
        [InlineKeyboardButton("🗓 За неделю", callback_data="report_week_current")],
        [InlineKeyboardButton("📆 За месяц", callback_data="report_month_current")],
        [InlineKeyboardButton("📆 За год", callback_data="report_year")],
        [InlineKeyboardButton("📆 Произвольный период", callback_data="report_custom")],
        [InlineKeyboardButton("🔙 Финансы", callback_data="finance_menu")]
    ])

def staff_menu_kb(is_admin=False):
    kb = [[InlineKeyboardButton("🗓 График смен", callback_data="view_shifts")]]
    if is_admin:
        kb.append([InlineKeyboardButton("✏️ Назначить/Изменить смену", callback_data="edit_shifts")])
        kb.append([InlineKeyboardButton("📊 Статистика продавцов", callback_data="seller_stats")])
    kb.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(kb)

def suppliers_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить накладную", callback_data="add_supplier")],
        [InlineKeyboardButton("🚚 Журнал прибытия товаров", callback_data="view_suppliers")],
        [InlineKeyboardButton("📄 Накладные за сегодня", callback_data="view_today_invoices")],
        # --- ВОЗВРАЩАЕМ КНОПКУ НА МЕСТО ---
        [InlineKeyboardButton("📅 Планирование", callback_data="planning")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])


def debts_menu_kb():
    return InlineKeyboardMarkup([
        # ИСПРАВЛЕНИЕ: Указываем начальную страницу 0 для пагинации
        [InlineKeyboardButton("📋 Текущие долги", callback_data="current_debts_0")],
        [InlineKeyboardButton("📆 Предстоящие платежи", callback_data="upcoming_payments")],
        [InlineKeyboardButton("✅ Погасить долг", callback_data="close_debt")],
        [InlineKeyboardButton("📜 История долгов", callback_data="debts_history_0")],
        [InlineKeyboardButton("🔎 Поиск долгов", callback_data="search_debts")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])

def analytics_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Продажи", callback_data="sales_analytics")],
        [InlineKeyboardButton("💸 Расходы", callback_data="expenses_analytics")],
        [InlineKeyboardButton("📦 Поставщики", callback_data="suppliers_analytics")],
        [InlineKeyboardButton("👥 Продавцы", callback_data="sellers_analytics")],
        [InlineKeyboardButton("📊 Финансовый отчёт", callback_data="financial_report")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])

def settings_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Профиль", callback_data="profile_settings")],
        [InlineKeyboardButton("🔔 Уведомления", callback_data="notification_settings")],
        [InlineKeyboardButton("📱 Внешний вид", callback_data="ui_settings")],
        [InlineKeyboardButton("🔑 Безопасность", callback_data="security_settings")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])

def admin_panel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Финансовый анализ", callback_data="financial_analysis")],
        [InlineKeyboardButton("👥 Управление персоналом", callback_data="staff_management")],
        [InlineKeyboardButton("⚙️ Системные настройки", callback_data="system_settings")],
        [InlineKeyboardButton("📋 Журнал действий", callback_data="action_log")],
        [InlineKeyboardButton("🧮 Переучёт", callback_data="admin_revision")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ])

def back_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ])


def cancel_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="main_menu")]])

def faq_kb():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(q, callback_data=f"faq_{i}")] for i, (q, _) in enumerate(FAQ)] + 
        [[InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]]
    )

# --- ОБРАБОТЧИКИ КОМАНД ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_admin = str(user.id) in ADMINS
    push_nav(context, "main_menu")  # <-- Важно!
    await update.message.reply_text(
        f"🏪 Добро пожаловать, {user.first_name}!\n"
        "📊 Ваш помощник в управлении магазином\n\n"
        "Выберите раздел меню:",
        reply_markup=main_kb(is_admin)
    )
    log_action(user, user.first_name, "Старт")

async def start_inventory_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['inventory_expense'] = {'step': 'amount'}
    await update.callback_query.message.edit_text(
        "💸 Введите сумму списания:",
        reply_markup=back_kb()
    )

async def handle_inventory_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(',', '.'))
        context.user_data['inventory_expense']['amount'] = amount
        context.user_data['inventory_expense']['step'] = 'comment'
        await update.message.reply_text(
            "📝 Введите комментарий к списанию (например, порча, возврат, подарок):",
            reply_markup=back_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Введите сумму числом.")

async def save_inventory_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text
    amount = context.user_data['inventory_expense']['amount']
    user = update.effective_user.first_name
    add_inventory_operation("Списание", amount, comment, user)
    await update.message.reply_text(f"✅ Списание {amount:.2f}₴ добавлено!\nКомментарий: {comment}")
    context.user_data.pop('inventory_expense', None)


# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    is_admin = str(query.from_user.id) in ADMINS
    # Убираем query.answer(), так как он уже есть в handle_callback
    await query.message.edit_text(
        "🏪 Главное меню\nВыберите раздел:",
        reply_markup=main_kb(is_admin)
    )

async def close_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.message.delete()
    await query.answer("Меню закрыто")

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_planned_arrivals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает интерактивный журнал с планами на СЕГОДНЯ и на ЗАВТРА."""
    query = update.callback_query
    await query.answer()

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)

    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        rows = ws.get_all_values()[1:]
    except Exception as e:
        await query.message.edit_text(f"❌ Не удалось прочитать лист планов: {e}")
        return

    # Собираем данные, добавляя номер строки в таблице (i+2, т.к. нумерация с 1 и есть заголовок)
    today_arrivals = [row + [i+2] for i, row in enumerate(rows) if row and pdate(row[0]) == today]
    tomorrow_plans = [row + [i+2] for i, row in enumerate(rows) if row and pdate(row[0]) == tomorrow]
    
    msg = "<b>🚚 Журнал прибытия и планов</b>\n"
    kb = []

    # --- Блок на СЕГОДНЯ (интерактивный) ---
    msg += f"\n<b><u>План на сегодня ({sdate(today)}):</u></b>\n"
    if not today_arrivals:
        msg += "<i>Нет запланированных прибытий.</i>\n"
    else:
        for arrival in today_arrivals:
            # arrival -> ['дата', 'поставщик', 'сумма', 'тип', 'кто', 'статус', 'номер_строки']
            status_icon = "✅" if len(arrival) > 5 and arrival[5] == "Прибыл" else "🛑"
            supplier, amount, pay_type, row_num = arrival[1], arrival[2], arrival[3], arrival[6]
            pay_type_human = "Наличные" if 'налич' in pay_type.lower() else "Карта" if 'карт' in pay_type.lower() else "Долг"
            button_text = f"{status_icon} {supplier} - {amount}₴ ({pay_type_human})"
            kb.append([InlineKeyboardButton(button_text, callback_data=f"toggle_arrival_{row_num}")])

    # --- Блок на ЗАВТРА (редактируемый) ---
    msg += f"\n<b><u>Планы на завтра ({sdate(tomorrow)}):</u></b>\n"
    if not tomorrow_plans:
        msg += "<i>Планов на завтра еще нет.</i>\n"
    else:
        for plan in tomorrow_plans:
            supplier, amount, pay_type, row_num = plan[1], plan[2], plan[3], plan[6]
            pay_type_human = "Наличные" if 'налич' in pay_type.lower() else "Карта" if 'карт' in pay_type.lower() else "Долг"
            button_text = f"✏️ {supplier} - {amount}₴ ({pay_type_human})"
            kb.append([InlineKeyboardButton(button_text, callback_data=f"edit_plan_{row_num}")])

    kb.append([InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")])
    
    try:
        await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer("Нет изменений для обновления.", show_alert=False)
        else:
            raise e
        
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
# --- ЗАМЕНИТЕ ФУНКЦИЮ show_planned_arrivals НА ЭТУ ---
# --- ЗАМЕНИТЕ ФУНКЦИЮ show_arrivals_journal НА ЭТУ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_arrivals_journal(update: Update, context: ContextTypes.DEFAULT_TYPE, target_date: dt.date = None):
    """Показывает обновленный, красивый и устойчивый журнал прибытия (План/Факт)."""
    query = update.callback_query
    if query:
        await query.answer()

    today = dt.date.today()
    if target_date is None:
        target_date = today

    target_date_str = sdate(target_date)
    day_of_week_name = DAYS_OF_WEEK_RU[target_date.weekday()]

    # Период навигации
    days_until_next_sunday = (6 - today.weekday()) + 7
    end_of_viewing_period = today + dt.timedelta(days=days_until_next_sunday)

    # --- Получение данных ---
    try:
        all_plans = get_cached_sheet_data(context, SHEET_PLAN_FACT, force_update=True) or []
        all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS, force_update=True) or []
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка чтения данных: {e}")
        return
        
    plans_for_day = [row for row in all_plans if row and row[0] == target_date_str]
    invoices_for_day = [row for row in all_invoices if row and row[0] == target_date_str]
    
    # --- Собираем сообщение ---
    msg_parts = [f"<b>🚚 Журнал прибытия на {day_of_week_name.upper()}, {target_date_str}</b>"]

    # --- Агрегируем данные для сводки ---
    suppliers_status = defaultdict(lambda: {
        'plan_amount': 0, 'plan_type': '-', 
        'fact_amount': 0, 'fact_types': set()
    })

    for plan in plans_for_day:
        supplier, amount, p_type = plan[1], parse_float(plan[2]), plan[3]
        suppliers_status[supplier]['plan_amount'] += amount
        suppliers_status[supplier]['plan_type'] = p_type

    for invoice in invoices_for_day:
        supplier, to_pay, pay_type = invoice[1], parse_float(invoice[4]), invoice[6]
        suppliers_status[supplier]['fact_amount'] += to_pay
        suppliers_status[supplier]['fact_types'].add(pay_type)

    if not suppliers_status:
        msg_parts.append("\n<i>На этот день нет ни планов, ни фактов.</i>")
    else:
        for supplier, data in sorted(suppliers_status.items()):
            status_icon = "✅" if data['fact_amount'] > 0 else "⌛️"
            
            plan_amount_str = f"{data['plan_amount']:.2f}₴"
            fact_amount_str = f"{data['fact_amount']:.2f}₴"
            
            plan_type_str = data['plan_type']
            fact_type_str = ", ".join(sorted(list(data['fact_types']))) or "-"

            supplier_block = (
                f"──────────────────\n"
                f"{status_icon} <b>{supplier}</b>\n"
                f"    • <b>План:</b> {plan_amount_str} <i>({plan_type_str})</i>\n"
                f"    • <b>Факт:</b> {fact_amount_str} <i>({fact_type_str})</i>"
            )
            msg_parts.append(supplier_block)

    # --- Собираем клавиатуру ---
    kb = []
    nav_row = []
    prev_day = target_date - dt.timedelta(days=1)
    # Ограничим навигацию назад, чтобы не уходить в далекое прошлое (например, 30 дней)
    if (today - prev_day).days < 30:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"journal_nav_{sdate(prev_day)}"))
    
    nav_row.append(InlineKeyboardButton("Сегодня", callback_data=f"journal_nav_{sdate(today)}"))
    
    next_day = target_date + dt.timedelta(days=1)
    if next_day <= end_of_viewing_period:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"journal_nav_{sdate(next_day)}"))
    
    kb.append(nav_row)
    kb.append([InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")])

    final_msg = "\n".join(msg_parts)
    if query:
        await query.message.edit_text(final_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        

async def toggle_arrival_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключает статус прибытия товара."""
    query = update.callback_query
    
    try:
        row_num = int(query.data.split('_')[2])
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        current_status = ws.cell(row_num, 6).value
        new_status = "Прибыл" if current_status != "Прибыл" else "Ожидается"
        ws.update_cell(row_num, 6, new_status)
        await query.answer(f"Статус изменен на '{new_status}'")
    except Exception as e:
        await query.answer(f"❌ Ошибка обновления: {e}", show_alert=True)
        return
        
    await show_arrivals_journal(update, context)

async def edit_plan_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс редактирования плана."""
    query = update.callback_query
    await query.answer()
    row_num = int(query.data.split('_')[2])
    context.user_data['edit_plan'] = {'row': row_num}

    kb = [
        [InlineKeyboardButton("💰 Изменить сумму", callback_data=f"edit_plan_field_amount")],
        [InlineKeyboardButton("💳 Изменить тип оплаты", callback_data=f"edit_plan_field_pay_type")],
        [InlineKeyboardButton("🔙 Назад в журнал", callback_data="view_suppliers")],
    ]
    await query.message.edit_text("Что вы хотите изменить в этом плане?", reply_markup=InlineKeyboardMarkup(kb))

async def edit_plan_choose_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает новое значение для выбранного поля."""
    query = update.callback_query
    await query.answer()
    field = query.data.split('_')[-1]
    context.user_data['edit_plan']['field'] = field

    if field == 'amount':
        await query.message.edit_text("Введите новую сумму:")
    elif field == 'pay_type':
        kb = [
            [InlineKeyboardButton("💵 Наличные", callback_data="edit_plan_value_Наличные")],
            [InlineKeyboardButton("💳 Карта", callback_data="edit_plan_value_Карта")],
            [InlineKeyboardButton("📆 Долг", callback_data="edit_plan_value_Долг")],
        ]
        await query.message.edit_text("Выберите новый тип оплаты:", reply_markup=InlineKeyboardMarkup(kb))


async def edit_plan_save_value(update: Update, context: ContextTypes.DEFAULT_TYPE, new_value=None):
    """Сохраняет новое значение и вызывает обновление журнала."""
    query = update.callback_query
    from_button = bool(query)

    if from_button:
        await query.answer()
        # Извлекаем значение из callback_data кнопки
        new_value = query.data.split('_')[-1]
    
    edit_data = context.user_data.get('edit_plan', {})
    row_num = edit_data.get('row')
    field = edit_data.get('field')
    
    # Проверяем, что у нас есть все данные для работы
    if not all([row_num, field, new_value is not None]):
        error_text = "Ошибка: не хватает данных для редактирования. Начните заново."
        if from_button: await query.answer(error_text, show_alert=True)
        else: await update.message.reply_text(error_text)
        return

    # Обновляем ячейку в таблице
    success = update_plan_in_sheet(row_num, field, new_value)

    if success:
        success_text = "✅ План обновлен!"
        if from_button: await query.answer(success_text)
        else: await update.message.reply_text(success_text)
    else:
        error_text = "❌ Ошибка обновления плана в таблице."
        if from_button: await query.answer(error_text, show_alert=True)
        else: await update.message.reply_text(error_text)

    # Очищаем состояние и показываем обновленный журнал
    context.user_data.pop('edit_plan', None)
    await show_arrivals_journal(update, context)

# --- МЕНЮ РАЗДЕЛОВ ---
async def finance_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "finance_menu")
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "💰 Управление финансами\nВыберите действие:",
        reply_markup=finance_menu_kb())
    
async def staff_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    is_admin = str(query.from_user.id) in ADMINS
    await query.message.edit_text(
        "👥 Управление персоналом\nВыберите действие:",
        reply_markup=staff_menu_kb(is_admin))
    
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def suppliers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Получаем статус админа и передаем его в клавиатуру ---
    is_admin = str(query.from_user.id) in ADMINS
    
    # Получаем сегодняшнюю дату для создания правильной callback_data
    today_str = sdate()

    # Создаем клавиатуру динамически
    kb = [
        [InlineKeyboardButton("➕ Добавить накладную", callback_data="add_supplier")],
        [InlineKeyboardButton("🚚 Журнал прибытия товаров", callback_data="view_suppliers")],
        [InlineKeyboardButton("📄 Накладные за сегодня", callback_data=f"invoices_list_{today_str}")],
        [InlineKeyboardButton("📅 Планирование", callback_data="planning")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    
    await query.message.edit_text(
        "📦 Управление поставщиками\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    
async def debts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "debts_menu")  # <---
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "🏦 Управление долгами\nВыберите действие:",
        reply_markup=debts_menu_kb())
    
async def analytics_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "analytics_menu")  # <--
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "📈 Аналитика и отчеты\nВыберите действие:",
        reply_markup=analytics_menu_kb())
    
async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "settings_menu")  # <-
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "⚙️ Настройки системы\nВыберите действие:",
        reply_markup=settings_menu_kb())

async def stock_safe_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "stock_safe_menu")
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "💼 Работа с остатком и сейфом. Выберите действие:",
        reply_markup=stock_safe_kb(str(query.from_user.id) in ADMINS)
    )

    
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if str(query.from_user.id) not in ADMINS:
        await query.answer("🚫 Только для администраторов", show_alert=True)
        return
    # Убираем query.answer()
    await query.message.edit_text(
        "🔐 Админ-панель\nВыберите действие:",
        reply_markup=admin_panel_kb())

# --- ОТЧЕТЫ ---
async def view_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        "📋 Просмотр отчетов\nВыберите период:",
        reply_markup=reports_menu_kb())

# --- ЗАМЕНИТЕ ЭТИ ДВЕ ФУНКЦИИ ---

async def get_report_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    start, end = week_range()
    # Передаем context в show_report
    await show_report(update, context, start, end)

async def get_report_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    start, end = month_range()
    # Передаем context в show_report
    await show_report(update, context, start, end)

async def get_report_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    today = dt.date.today()
    start = dt.date(today.year, 1, 1)
    end = dt.date(today.year, 12, 31)
    await show_report(update, start, end)

async def get_report_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['report_period'] = {'step': 'start_date'}
    await query.message.edit_text(
        "📅 Введите начальную дату (ДД.ММ.ГГГГ):",
        reply_markup=cancel_kb()
    )

async def handle_report_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        start_date = pdate(update.message.text)
        context.user_data['report_period']['start_date'] = start_date
        context.user_data['report_period']['step'] = 'end_date'
        await update.message.reply_text(
            f"📅 Начальная дата: {sdate(start_date)}\n\n"
            "📅 Введите конечную дату (ДД.ММ.ГГГГ):",
            reply_markup=cancel_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ")

async def handle_report_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        end_date = pdate(update.message.text)
        start_date = context.user_data['report_period']['start_date']
        if end_date < start_date:
            await update.message.reply_text("❌ Конечная дата не может быть раньше начальной")
            return
            
        # ИСПРАВЛЕНИЕ: Добавляем 'context' в вызов функции
        await show_report(update, context, start_date, end_date)
        
        context.user_data.pop('report_period', None)
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ")
        
        
# --- ОТЧЕТ О СМЕНЕ ---
async def start_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = [[InlineKeyboardButton(seller, callback_data=f"report_seller_{seller}")] for seller in SELLERS]
    kb.append([InlineKeyboardButton("❌ Отмена", callback_data="finance_menu")])
    await query.message.edit_text(
        "👤 Выберите продавца:",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def handle_report_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    seller = query.data.split('_')[2]
    context.user_data['report'] = {'seller': seller, 'step': 'cash'}
    await query.message.edit_text(
        f"💵 Введите сумму наличных за смену (в гривнах):",
        reply_markup=InlineKeyboardMarkup([  # <-- Заменить back_kb() на это
            [InlineKeyboardButton("🔙 Назад", callback_data="add_report")],
            [InlineKeyboardButton("❌ Отменить", callback_data="cancel_report")]
         ])
    )

# --- ДОБАВЬТЕ ВЕСЬ ЭТОТ БЛОК НОВЫХ ФУНКЦИЙ ---

# --- ЛОГИКА УПРАВЛЕНИЯ СМЕНАМИ ---

def generate_calendar_keyboard(year: int, month: int, shifts_data: dict, mode: str = 'view'):
    """Генерирует красивую клавиатуру с календарем на русском и с инициалами продавцов."""
    # Словарь с русскими названиями месяцев
    RU_MONTHS = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
        7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    
    kb = []
    # Название месяца и кнопки навигации
    month_name = f"{RU_MONTHS.get(month, '')} {year}"
    nav_row = [
        InlineKeyboardButton("◀️", callback_data=f"shift_nav_{year}_{month-1}" if month > 1 else f"shift_nav_{year-1}_12"),
        InlineKeyboardButton(month_name, callback_data="noop"),
        InlineKeyboardButton("▶️", callback_data=f"shift_nav_{year}_{month+1}" if month < 12 else f"shift_nav_{year+1}_1")
    ]
    kb.append(nav_row)

    # Дни недели
    kb.append([InlineKeyboardButton(day, callback_data="noop") for day in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

    # Даты календаря
    month_calendar = calendar.monthcalendar(year, month)
    for week in month_calendar:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
            else:
                date_str = f"{day:02d}.{month:02d}.{year}"
                sellers_on_day = shifts_data.get(date_str, [])
                
                # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Добавляем инициалы на кнопку ---
                if sellers_on_day:
                    initials = "".join([s[0] for s in sellers_on_day])
                    btn_text = f"{day}✅"
                else:
                    btn_text = str(day)
                
                callback = f"edit_shift_{date_str}" if mode == 'edit' else f"view_shift_{date_str}"
                row.append(InlineKeyboardButton(btn_text, callback_data=callback))
        kb.append(row)
        
    kb.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="staff_menu")])
    return InlineKeyboardMarkup(kb)

# --- ДОБАВЬТЕ ВЕСЬ ЭТОТ БЛОК НОВЫХ ФУНКЦИЙ ---

def get_seller_stats_data(context: ContextTypes.DEFAULT_TYPE, seller_name: str, days_period: int = 30):
    """Собирает статистику продаж для продавца за указанный период."""
    today = dt.date.today()
    start_date = today - dt.timedelta(days=days_period)
    
    reports = get_cached_sheet_data(context, SHEET_REPORT)
    if not reports:
        return None

    seller_reports = [row for row in reports if len(row) > 4 and row[1] == seller_name and pdate(row[0]) and start_date <= pdate(row[0]) <= today]
    
    if not seller_reports:
        return {'total_sales': 0, 'shift_count': 0, 'avg_sales': 0, 'sales_by_dow': defaultdict(float), 'days_worked': []}

    total_sales = 0
    sales_by_dow = defaultdict(float)
    days_worked = []
    
    for report in seller_reports:
        sales = float(report[4].replace(',', '.'))
        report_date = pdate(report[0])
        
        total_sales += sales
        dow_name = DAYS_OF_WEEK_RU[report_date.weekday()]
        sales_by_dow[dow_name] += sales
        days_worked.append(report_date)

    shift_count = len(seller_reports)
    avg_sales = total_sales / shift_count if shift_count > 0 else 0

    return {
        'total_sales': total_sales,
        'shift_count': shift_count,
        'avg_sales': avg_sales,
        'sales_by_dow': sales_by_dow,
        'days_worked': len(days_worked)
    }

def generate_seller_stats_image(seller_name: str, stats_data: dict) -> io.BytesIO:
    """Генерирует изображение с графиком продаж по дням недели."""
    days = DAYS_OF_WEEK_RU
    sales = [stats_data['sales_by_dow'].get(day, 0) for day in days]
    
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.bar(days, sales, color='#4c72b0', alpha=0.7)
    
    ax.set_title(f'Статистика продаж для {seller_name} по дням недели', fontsize=16, pad=20)
    ax.set_ylabel('Сумма продаж, ₴', fontsize=12)
    ax.tick_params(axis='x', rotation=45, labelsize=10)
    ax.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)
    
    # Добавляем значения над столбцами
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}', ha='center', va='bottom', fontsize=10)
            
    fig.tight_layout()
    
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_seller_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню выбора продавца или запускает сравнение."""
    query = update.callback_query
    await query.answer()

    sellers_for_stats = ["Людмила", "Мария"]
    kb = []
    for seller in sellers_for_stats:
        kb.append([InlineKeyboardButton(f"📊 Статистика: {seller}", callback_data=f"view_seller_stats_{seller}")])
    
    # Новая кнопка для сравнения
    kb.append([InlineKeyboardButton("🏆 Сравнить продавцов", callback_data="compare_sellers")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="staff_menu")])
    
    # Удаляем старое сообщение с фото и присылаем новое с меню
    await query.message.delete()
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="Выберите продавца для просмотра индивидуальной статистики или запустите сравнение:",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    
async def show_seller_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Формирует и отправляет статистику с графиком для выбранного продавца."""
    query = update.callback_query
    seller_name = query.data.split('_', 3)[3]
    await query.message.edit_text("⏳ Собираю данные и рисую график...")

    stats = get_seller_stats_data(context, seller_name)
    
    if not stats or not stats['shift_count']:
        await query.message.edit_text(f"Недостаточно данных для построения статистики по продавцу {seller_name}.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="seller_stats")]]))
        return

    msg = (f"<b>📈 Статистика для {seller_name}</b> (за последние 30 дней)\n\n"
           f"<b>Всего смен:</b> {stats['shift_count']}\n"
           f"<b>Общая сумма продаж:</b> {stats['total_sales']:.2f}₴\n"
           f"<b>Средний чек за смену:</b> {stats['avg_sales']:.2f}₴\n")

    image_buffer = generate_seller_stats_image(seller_name, stats)
    
    # Удаляем сообщение "рисую график" и отправляем фото с подписью
    await query.message.delete()
    await context.bot.send_photo(
        chat_id=query.message.chat_id,
        photo=image_buffer,
        caption=msg,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="seller_stats")]])
    )

async def view_shifts_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE, year=None, month=None):
    """Показывает календарь смен для просмотра."""
    query = update.callback_query
    
    if year is None or month is None:
        today = dt.date.today()
        year, month = today.year, today.month

    # Загружаем данные о сменах
    rows = get_cached_sheet_data(context, SHEET_SHIFTS)
    shifts_data = {row[0]: [seller for seller in row[1:] if seller] for row in rows} if rows else {}
    
    kb = generate_calendar_keyboard(year, month, shifts_data, mode='view')
    await query.message.edit_text("🗓️ <b>График смен</b>\nНажмите на дату, чтобы увидеть детали.",
                                  parse_mode=ParseMode.HTML, reply_markup=kb)

# --- ДОБАВЬТЕ ЭТУ НОВУЮ ФУНКЦИЮ ---
async def show_shift_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детали смены для выбранной даты."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Формат callback_data: view_shift_ДД.ММ.ГГГГ
        date_str = query.data.split('_', 2)[2]
    except IndexError:
        await query.message.edit_text("❌ Ошибка: не удалось получить дату.")
        return

    # Используем кэш для быстрой загрузки данных
    rows = get_cached_sheet_data(context, SHEET_SHIFTS)
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения данных о сменах.")
        return
        
    sellers_on_day = []
    for row in rows:
        if row and row[0] == date_str:
            # Собираем всех продавцов из строки, убирая пустые ячейки
            sellers_on_day = [seller for seller in row[1:] if seller]
            break
            
    msg = f"🗓️ <b>Смена на {date_str}</b>\n\n"
    if not sellers_on_day:
        msg += "<i>На эту дату смена не назначена.</i>"
    else:
        msg += "<b>В этот день работают:</b>\n"
        for seller in sellers_on_day:
            msg += f"  • 👤 {seller}\n"
    
    # Клавиатура для возврата к календарю
    date_obj = pdate(date_str)
    year, month = date_obj.year, date_obj.month
    
    # Проверяем, админ ли пользователь, чтобы решить, куда возвращаться
    is_admin = str(query.from_user.id) in ADMINS
    back_callback = "edit_shifts" if is_admin else "view_shifts"
    
    kb = [[InlineKeyboardButton("🔙 К календарю", callback_data=back_callback)]]
    
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

async def edit_shifts_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE, year=None, month=None):
    """Показывает календарь для назначения/изменения смен (только для админов)."""
    query = update.callback_query
    if str(query.from_user.id) not in ADMINS:
        await query.answer("🚫 Доступ запрещен.", show_alert=True)
        return

    if year is None or month is None:
        today = dt.date.today()
        year, month = today.year, today.month
        
    rows = get_cached_sheet_data(context, SHEET_SHIFTS)
    shifts_data = {row[0]: [seller for seller in row[1:] if seller] for row in rows} if rows else {}
    
    kb = generate_calendar_keyboard(year, month, shifts_data, mode='edit')
    await query.message.edit_text("✏️ <b>Назначение смен</b>\nНажмите на дату, чтобы назначить или изменить продавцов.",
                                  parse_mode=ParseMode.HTML, reply_markup=kb)

async def edit_single_shift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню выбора продавцов для конкретной даты."""
    query = update.callback_query
    date_str = query.data.split('_', 2)[2]
    
    rows = get_cached_sheet_data(context, SHEET_SHIFTS)
    shifts_data = {row[0]: [seller for seller in row[1:] if seller] for row in rows} if rows else {}
    
    sellers_on_day = shifts_data.get(date_str, [])
    
    # Сохраняем состояние
    context.user_data['edit_shift'] = {
        'date': date_str,
        'sellers': sellers_on_day
    }

    kb = []
    for seller in SELLERS:
        icon = "✅" if seller in sellers_on_day else "❌"
        kb.append([InlineKeyboardButton(f"{icon} {seller}", callback_data=f"toggle_seller_{seller}")])
    
    kb.append([InlineKeyboardButton("💾 Сохранить смену", callback_data="save_shift")])
    kb.append([InlineKeyboardButton("🔙 К календарю", callback_data="edit_shifts")])
    
    await query.message.edit_text(f"Выберите продавцов на <b>{date_str}</b>:",
                                  parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
                                  
async def toggle_seller_for_shift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет/убирает продавца из смены."""
    query = update.callback_query
    await query.answer()
    
    seller_name = query.data.split('_', 2)[2]
    edit_state = context.user_data.get('edit_shift', {})
    
    if seller_name in edit_state['sellers']:
        edit_state['sellers'].remove(seller_name)
    else:
        # Можно добавить ограничение, например, не больше 2 продавцов
        if len(edit_state['sellers']) < 2:
            edit_state['sellers'].append(seller_name)
        else:
            await query.answer("🚫 Нельзя назначить больше двух продавцов на смену.", show_alert=True)
            return
            
    # Обновляем клавиатуру, не отправляя новое сообщение
    kb = []
    for seller in SELLERS:
        icon = "✅" if seller in edit_state['sellers'] else "❌"
        kb.append([InlineKeyboardButton(f"{icon} {seller}", callback_data=f"toggle_seller_{seller}")])
    
    kb.append([InlineKeyboardButton("💾 Сохранить смену", callback_data="save_shift")])
    kb.append([InlineKeyboardButton("🔙 К календарю", callback_data="edit_shifts")])
    
    await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(kb))

async def save_shift_changes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет изменения смены в Google Таблицу."""
    query = update.callback_query
    
    edit_state = context.user_data.get('edit_shift', {})
    date_str = edit_state.get('date')
    new_sellers = edit_state.get('sellers', [])
    
    try:
        ws = GSHEET.worksheet(SHEET_SHIFTS)
        all_rows = ws.get_all_values()
        
        found_row_index = -1
        for i, row in enumerate(all_rows):
            if row and row[0] == date_str:
                found_row_index = i + 1
                break
        
        if found_row_index != -1: # Если дата найдена, обновляем
            ws.update_cell(found_row_index, 2, new_sellers[0] if len(new_sellers) > 0 else "")
            ws.update_cell(found_row_index, 3, new_sellers[1] if len(new_sellers) > 1 else "")
        else: # Если нет, добавляем новую строку
            row_to_add = [date_str] + new_sellers + [""] * (2 - len(new_sellers))
            ws.append_row(row_to_add)
        
        # Сбрасываем кэш
        if 'sheets_cache' in context.bot_data:
            context.bot_data['sheets_cache'].pop(SHEET_SHIFTS, None)
            
        await query.answer("✅ Смена сохранена!", show_alert=True)
        context.user_data.pop('edit_shift', None)
        
        # Возвращаемся к календарю
        await edit_shifts_calendar(update, context)
        
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка сохранения смены: {e}")


async def handle_report_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cash = float(update.message.text.replace(',', '.'))
        context.user_data['report']['cash'] = cash
        context.user_data['report']['step'] = 'terminal'
        
        # Создаем клавиатуру с кнопкой отмены - ИСПРАВЛЕННЫЙ ВАРИАНТ
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔙 Назад", callback_data="back_to_cash_input"),
                InlineKeyboardButton("❌ Отменить отчет", callback_data="cancel_report")
            ]
        ])
        
        await update.message.reply_text(
            "💳 Введите сумму по терминалу:",
            reply_markup=keyboard  # Передаем клавиатуру здесь
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы. Введите число:")

async def handle_report_terminal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        terminal = float(update.message.text.replace(',', '.'))
        context.user_data['report']['terminal'] = terminal
        context.user_data['report']['step'] = 'expenses_ask'
        
        kb = [
            [InlineKeyboardButton("✅ Да", callback_data="exp_yes")],
            [InlineKeyboardButton("❌ Нет", callback_data="exp_no")],# <-- Добавить назад
            [InlineKeyboardButton("❌ Отменить отчет", callback_data="cancel_report")]     # <-- Добавить отмену
        ]
        
        await update.message.reply_text(
            "💸 Были ли расходы во время смены?",
            reply_markup=InlineKeyboardMarkup(kb))
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы. Введите число:")

async def cancel_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет процесс сдачи отчета и возвращает в главное меню"""
    query = update.callback_query
    await query.answer("Сдача отчета отменена")
    
    # Очищаем состояние отчета
    if 'report' in context.user_data:
        del context.user_data['report']
    
    # Возвращаем в главное меню
    is_admin = str(query.from_user.id) in ADMINS
    await query.message.edit_text(
        "❌ Сдача отчета отменена",
        reply_markup=main_kb(is_admin)
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущую операцию"""
    # Очищаем все состояния
    for key in ['report', 'supplier', 'expense', 'inventory_expense']:
        if key in context.user_data:
            del context.user_data[key]
    
    is_admin = str(update.effective_user.id) in ADMINS
    await update.message.reply_text(
        "❌ Текущая операция отменена",
        reply_markup=main_kb(is_admin)
    )

# Добавьте в прилож

async def handle_report_expenses_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "exp_yes":
        context.user_data['report']['step'] = 'expenses'
        context.user_data['report']['expenses'] = []
        await query.message.edit_text(
            "💸 Введите сумму расхода:",
            reply_markup=back_kb()
        )
    else:
        # Если расходов нет, сразу переходим к комментарию
        context.user_data['report']['step'] = 'comment'
        await query.message.edit_text(
            "📝 Добавьте комментарий к отчету (или нажмите 'Пропустить'):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_comment")],
                [InlineKeyboardButton("🔙 Назад", callback_data="add_report")]
            ])
        )
async def handle_report_expenses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(',', '.'))
        context.user_data['report']['expenses'].append({'amount': amount})
        context.user_data['report']['step'] = 'expense_comment'
        await update.message.reply_text(
            "📝 Введите комментарий к расходу:",
            reply_markup=back_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы. Введите число:")

async def debug_get_planning_details(report_date: dt.date):
    """Временная функция для детальной диагностики чтения листа ПланФактНаЗавтра."""
    report_date_str = sdate(report_date)
    debug_log = [f"\n\n<b>--- ДИАГНОСТИКА ПЛАНОВ НА {report_date_str} ---</b>"]
    
    try:
        ws = GSHEET.worksheet(SHEET_PLAN_FACT)
        debug_log.append(f"✅ Лист '{SHEET_PLAN_FACT}' успешно открыт.")
        
        rows = ws.get_all_values()[1:] # Пропускаем заголовок
        debug_log.append(f"ℹ️ Найдено всего строк в листе: {len(rows)}.")
        
        if not rows:
            debug_log.append("❌ Лист пуст или не удалось прочитать строки.")
            return "\n".join(debug_log)

        found_match = False
        # Проверяем первые 15 строк, чтобы не засорять отчет
        for i, row in enumerate(rows[:15]):
            if not row or not row[0]:
                debug_log.append(f"  - Строка {i+1}: пустая.")
                continue

            sheet_date_str = row[0].strip()
            # Используем repr() чтобы увидеть скрытые символы, если они есть
            debug_log.append(f"  - Строка {i+1}: в ячейке A записано: <code>{repr(row[0])}</code>")
            
            # Сравниваем
            is_match = (sheet_date_str == report_date_str)
            if is_match:
                found_match = True
                debug_log.append(f"    <b>✅ СОВПАДЕНИЕ!</b> ( {sheet_date_str} == {report_date_str} )")
            else:
                debug_log.append(f"    <b>❌ НЕТ совпадения.</b> ( {sheet_date_str} != {report_date_str} )")
        
        if len(rows) > 15:
            debug_log.append("  ...")
            
        if found_match:
            debug_log.append("<b>Вывод: Совпадение найдено, но данные могут не парситься. Проверьте формат суммы.</b>")
        else:
            debug_log.append("<b>Вывод: Совпадений по дате не найдено. Проверьте формат даты в таблице.</b>")

    except Exception as e:
        debug_log.append(f"💥 КРИТИЧЕСКАЯ ОШИБКА при чтении листа: {e}")
        
    return "\n".join(debug_log)

async def handle_expense_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text
    expenses = context.user_data['report']['expenses']
    expenses[-1]['comment'] = comment
    context.user_data['report']['step'] = 'expense_more'
    
    kb = [
        [InlineKeyboardButton("✅ Да", callback_data="more_yes")],
        [InlineKeyboardButton("❌ Нет", callback_data="more_no")],
        [InlineKeyboardButton("🔙 Назад", callback_data="add_report")]
    ]
    
    await update.message.reply_text(
        "💸 Добавить еще один расход?",
        reply_markup=InlineKeyboardMarkup(kb))

async def handle_expense_more(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "more_yes":
        context.user_data['report']['step'] = 'expenses'
        await query.message.edit_text(
            "💸 Введите сумму расхода:",
            reply_markup=back_kb()
        )
    else:
        # После всех расходов переходим к комментарию
        context.user_data['report']['step'] = 'comment'
        await query.message.edit_text(
            "📝 Добавьте комментарий к отчету (или нажмите 'Пропустить'):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_comment")],
                [InlineKeyboardButton("🔙 Назад", callback_data="add_report")]
            ])
        )

async def show_today_invoices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список всех накладных, добавленных за сегодня."""
    query = update.callback_query
    await query.answer()
    today_str = sdate()
    
    # Используем кэш для быстрой загрузки
    rows = get_cached_sheet_data(context, SHEET_SUPPLIERS)
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения данных о поставщиках.")
        return
        
    # Отбираем только те накладные, что были добавлены сегодня
    today_invoices = [row for row in rows if len(row) > 6 and row[0].strip() == today_str]
    
    msg = f"📄 <b>Накладные, добавленные сегодня ({today_str}):</b>\n"
    if not today_invoices:
        msg += "\n<i>За сегодня еще не было добавлено ни одной накладной.</i>"
    else:
        for invoice in today_invoices:
            # Безопасно извлекаем данные
            supplier = invoice[1] if len(invoice) > 1 else "Не указан"
            to_pay_str = invoice[4] if len(invoice) > 4 else "0"
            pay_type = invoice[6] if len(invoice) > 6 else "Не указан"
            
            try:
                to_pay = float(to_pay_str.replace(',', '.'))
            except (ValueError, TypeError):
                to_pay = 0.0

            msg += "\n──────────────────\n"
            msg += f"<b>{supplier}</b>\n"
            msg += f"  • Сумма к оплате: {to_pay:.2f}₴\n"
            msg += f"  • Тип оплаты: {pay_type}"

    kb = [[InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")]]
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    
async def save_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        # Если пропустили комментарий, устанавливаем его пустым
        if 'report' not in context.user_data: context.user_data['report'] = {}
        context.user_data['report']['comment'] = ""
    else:
        context.user_data['report']['comment'] = update.message.text

    report_data = context.user_data['report']
    today_str = sdate()
    current_date = pdate(today_str)
    tomorrow_date = current_date + dt.timedelta(days=1)
    
    # 1. Очищаем планы, которые были составлены НА СЕГОДНЯ
    clear_plan_for_date(today_str)

    # 2. Собираем данные из отчета
    cash = report_data['cash']
    terminal = report_data['terminal']
    total_sales = cash + terminal
    seller = report_data['seller']
    comment = report_data.get('comment', '')
    
    # 3. Записываем расходы в таблицу РАСХОДЫ
    expenses_total = sum(exp['amount'] for exp in report_data.get('expenses', []))
    if 'expenses' in report_data and report_data['expenses']:
        ws_exp = GSHEET.worksheet(SHEET_EXPENSES)
        for exp in report_data['expenses']:
            # Расходы вычитаются из кассы, поэтому не трогаем сейф напрямую
            ws_exp.append_row([today_str, exp['amount'], exp.get('comment', ''), seller])

    # 4. Проводим операции с СЕЙФОМ
    balance_before_shift = get_safe_balance(context)
    cash_balance = cash - expenses_total
    add_safe_operation("Пополнение", cash_balance, "Остаток кассы за день", seller)
    add_inventory_operation("Продажа", total_sales, "Продажа товаров за смену", seller)
    
    # Вычитаем ставку ЗП из сейфа
    if seller in ["Мария", "Людмила"]:
        add_safe_operation("Зарплата", 700, f"Ставка за смену для {seller}", seller)
        add_salary_record(seller, "Ставка", 700, "Выплачено из сейфа")
        if total_sales > 35000:
            bonus = round((total_sales * 0.02) - 700, 2)
            if bonus > 0:
                add_salary_record(seller, "Премия 2%", bonus, f"За {today_str} (продажи: {total_sales:.2f}₴)")

    if 'sheets_cache' in context.bot_data and "Сейф" in context.bot_data['sheets_cache']:
        del context.bot_data['sheets_cache']["Сейф"]


    # Фиксируем итоговый баланс сейфа
    safe_bal_after_shift = get_safe_balance(context)

    # 5. Получаем данные для отчета НА ЗАВТРА
    total_debts, suppliers_debts = get_debts_for_date(context, tomorrow_date)
    planning_report, planned_cash, planned_card, planned_total = get_planning_details_for_date(context, tomorrow_date)
    
    # 6. Записываем итоговую строку в ДНЕВНЫЕ ОТЧЕТЫ
    ws_report = GSHEET.worksheet(SHEET_REPORT)
    report_row_data = [
        today_str, seller, cash, terminal, total_sales, 
        cash_balance, total_debts, planned_total, comment, safe_bal_after_shift
    ]
    ws_report.append_row(report_row_data)

    # 7. Формируем финальное сообщение
    resp = (f"✅ <b>Смена полностью завершена!</b>\n\n"
            f"📅 Дата: {today_str}\n"
            f"👤 Продавец: {seller}\n"
            f"💵 Наличные: {cash:.2f}₴\n"
            f"💳 Карта: {terminal:.2f}₴\n"
            f"💰 Общая сумма: {total_sales:.2f}₴\n"
            f"💸 Расходы: {expenses_total:.2f}₴\n"
            f"🏦 Остаток кассы: {cash_balance:.2f}₴\n"
            f"\n<b>--- Расчет сейфа ---</b>\n"
            f"• Было в сейфе: {balance_before_shift:.2f}₴\n"
            f"• Остаток кассы: +{cash_balance:.2f}₴\n"
            f"• Зарплата (ставка): -700.00₴\n"
            f"• <b>Стало в сейфе: {safe_bal_after_shift:.2f}₴</b>\n")
    
    if not planning_report and not suppliers_debts:
         resp += f"\n\nℹ️ *Нет планов или долгов на {sdate(tomorrow_date)}.*"
    else:
        if planning_report: resp += planning_report
        if suppliers_debts:
            resp += "\n\n<b>🗓 Долги к оплате на завтра:</b>\n" + "\n".join([f"- {n}: {a:.2f}₴" for n, a in suppliers_debts])
    
    total_needed_cash = total_debts + planned_cash
    total_needed_card = planned_card
    
    resp += "\n"
    if total_needed_cash > 0: resp += f"\n<b>ИТОГО на завтра наличными: {total_needed_cash:.2f}₴</b>"
    if total_needed_card > 0: resp += f"\n<b>ИТОГО на завтра картой: {total_needed_card:.2f}₴</b>"
    
    kb = [[
        InlineKeyboardButton("💸 Детально расходы", callback_data=f"details_exp_{today_str}_{today_str}"),
        InlineKeyboardButton("📦 Детально накладные", callback_data=f"details_sup_{today_str}_{today_str}_0")
    ], [InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")]]
    markup = InlineKeyboardMarkup(kb)

    if update.callback_query:
        await update.callback_query.message.edit_text(resp, parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
        await update.message.reply_text(resp, parse_mode=ParseMode.HTML, reply_markup=markup)
    context.user_data.pop('report', None)

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def generate_daily_report_text(context: ContextTypes.DEFAULT_TYPE, report_date_str: str):
    """Готовит текст детального отчета, правильно читая 10 столбцов."""
    reports = get_cached_sheet_data(context, SHEET_REPORT)
    if reports is None: return "❌ Ошибка чтения отчетов."
    
    daily_report_row = next((row for row in reports if row and row[0].strip() == report_date_str), None)
    if not daily_report_row: return f"❌ Отчет за дату {report_date_str} не найден."

    try:
        # ИСПРАВЛЕНИЕ: Распаковываем 10 столбцов, как в таблице
        if len(daily_report_row) < 10:
            raise IndexError("Недостаточно столбцов в строке отчета для полной детализации.")
            
        date, seller, cash_s, term_s, total_s, cash_bal_s, _, _, comment, safe_bal_s = daily_report_row[:10]
        cash, terminal, total_sales, safe_balance = map(float, [v.replace(',', '.') for v in [cash_s, term_s, total_s, safe_bal_s]])
    except (ValueError, IndexError) as e:
        return f"❌ Ошибка данных в отчете за {report_date_str}: {e}"
    
    expenses = get_cached_sheet_data(context, SHEET_EXPENSES)
    expenses_total = sum(float(row[1].replace(',', '.')) for row in expenses if row and row[0].strip() == date and len(row) > 1 and row[1]) if expenses else 0

    resp = (f"📖 <b>Детальный отчет за {date}</b>\n\n"
            f"👤 Продавец: {seller}\n"
            f"💵 Наличные: {cash:.2f}₴\n"
            f"💳 Карта: {terminal:.2f}₴\n"
            f"💰 Общая сумма: {total_sales:.2f}₴\n"
            f"💸 Расходы: {expenses_total:.2f}₴\n"
            f"💼 <b>Остаток в сейфе (на конец того дня): {safe_balance:.2f}₴</b>")
    if comment: resp += f"\n📝 Комментарий: {comment}"
    
    report_date = pdate(date)
    if report_date:
        next_day = report_date + dt.timedelta(days=1)
        total_debts, suppliers_debts = get_debts_for_date(context, next_day)
        planning_report, planned_cash, planned_card, _ = get_planning_details_for_date(context, next_day)

        if not planning_report and not suppliers_debts:
             resp += f"\n\nℹ️ *Нет планов или долгов на {sdate(next_day)}.*"
        else:
            if planning_report: resp += planning_report
            if suppliers_debts:
                resp += "\n\n<b>🗓 Долги к оплате на след. день:</b>\n" + "\n".join([f"- {n}: {a:.2f}₴" for n, a in suppliers_debts])
            
            total_needed_cash = total_debts + planned_cash
            total_needed_card = planned_card
            
            resp += "\n"
            if total_needed_cash > 0: resp += f"\n<b>ИТОГО на след. день наличными: {total_needed_cash:.2f}₴</b>"
            if total_needed_card > 0: resp += f"\n<b>ИТОГО на след. день картой: {total_needed_card:.2f}₴</b>"
            
    return resp
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def show_detailed_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает страничный детальный отчет с исправленной навигацией и проверкой дат."""
    query = update.callback_query
    await query.answer()

    try:
        _, _, _, start_str, end_str, index_str = query.data.split('_')
        current_index = int(index_str)
    except (IndexError, ValueError):
        await query.message.edit_text("❌ Ошибка в данных навигации. Не удалось загрузить отчет.")
        return

    start_date, end_date = pdate(start_str), pdate(end_str)
    
    report_rows = get_cached_sheet_data(context, SHEET_REPORT)
    if report_rows is None:
        await query.message.edit_text("❌ Ошибка чтения отчетов из кэша.")
        return

    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Добавляем проверку `if pdate(row[0])` ---
    # Это гарантирует, что мы работаем только со строками, где есть корректная дата
    period_report_dates = sorted(
        list({row[0].strip() for row in report_rows if pdate(row[0]) and start_date <= pdate(row[0]) <= end_date}),
        key=pdate, 
        reverse=True
    )

    if not period_report_dates or current_index >= len(period_report_dates):
        await query.message.edit_text("❌ В этом периоде нет отчетов для детального просмотра.")
        return

    target_date_str = period_report_dates[current_index]
    # Передаем context дальше, чтобы generate_daily_report_text тоже мог использовать кэш
    report_text = await generate_daily_report_text(context, target_date_str)

    # Формирование кнопок (логика без изменений)
    nav_buttons = []
    if current_index < len(period_report_dates) - 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"detail_report_nav_{start_str}_{end_str}_{current_index + 1}"))
    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"detail_report_nav_{start_str}_{end_str}_{current_index - 1}"))
    
    full_nav_context = f"{target_date_str}_{start_str}_{end_str}_{current_index}"
    kb = []
    if nav_buttons:
        kb.append(nav_buttons)
    
    kb.append([
        InlineKeyboardButton("💸 Расходы за день", callback_data=f"details_exp_{full_nav_context}"),
        InlineKeyboardButton("📦 Накладные за день", callback_data=f"details_sup_{full_nav_context}_0")
    ])
    
    back_callback = f"report_week_{start_str}_{end_str}" if (end_date - start_date).days <= 7 else f"report_month_{start_str}_{end_str}"
    kb.append([InlineKeyboardButton("⬅️ К общему отчету", callback_data=back_callback)])
    
    await query.message.edit_text(report_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
# --- ЗАМЕНИТЕ ЭТИ ДВЕ ФУНКЦИИ ---

async def get_report_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    today_str = sdate(dt.date.today())
    
    # ИСПРАВЛЕНИЕ: Передаем context и report_date_str
    report_text = await generate_daily_report_text(context, today_str)
    
    kb = [[
        InlineKeyboardButton("💸 Детально расходы", callback_data=f"details_exp_{today_str}_{today_str}"),
        InlineKeyboardButton("📦 Детально накладные", callback_data=f"details_sup_{today_str}_{today_str}_0")
    ], [InlineKeyboardButton("🔙 К выбору периода", callback_data="view_reports_menu")]]
    
    await query.message.edit_text(
        report_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def get_report_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    yesterday = dt.date.today() - dt.timedelta(days=1)
    yesterday_str = sdate(yesterday)

    # ИСПРАВЛЕНИЕ: Передаем context и report_date_str
    report_text = await generate_daily_report_text(context, yesterday_str)
    
    kb = [[
        InlineKeyboardButton("💸 Детально расходы", callback_data=f"details_exp_{yesterday_str}_{yesterday_str}"),
        InlineKeyboardButton("📦 Детально накладные", callback_data=f"details_sup_{yesterday_str}_{yesterday_str}_0")
    ], [InlineKeyboardButton("🔙 К выбору периода", callback_data="view_reports_menu")]]
    
    await query.message.edit_text(
        report_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(kb)
    )
async def choose_details_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Формирует меню выбора даты для просмотра деталей (расходов или накладных)."""
    query = update.callback_query
    await query.answer()

    try:
        # Формат: choose_date_ТИП_ДАТА-СТАРТ_ДАТА-КОНЕЦ
        _, _, detail_type, start_str, end_str = query.data.split('_')
    except ValueError:
        await query.message.edit_text("❌ Ошибка навигации. Неверный формат callback.")
        return

    start_date = pdate(start_str)
    end_date = pdate(end_str)
    
    msg = f"📅 <b>Выберите дату</b> для просмотра деталей за период\n{start_str} — {end_str}\n\n"
    kb = []
    
    # Создаем кнопки для каждой даты в периоде
    current_date = start_date
    while current_date <= end_date:
        date_str = sdate(current_date)
        # Добавляем _0 для старта пагинации с первого элемента
        callback = f"details_{detail_type}_{date_str}_{start_str}_{end_str}_0"
        kb.append([InlineKeyboardButton(date_str, callback_data=callback)])
        current_date += dt.timedelta(days=1)

    # Если это расходы, показываем общую сводку за период
    if detail_type == 'exp':
        ws_exp = GSHEET.worksheet(SHEET_EXPENSES)
        rows = ws_exp.get_all_values()[1:]
        total_exp = 0
        for row in rows:
            try:
                d = pdate(row[0])
                if start_date <= d <= end_date and row[1]:
                    total_exp += float(row[1].replace(',', '.'))
            except (ValueError, IndexError):
                continue
        msg += f"<b>Общая сумма расходов за период: {total_exp:.2f}₴</b>\n"

    kb.append([InlineKeyboardButton("🔙 Назад к общему отчету", callback_data=f"report_week_{start_str}_{end_str}")])
    await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    

# --- РАСХОДЫ ---
async def start_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['expense'] = {'step': 'value'}
    await update.message.reply_text(
        "📝 Введите комментарий к расходу:",
        reply_markup=back_kb()
    )


async def handle_expense_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(',', '.'))
        context.user_data['expense']['amount'] = amount
        context.user_data['expense']['step'] = 'comment'
        await update.message.reply_text(
            "📝 Введите комментарий к расходу:",
            reply_markup=back_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы. Введите число:")

async def save_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text
    amount = context.user_data['expense']['amount']
    # Сохранение в Google Sheets
    try:
        ws = GSHEET.worksheet(SHEET_EXPENSES)
        ws.append_row([sdate(), amount, comment, update.effective_user.first_name])
        await update.message.reply_text("✅ Расход успешно сохранен!")
        context.user_data.pop('expense', None)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

# --- ПОСТАВЩИКИ ---
# --- START ADD SUPPLIER ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
async def start_supplier(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс добавления накладной, скрывая уже добавленных поставщиков."""
    query = update.callback_query
    await query.answer()

    today_str = sdate()
    day_of_week = DAYS_OF_WEEK_RU[dt.date.today().weekday()]

    try:
        # 1. Получаем всех поставщиков по графику на сегодня
        scheduled_suppliers = get_suppliers_for_day(day_of_week)
        
        # 2. Получаем всех поставщиков, по которым УЖЕ есть накладные за сегодня
        ws_sup = GSHEET.worksheet(SHEET_SUPPLIERS)
        rows = ws_sup.get_all_values()[1:]
        added_today_suppliers = {row[1].strip() for row in rows if len(row) > 1 and row[0].strip() == today_str}

        # 3. Оставляем только тех, кого еще не добавляли
        suppliers_to_show = [s for s in scheduled_suppliers if s not in added_today_suppliers]

    except Exception as e:
        logging.error(f"Не удалось получить списки поставщиков: {e}")
        suppliers_to_show = []

    kb = []
    for supplier in suppliers_to_show:
        kb.append([InlineKeyboardButton(f"🚚 {supplier}", callback_data=f"add_sup_{supplier}")])
    
    kb.append([InlineKeyboardButton("📝 Другой (не по графику)", callback_data="add_sup_other")])
    kb.append([InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")])

    await query.message.edit_text(
        "📦 <b>Добавление накладной</b>\n\nВыберите поставщика из списка:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.HTML
    )


# --- ДОБАВЬТЕ ЭТУ НОВУЮ ФУНКЦИЮ ---
# --- ДОБАВЬТЕ ЭТУ НОВУЮ ФУНКЦИЮ ---
async def inventory_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает последние 15 операций с остатком магазина."""
    query = update.callback_query
    await query.answer()
    
    rows = get_cached_sheet_data(context, "Остаток магазина")
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения истории остатка магазина.")
        return

    # Берем последние 15 операций
    last_ops = rows[-15:]
    last_ops.reverse() # Новые сверху
    
    text = "📦 <b>Последние 15 операций с остатком магазина:</b>\n"
    if not last_ops:
        text += "\n<i>Операций еще не было.</i>"
    else:
        for row in last_ops:
            # Безопасно извлекаем данные
            date, op_type, amount, comment, user = (row + ["", "", "", "", ""])[:5]
            
            # Подбираем иконку в зависимости от типа операции
            icon = "⚙️" # Иконка по умолчанию
            if op_type == "Приход":
                icon = "🟢"
            elif op_type in ["Продажа", "Списание"]:
                icon = "🔴"
            elif op_type == "Переучет":
                icon = "🔵"
            
            # Для переучета сумма может быть в другом месте или отсутствовать, делаем красиво
            amount_text = f"{amount}₴" if amount else ""

            text += "\n──────────────────\n"
            text += f"{icon} <b>{op_type}: {amount_text}</b> ({user})\n"
            text += f"   <i>{date} - {comment}</i>"
        
    await query.message.edit_text(text, parse_mode='HTML', reply_markup=stock_safe_kb())


    
# --- ДОБАВЬТЕ ЭТУ НОВУЮ ФУНКЦИЮ ---
async def handle_add_supplier_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор поставщика при добавлении накладной."""
    query = update.callback_query
    await query.answer()
    
    supplier_name = query.data.split('_', 2)[2]
    
    # Если нажали "Другой"
    if supplier_name == "other":
        context.user_data['supplier'] = {'step': 'name'}
        await query.message.edit_text(
            "📦 Введите имя поставщика:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]])
        )
    # Если выбрали из списка
    else:
        context.user_data['supplier'] = {'name': supplier_name, 'step': 'amount_income'}
        await query.message.edit_text(
            f"💰 Введите сумму прихода по накладной для <b>{supplier_name}</b>:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]]),
            parse_mode=ParseMode.HTML
        )


async def handle_supplier_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['supplier']['name'] = update.message.text
    context.user_data['supplier']['step'] = 'amount_income'
    await update.message.reply_text(
        "💰 Введите сумму прихода (по накладной):",
        reply_markup=back_kb()
    )

# 2. Сумма прихода
async def handle_supplier_amount_income(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount_income = float(update.message.text.replace(',', '.'))
        context.user_data['supplier']['amount_income'] = amount_income
        context.user_data['supplier']['step'] = 'writeoff'
        await update.message.reply_text(
            "↩️ Введите сумму возврата/списания (если нет — 0):",
            reply_markup=back_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Введите сумму числом!")

# 3. Возврат/Списание
async def handle_supplier_writeoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        writeoff = float(update.message.text.replace(',', '.'))
        context.user_data['supplier']['writeoff'] = writeoff
        context.user_data['supplier']['step'] = 'invoice_total_markup'
        await update.message.reply_text(
            "📑 Введите сумму накладной после наценки (итоговая сумма, которая должна добавиться в остаток магазина):",
            reply_markup=back_kb()
        )
    except ValueError:
        await update.message.reply_text("❌ Введите сумму числом!")

# 4. Сумма накладной после наценки
async def handle_supplier_invoice_total_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        invoice_total = float(update.message.text.replace(',', '.'))
        context.user_data['supplier']['invoice_total_markup'] = invoice_total
        context.user_data['supplier']['step'] = 'payment_type'
        kb = [
            [InlineKeyboardButton("💵 Наличные", callback_data="pay_Наличные")],
            [InlineKeyboardButton("💳 Карта", callback_data="pay_Карта")],
            [InlineKeyboardButton("📆 Долг", callback_data="pay_Долг")],
            [InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]
        ]
        await update.message.reply_text(
            "💳 Выберите тип оплаты:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except ValueError:
        await update.message.reply_text("❌ Введите сумму числом!")


# 5. Тип оплаты (callback)
async def handle_supplier_pay_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pay_type = query.data.split('_', 1)[1] # Теперь здесь будет "Наличные", "Карта" или "Долг"
    context.user_data['supplier']['payment_type'] = pay_type

    # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Проверяем по-русски ---
    if pay_type == "Долг":
        context.user_data['supplier']['step'] = 'due_date'
        await query.message.edit_text(
            "📅 Введите дату погашения долга (ДД.ММ.ГГГГ):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]])
        )
    else:
        context.user_data['supplier']['step'] = 'comment'
        await query.message.edit_text(
            "📝 Добавьте комментарий (или нажмите 'Пропустить'):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_comment_supplier")],
                [InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]
            ])
        )
# 6. Срок долга
async def handle_supplier_due_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        due_date = pdate(update.message.text)
        context.user_data['supplier']['due_date'] = due_date
        context.user_data['supplier']['step'] = 'comment'
        await update.message.reply_text(
            "📝 Добавьте комментарий (или нажмите 'Пропустить'):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_comment_supplier")],
                [InlineKeyboardButton("🔙 Назад", callback_data="add_supplier")]
            ])
        )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ")

async def view_debts_history(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    query = update.callback_query
    await query.answer()
    
    context.user_data['debts_history_page'] = page
    rows = get_cached_sheet_data(context, SHEET_DEBTS)
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения истории долгов.")
        return
        
    per_page = 10
    total = len(rows)
    total_pages = max(1, math.ceil(total / per_page))

    # Новые сверху:
    rows = rows[::-1]
    page_rows = rows[page * per_page : (page + 1) * per_page]

    if not page_rows:
        await query.message.edit_text("История долгов пуста.", reply_markup=debts_menu_kb())
        return

    msg = f"<b>📜 История долгов (стр. {page+1}/{total_pages}):</b>\n\n"
    for idx, row in enumerate(page_rows, 1 + page * per_page):
        status = "✅" if row[6].strip().lower() == "да" else "🟠"
        msg += (
            f"{idx}. {status} <b>{row[1]}</b>\n"
            f"   • Дата: {row[0]}\n"
            f"   • Сумма: <b>{parse_float(row[2]):.2f}₴</b>\n"
            f"   • Оплачено: {parse_float(row[3]):.2f}₴ \n"
            f"   • Срок: {row[5]} | Погашено: {row[6]}\n"
            "─────────────\n"
        )

    # Кнопки Вперёд/Назад
    kb = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Назад", callback_data="debts_history_prev"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("➡️ Вперёд", callback_data="debts_history_next"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("🔙 Долги", callback_data="debts_menu")])

    await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    context.user_data['debts_history_page'] = page

    
# 7. Сохранение (после комментария или "пропустить")
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def save_supplier(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет накладную и корректно проводит все финансовые операции."""
    query = update.callback_query
    message = query.message if query else update.message
    
    if query:
        await query.answer()
    
    # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ: Проверяем, существуют ли данные перед их использованием ---
    if 'supplier' not in context.user_data:
        await message.edit_text(
            "❌ Ошибка: сессия добавления накладной утеряна. Пожалуйста, начните заново.", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")]])
        )
        return

    supplier_data = context.user_data['supplier']

    # Устанавливаем комментарий, если его пропустили или ввели текстом
    if query and query.data == "skip_comment_supplier":
        supplier_data['comment'] = ""
    elif not query:
        supplier_data['comment'] = update.message.text

    # Проверяем, что все ключевые данные на месте
    required_keys = ['name', 'amount_income', 'writeoff', 'invoice_total_markup', 'payment_type']
    if not all(key in supplier_data for key in required_keys):
        await message.reply_text("❌ Ошибка: не все данные накладной были введены. Пожалуйста, начните заново.", reply_markup=suppliers_menu_kb())
        context.user_data.pop('supplier', None)
        return

    pay_type = supplier_data['payment_type']
    who = update.effective_user.first_name

    amount_income = float(supplier_data['amount_income'])
    amount_writeoff = float(supplier_data.get('writeoff', 0))
    invoice_total_markup = float(supplier_data['invoice_total_markup'])
    sum_to_pay = amount_income - amount_writeoff
    
    paid_status = "Нет"
    debt_amount = 0
    due_date = ""

    if pay_type == "Долг":
        debt_amount = sum_to_pay
        due_date_obj = supplier_data.get('due_date')
        due_date = sdate(due_date_obj) if due_date_obj else ""
    else:
        paid_status = f"Да ({sum_to_pay:.2f})"
        if pay_type == "Наличные":
            try:
                comment_for_safe = f"Оплата поставщику: {supplier_data['name']} ({pay_type})"
                add_safe_operation("Расход", sum_to_pay, comment_for_safe, who)
            except Exception as e:
                logging.error(f"Ошибка при списании оплаты поставщику из сейфа: {e}")
            
    row_to_save = [
        sdate(), supplier_data['name'], amount_income, amount_writeoff, sum_to_pay,
        invoice_total_markup, pay_type, paid_status, debt_amount, due_date, 
        supplier_data.get('comment', ''), who, ""
    ]
    
    try:
        ws_sup = GSHEET.worksheet(SHEET_SUPPLIERS)
        ws_sup.append_row(row_to_save)

        if pay_type == "Долг":
            ws_debts = GSHEET.worksheet(SHEET_DEBTS)
            ws_debts.append_row([sdate(), supplier_data['name'], sum_to_pay, 0, sum_to_pay, due_date, "Нет", "Наличные"])

        add_inventory_operation("Приход", invoice_total_markup, f"Поставщик: {supplier_data['name']}", who)

        # Автоматическая отметка в журнале прибытия
        try:
            today_str = sdate()
            supplier_name_to_check = supplier_data['name']
            if supplier_name_to_check:
                ws_plan = GSHEET.worksheet(SHEET_PLAN_FACT)
                plan_rows = get_cached_sheet_data(context, SHEET_PLAN_FACT, force_update=True)
                for i, plan_row in enumerate(plan_rows, start=2):
                    if len(plan_row) > 5 and plan_row[0] == today_str and plan_row[1] == supplier_name_to_check and plan_row[5] != "Прибыл":
                        ws_plan.update_cell(i, 6, "Прибыл")
                        logging.info(f"Автоматически обновлен статус на 'Прибыл' для '{supplier_name_to_check}'")
                        break
        except Exception as e:
            logging.error(f"Ошибка автоматической отметки статуса в журнале: {e}")

        msg = (
            "✅ Накладная успешно добавлена!\n\n"
            f"📦 Поставщик: {supplier_data['name']}\n"
            f"📥 Приход: {amount_income:.2f}₴\n"
            f"↩️ Возврат/Списание: {amount_writeoff:.2f}₴\n"
            f"💸 К оплате: <b>{sum_to_pay:.2f}₴</b>\n"
            f"💰 В остаток магазина: {invoice_total_markup:.2f}₴\n"
            f"💳 Тип оплаты: {pay_type}\n"
        )
        if pay_type == 'Долг':
            msg += f"📅 Срок долга: {due_date}"

        kb = [[InlineKeyboardButton("🔙 В меню поставщиков", callback_data="suppliers_menu")]]
        
        if query:
            await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
        else:
            await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
        
        context.user_data.pop('supplier', None)

    except Exception as e:
        error_msg = f"❌ Ошибка сохранения поставщика: {str(e)}"
        if query:
            await query.message.edit_text(error_msg)
        else:
            await update.message.reply_text(error_msg)
            
async def add_shift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['shift'] = {'step': 'date'}
    await query.message.edit_text(
        "📅 Введите дату смены (ДД.ММ.ГГГГ):",
        reply_markup=back_kb()
    )

# Изменяем show_expenses_detail
async def show_expenses_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    report_date_str = parts[2]
    try:
        report_date = pdate(report_date_str)
    except Exception:
        await query.message.edit_text("❌ Ошибка формата даты в навигации.")
        return

    ws_exp = GSHEET.worksheet(SHEET_EXPENSES)
    rows = ws_exp.get_all_values()[1:]
    exp_list = [row for row in rows if len(row) >= 2 and pdate(row[0].strip()) == report_date]

    if not exp_list:
        msg = "💸 За этот день расходов не найдено."
    else:
        msg = f"<b>💸 Расходы за {report_date_str}:</b>\n\n"
        for row in exp_list:
            amount = float(row[1].replace(',', '.'))
            comment = row[2] if len(row) > 2 else ''
            seller = row[3] if len(row) > 3 else ''
            msg += f"<b>{amount:.2f}₴</b>"
            if comment: msg += f" — {comment}"
            if seller: msg += f" ({seller})"
            msg += "\n"
    
    # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Всегда полный набор кнопок ---
    kb = []
    # Если мы пришли из детального отчета с навигацией (длинный callback)
    if len(parts) > 5:
        start_date_str, end_date_str, index = parts[3], parts[4], parts[5]
        full_nav_context = f"{report_date_str}_{start_date_str}_{end_date_str}_{index}"
        
        # Добавляем _0 для старта просмотра накладных с первой
        kb.append([InlineKeyboardButton("📦 Накладные за день", callback_data=f"details_sup_{full_nav_context}_0")])
        kb.append([InlineKeyboardButton("⬅️ К детальному отчету", callback_data=f"detail_report_nav_{start_date_str}_{end_date_str}_{index}")])
    else: # Если пришли из отчета о сдаче смены (короткий callback)
         kb.append([InlineKeyboardButton("📦 Накладные за день", callback_data=f"details_sup_{report_date_str}_{report_date_str}_0")])
         kb.append([InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")])
        
    await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    
# Аналогично show_suppliers_detail — чуть ниже будет обновлён!

async def show_suppliers_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Эта функция-переходник. Она получает старую команду `details_sup_...` 
    и вызывает новый обработчик `show_invoices_list`, который ее поймет.
    """
    query = update.callback_query
    # Просто вызываем новый обработчик, он сам разберет callback_data
    # Мы больше не пытаемся менять query.data, что и вызывало ошибку
    await show_invoices_list(update, context)
    
async def handle_shift_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    seller = query.data.split('_')[2]
    
    if seller in context.user_data['shift']['selected_sellers']:
        context.user_data['shift']['selected_sellers'].remove(seller)
    else:
        context.user_data['shift']['selected_sellers'].append(seller)
    
    # Обновляем сообщение
    selected = context.user_data['shift']['selected_sellers']
    text = f"👥 Выбранные продавцы: {', '.join(selected) if selected else 'нет'}"
    
    kb = [[InlineKeyboardButton(seller, callback_data=f"shift_seller_{seller}")] for seller in SELLERS]
    kb.append([InlineKeyboardButton("✅ Завершить выбор", callback_data="shift_done")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="add_shift")])
    
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(kb))

async def save_shift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    shift_data = context.user_data['shift']
    date = sdate(shift_data['date'])
    sellers = shift_data['selected_sellers']
    
    if len(sellers) < 1:
        await query.answer("❌ Выберите хотя бы одного продавца", show_alert=True)
        return
    
    try:
        ws = GSHEET.worksheet(SHEET_SHIFTS)
        # Проверяем, есть ли уже смена на эту дату
        existing = None
        try:
            cell = ws.find(date)
            existing = ws.row_values(cell.row)
        except gspread.exceptions.CellNotFound:
            pass
        
        if existing:
            # Обновляем существующую запись
            ws.update_cell(cell.row, 2, sellers[0])
            if len(sellers) > 1:
                ws.update_cell(cell.row, 3, sellers[1])
            else:
                ws.update_cell(cell.row, 3, "")
        else:
            # Добавляем новую запись
            row = [date] + sellers[:2]  # Максимум 2 продавца
            if len(sellers) < 2:
                row += [""] * (2 - len(sellers))
            ws.append_row(row)
        
        await query.message.edit_text(
            f"✅ Смена на {date} успешно сохранена!\n"
            f"👥 Продавцы: {', '.join(sellers)}")
        
        # Очистка данных
        context.user_data.pop('shift', None)
        
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка сохранения смены: {str(e)}")

# --- ДОЛГИ ---
async def show_current_debts(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """Показывает страничный список АКТУАЛЬНЫХ долгов с остатком > 0."""
    query = update.callback_query
    if query:
        await query.answer()

    try:
        ws = GSHEET.worksheet(SHEET_DEBTS)
        rows = ws.get_all_values()[1:]
        
        unpaid_debts = []
        # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ ЗДЕСЬ ---
        # Добавляем проверку, что остаток (столбец E, индекс 4) больше нуля.
        for row in rows:
            try:
                # Проверяем, что в строке есть все нужные столбцы
                if len(row) >= 7:
                    status_paid = row[6].strip().lower()
                    balance_str = row[4].strip()
                    # Убеждаемся, что строка с остатком не пустая, прежде чем преобразовывать в число
                    if balance_str and float(balance_str.replace(',', '.')) > 0 and status_paid != "да":
                        unpaid_debts.append(row)
            except (IndexError, ValueError):
                # Игнорируем строки с неверным форматом данных
                continue

        unpaid_debts.sort(key=lambda x: pdate(x[5]) or dt.date.max)
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка чтения таблицы долгов: {e}")
        return

    # Логика пагинации
    per_page = 10
    total_records = len(unpaid_debts)
    total_pages = math.ceil(total_records / per_page) if total_records > 0 else 1
    page = max(0, min(page, total_pages - 1)) 

    start_index = page * per_page
    end_index = start_index + per_page
    page_debts = unpaid_debts[start_index:end_index]

    msg = f"<b>📋 Текущие долги (Стр. {page + 1}/{total_pages}):</b>\n"

    if not page_debts:
        msg = "✅ <b>Отлично! Текущих долгов нет.</b>"
    else:
        for debt in page_debts:
            # Ваш формат вывода полностью сохранен
            date_created, supplier, total_amount, _, to_pay, due_date, _, pay_type = (debt + ["Наличные"])[:8]
            
            msg += "\n──────────────────\n"
            msg += f"<b>Поставщик:</b> {supplier}\n"
            msg += f"    💰 <b>Сумма долга:</b> {float(to_pay.replace(',', '.')):.2f}₴\n"
            msg += f"    🗓 <b>Дата долга:</b> {date_created}\n"
            msg += f"    ❗️ <b>Срок погашения:</b> {due_date}\n"
            msg += f"    💳 <b>Тип оплаты:</b> {pay_type}\n"
    
    kb = []
    kb_nav = []
    if page > 0:
        kb_nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"current_debts_{page - 1}"))
    if (page + 1) < total_pages:
        kb_nav.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"current_debts_{page + 1}"))
    if kb_nav:
        kb.append(kb_nav)

    kb.append([InlineKeyboardButton("✅ Погасить долг", callback_data="close_debt")])
    kb.append([InlineKeyboardButton("🔙 В меню Долги", callback_data="debts_menu")])

    if query and query.message:
        await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    elif update.message:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

async def show_upcoming_payments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает предстоящие долги, используя кэш."""
    query = update.callback_query
    await query.answer()

    today = dt.date.today()
    _, end_of_week = week_range(today)
    
    # ИСПОЛЬЗУЕМ КЭШ
    ws = GSHEET.worksheet(SHEET_DEBTS)
    rows = ws.get_all_values()[1:]
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения таблицы долгов. Попробуйте позже.")
        return

    upcoming_payments = []
    for row in rows:
        # Проверяем столбец G (индекс 6)
        if len(row) >= 7 and row[6].strip().lower() != "да" and row[5]:
            due_date = pdate(row[5].strip())
            if due_date and (today <= due_date <= end_of_week):
                upcoming_payments.append(row)

    upcoming_payments.sort(key=lambda x: pdate(x[5]))
    msg = f"<b>🗓️ Предстоящие платежи до {sdate(end_of_week)}</b>\n"
    if not upcoming_payments:
        msg += "\n<i>На этой неделе предстоящих платежей по долгам нет.</i>"
    else:
        payments_by_date = defaultdict(list)
        for payment in upcoming_payments:
            payments_by_date[payment[5]].append(payment)

        for due_date_str, payments in sorted(payments_by_date.items(), key=lambda item: pdate(item[0])):
            msg += f"\n<b><u>Срок: {due_date_str}</u></b>\n"
            for payment in payments:
                supplier = payment[1]
                to_pay = float(payment[4].replace(',', '.'))
                msg += f"  • {supplier}: <b>{to_pay:.2f}₴</b>\n"

    kb = [[InlineKeyboardButton("🔙 Назад в меню Долги", callback_data="debts_menu")]]
    await query.message.edit_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# --- ДОБАВЬТЕ ЭТУ НЕДОСТАЮЩУЮ ФУНКЦИЮ ---
async def repay_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, row_index: int):
    """Показывает сообщение с подтверждением погашения долга, используя ИНДЕКС СТРОКИ."""
    query = update.callback_query
    await query.answer()
    
    try:
        ws = GSHEET.worksheet(SHEET_DEBTS)
        debt_row = ws.row_values(row_index)
        
        # Индексы по вашему скриншоту: B(1) - Поставщик, E(4) - Остаток, F(5) - Срок
        supplier = debt_row[1]
        to_pay = float(debt_row[4].replace(',', '.'))
        due_date = debt_row[5]

        text = (
            f"❗️<b>Подтвердите действие</b>\n\n"
            f"Вы уверены, что хотите полностью погасить долг перед поставщиком?\n\n"
            f"<b>Поставщик:</b> {supplier}\n"
            f"<b>Сумма к погашению:</b> {to_pay:.2f}₴\n"
            f"<b>Срок:</b> {due_date}\n"
        )
        
        kb = [[
            InlineKeyboardButton("✅ Да, погасить", callback_data=f"repay_final_{row_index}"),
            InlineKeyboardButton("❌ Отмена", callback_data="close_debt")
        ]]
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        logging.error(f"Ошибка в repay_confirm для строки {row_index}: {e}")
        await query.message.edit_text(f"❌ Не удалось найти данные о долге. Возможно, он был удален.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")]]))

async def view_repayable_debts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список долгов для погашения, используя кэш."""
    query = update.callback_query
    await query.answer()

    # ИСПОЛЬЗУЕМ КЭШ
    ws = GSHEET.worksheet(SHEET_DEBTS)
    rows = ws.get_all_values()[1:]
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения таблицы долгов. Попробуйте позже.")
        return

    # Проверяем столбец G (индекс 6) и добавляем реальный индекс строки
    unpaid_debts = [row + [i+2] for i, row in enumerate(rows) if len(row) >= 7 and row[6].strip().lower() != "да"]
    unpaid_debts.sort(key=lambda x: pdate(x[5]) or dt.date.max)

    if not unpaid_debts:
        await query.message.edit_text("✅ Все долги погашены!", reply_markup=debts_menu_kb())
        return

    msg = "<b>💸 Погашение долга</b>\n\nВыберите из списка долг, который хотите погасить полностью:"
    kb = []
    for debt in unpaid_debts:
        row_index = debt[-1]
        date_str = debt[0] if len(debt) > 0 else ""
        supplier = debt[1] if len(debt) > 1 else ""
        total_str = debt[2] if len(debt) > 2 else "0"
        due_date_str = debt[5] if len(debt) > 5 else ""
        total_amount = float(total_str.replace(',', '.'))
        btn_text = f"{date_str} - {supplier} - {total_amount:.2f}₴"
        kb.append([InlineKeyboardButton(btn_text, callback_data=f"repay_confirm_{row_index}")])
    
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")])
    await query.message.edit_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))


# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ЦЕЛИКОМ ---
async def repay_final(update: Update, context: ContextTypes.DEFAULT_TYPE, row_index: int):
    """Окончательно закрывает долг, обновляя все связанные таблицы (Долги, Сейф, Поставщики)."""
    query = update.callback_query
    
    try:
        ws_debts = GSHEET.worksheet(SHEET_DEBTS)
        debt_row = ws_debts.row_values(row_index)
        
        date_created = debt_row[0]
        supplier_name = debt_row[1]
        total = float(debt_row[2].replace(',', '.'))
        
        # 1. Закрываем долг в листе "Долги"
        ws_debts.update_cell(row_index, 4, total)      # Оплачено
        ws_debts.update_cell(row_index, 5, 0)          # Остаток
        ws_debts.update_cell(row_index, 7, "Да")       # Погашено
        
        # 2. Списываем сумму с сейфа
        who = query.from_user.first_name
        comment = f"Оплата долга {supplier_name} за {date_created}"
        add_safe_operation("Расход", total, comment, who)
        
        # 3. Обновляем статус в листе "Поставщики"
        ws_sup = GSHEET.worksheet(SHEET_SUPPLIERS)
        sup_rows = ws_sup.get_all_values()[1:]
        for i, sup_row in enumerate(sup_rows, start=2):
            # Ищем накладную по дате и поставщику
            if len(sup_row) > 8 and sup_row[0] == date_created and sup_row[1] == supplier_name:
                # Обновляем столбцы: Оплачено(H,8), Долг(I,9), Срок(J,10), История(M,13)
                ws_sup.update_cell(i, 8, "Да")
                ws_sup.update_cell(i, 9, 0)
                ws_sup.update_cell(i, 10, "") # Очищаем срок долга
                history_comment = f"Погашен {sdate()}; "
                old_history = ws_sup.cell(i, 13).value or ""
                ws_sup.update_cell(i, 13, old_history + history_comment)
                logging.info(f"Обновлен статус долга в Поставщиках для строки {i}")
                break

        # Сбрасываем кэши измененных таблиц
        if 'sheets_cache' in context.bot_data:
            context.bot_data['sheets_cache'].pop(SHEET_DEBTS, None)
            context.bot_data['sheets_cache'].pop(SHEET_SUPPLIERS, None)
            context.bot_data['sheets_cache'].pop("Сейф", None)
        
        await query.answer(f"✅ Долг для {supplier_name} успешно закрыт!", show_alert=True)
        # Показываем обновленный список долгов
        await view_repayable_debts(update, context)
        
    except Exception as e:
        logging.error(f"Ошибка финального погашения долга: {e}", exc_info=True)
        await query.answer(f"❌ Ошибка обновления таблицы: {e}", show_alert=True)
        
async def view_debts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает список НЕПОГАШЕННЫХ долгов и создает для них кнопки
    с ПРАВИЛЬНЫМ идентификатором (номером строки в Google Таблице).
    """
    # Определяем, откуда пришел вызов (от кнопки или от текста)
    if hasattr(update, "callback_query") and update.callback_query:
        query = update.callback_query
        await query.answer()
        msg_func = query.message.edit_text
    else:
        msg_func = update.message.reply_text

    try:
        ws = GSHEET.worksheet(SHEET_DEBTS)
        rows = ws.get_all_values()[1:]
    except Exception as e:
        await msg_func(f"❌ Ошибка чтения таблицы долгов: {e}")
        return

    # Отбираем только непогашенные долги (столбец G, индекс 6) и добавляем к ним реальный номер строки (i+2)
    unpaid_debts = [row + [i+2] for i, row in enumerate(rows) if len(row) > 6 and row[6].strip().lower() != "да"]
    unpaid_debts.sort(key=lambda x: pdate(x[5]) or dt.date.max) # Сортируем по сроку погашения
    
    # Сохраняем этот список для дальнейшей работы (важно для repay_confirm)
    context.user_data['repay_debts'] = unpaid_debts

    if not unpaid_debts:
        await msg_func("🟢 Отлично! Нет долгов для погашения.", reply_markup=debts_menu_kb())
        return

    text = "<b>💸 Погасить долг</b>\n\nВыберите из списка:\n"
    kb = []
    # Используем enumerate для нумерации в тексте, но callback делаем с реальным индексом строки
    for i, debt_data in enumerate(unpaid_debts):
        # Безопасно извлекаем данные
        date, supplier, total_str, _, _, due, _, row_index = (debt_data + [sdate(), 0])[:8]
        
        text += (f"\n<b>{i+1}. {supplier}</b>\n"
                 f"   - Дата: {date}, Срок: {due}\n"
                 f"   - Сумма: <b>{float(total_str.replace(',','.')):.2f}₴</b>\n")
        
        # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ ---
        # Создаем кнопку с ПРАВИЛЬНЫМ номером строки, а не с порядковым номером i
        kb.append([InlineKeyboardButton(f"✅ Погасить долг №{i+1} для {supplier}", callback_data=f"repay_confirm_{row_index}")])
        
    kb.append([InlineKeyboardButton("🔙 Назад в меню долгов", callback_data="debts_menu")])

    await msg_func(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
# --- ОБРАБОТЧИКИ СЕЙФОВ, ПЕРЕУЧЕТОВ И ЗП ---

async def inventory_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    push_nav(context, "inventory_balance")  # <--- добавь!
    bal = get_inventory_balance()
    await update.callback_query.message.edit_text(
        f"📦 Текущий остаток магазина: <b>{bal:.2f}₴</b>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stock_safe_menu")]])
    )

async def safe_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    rows = get_cached_sheet_data(context, "Сейф")
    if rows is None:
        await query.message.edit_text("❌ Ошибка чтения истории сейфа.")
        return

    last_ops = rows[-15:]
    last_ops.reverse()
    
    text = "🧾 <b>Последние 15 операций с сейфом:</b>\n"
    if not last_ops:
        text += "\n<i>Операций еще не было.</i>"
    else:
        for row in last_ops:
            date, op_type, amount, comment, user = (row + ["", "", "", "", ""])[:5]
            icon = "🟢" if op_type == "Пополнение" else "🔴"
            text += "\n──────────────────\n"
            text += f"{icon} <b>{op_type}: {amount}₴</b> ({user})\n"
            text += f"   <i>{date} - {comment}</i>"
        
    await query.message.edit_text(text, parse_mode='HTML', reply_markup=stock_safe_kb())



# --- ЗАМЕНИТЕ ЭТИ ДВЕ ФУНКЦИИ ---

async def start_safe_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Устанавливаем правильное состояние для пополнения
    context.user_data['safe_op'] = {'type': 'deposit', 'step': 'amount'}
    await query.message.edit_text(
        "💵 Введите сумму для пополнения сейфа:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stock_safe_menu")]])
    )

async def start_safe_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Устанавливаем правильное состояние для снятия
    context.user_data['safe_op'] = {'type': 'withdraw', 'step': 'amount'}
    await query.message.edit_text(
        "💸 Введите сумму для снятия из сейфа:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stock_safe_menu")]])
    )
    
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
async def handle_safe_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод суммы для операций с сейфом и сбрасывает кэш."""
    try:
        amount = float(update.message.text.replace(',', '.'))
        user = update.effective_user
        op_data = context.user_data.get('safe_op', {})
        op_type = op_data.get('type')

        if not op_type:
            await update.message.reply_text("❌ Произошла ошибка состояния. Пожалуйста, начните заново.")
            context.user_data.pop('safe_op', None)
            return

        # 1. Выполняем операцию записи в таблицу
        if op_type == 'deposit':
            add_safe_operation("Пополнение", amount, "Внесение наличных", user.first_name)
        elif op_type == 'withdraw':
            add_safe_operation("Снятие", amount, "Снятие администратором", user.first_name)

        # 2. ГЛАВНОЕ ИСПРАВЛЕНИЕ: Принудительно сбрасываем кэш для листа "Сейф"
        if 'sheets_cache' in context.bot_data and "Сейф" in context.bot_data['sheets_cache']:
            del context.bot_data['sheets_cache']["Сейф"]
            logging.info("Кэш для листа 'Сейф' сброшен после операции.")

        # 3. Получаем уже 100% актуальный баланс
        bal = get_safe_balance(context)
        
        # 4. Формируем и отправляем ответ
        msg = f"✅ Сейф пополнен на {amount:.2f}₴" if op_type == 'deposit' else f"✅ Снято из сейфа: {amount:.2f}₴"
        msg += f"\n\nТекущий остаток: <b>{bal:.2f}₴</b>"
        kb = [[InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")]]
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        context.user_data.pop('safe_op', None)

    except (ValueError, KeyError):
        await update.message.reply_text("❌ Ошибка. Попробуйте снова или введите число.")
# --- ГРАФИКИ И ЭКСПОРТ ---
async def generate_chart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, chart_type, start_str, end_str = query.data.split('_')
    start_date = pdate(start_str)
    end_date = pdate(end_str)
    
    try:
        ws = GSHEET.worksheet(SHEET_REPORT)
        rows = ws.get_all_values()[1:]
        
        dates = []
        sales = []
        dates_set = set()
        
        current = start_date
        while current <= end_date:
            dates.append(sdate(current))
            sales.append(0)
            current += dt.timedelta(days=1)
        
        for row in rows:
            if len(row) < 5:
                continue
            try:
                row_date = pdate(row[0])
                if row_date is None:
                    continue  # добавил эту строку!
                if start_date <= row_date <= end_date:
                    pass
            except Exception as e:
                logging.error(f"Ошибка обработки строки отчета: {e}")
                continue
        
        plt.figure(figsize=(10, 6))
        plt.plot(dates, sales, marker='o', linestyle='-')
        plt.title(f"Продажи с {sdate(start_date)} по {sdate(end_date)}")
        plt.xlabel("Дата")
        plt.ylabel("Сумма (₴)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=InputFile(buf),
            caption=f"📈 График продаж за период"
        )
        
    except Exception as e:
        await query.message.reply_text(f"❌ Ошибка генерации графика: {str(e)}")

async def export_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # В реальной реализации здесь бы генерировался Excel-файл
    await query.message.reply_text(
        "📥 Экспорт в Excel временно недоступен. Функция в разработке.")

# --- ОБРАБОТЧИКИ ТЕКСТА ---
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Ваша строка text.lower() здесь была убрана, т.к. в некоторых шагах нужен оригинальный регистр
    text = update.message.text.strip() 
    
    if text.lower() == "/cancel":
        return await cancel(update, context)

    user_data = context.user_data
    state_key = next((key for key in [
        'revision', 'report', 'supplier', 'planning', 'edit_plan', 'edit_invoice', 
        'search_debt', 'safe_op', 'inventory_expense', 'repay', 'shift', 'report_period'
    ] if key in user_data), None)

    # Если никакого состояния нет, выходим
    if not state_key:
        return await update.message.reply_text(
            "ℹ️ Для взаимодействия с ботом, пожалуйста, используйте меню.",
            reply_markup=main_kb(str(update.effective_user.id) in ADMINS)
        )

    # --- Маршрутизация по активному состоянию ---
    
    if state_key == 'report':
        step = user_data['report'].get('step')
        if step == 'cash': return await handle_report_cash(update, context)
        elif step == 'terminal': return await handle_report_terminal(update, context)
        elif step == 'expenses': return await handle_report_expenses(update, context)
        elif step == 'expense_comment': return await handle_expense_comment(update, context)
        elif step == 'comment': return await save_report(update, context)

    elif state_key == 'supplier':
        step = user_data['supplier'].get('step')
        # --- ИСПРАВЛЕНИЕ: Добавлены все шаги для диалога добавления поставщика ---
        if step == 'name': return await handle_supplier_name(update, context)
        elif step == 'amount_income': return await handle_supplier_amount_income(update, context)
        elif step == 'writeoff': return await handle_supplier_writeoff(update, context)
        elif step == 'invoice_total_markup': return await handle_supplier_invoice_total_markup(update, context)
        elif step == 'due_date': return await handle_supplier_due_date(update, context)
        elif step == 'comment': return await save_supplier(update, context)

    elif state_key == 'expense':
        step = user_data['expense'].get('step')
        if step == 'value': return await handle_expense_value(update, context)
        elif step == 'comment': return await save_expense(update, context)

    elif state_key == 'revision':
        step = user_data['revision'].get('step')
        if step == 'actual_amount': return await handle_revision_amount(update, context)
        elif step == 'comment': return await save_revision(update, context)

    elif state_key == 'edit_invoice':
        # Эта логика обрабатывается отдельно, так как она более сложная
        edit_state = user_data['edit_invoice']
        step = edit_state.get('step', '')
        field_key = step.replace('editing_', '')
        
        fields_to_edit = edit_state.get('fields_to_edit_list', [])
        current_index = edit_state.get('current_field_index', 0)
        
        if fields_to_edit and current_index < len(fields_to_edit) and fields_to_edit[current_index] == field_key:
            edit_state.setdefault('new_values', {})[field_key] = update.message.text
            edit_state['current_field_index'] += 1
            await ask_for_invoice_edit_value(update, context)
        else:
            await update.message.reply_text("Пожалуйста, используйте кнопки или отвечайте на последний вопрос бота.")
        return

    elif state_key == 'planning':
        step = user_data['planning'].get('step')
        if step == 'amount': return await handle_planning_amount(update, context)
        elif step == 'other_supplier_name':
            supplier_name = update.message.text
            target_date_str = user_data['planning']['date']
            user_data['planning'].update({'supplier': supplier_name, 'step': 'amount'})
            await update.message.reply_text(
                f"💰 Введите примерную сумму для <b>{supplier_name}</b> на {target_date_str} (в гривнах):",
                parse_mode=ParseMode.HTML
            )
            return

    elif state_key == 'edit_plan':
        if user_data['edit_plan'].get('field') == 'amount':
            try:
                await edit_plan_save_value(update, context, new_value=parse_float(text))
            except ValueError:
                await update.message.reply_text("❌ Пожалуйста, введите числовое значение.")
        return

    elif state_key == 'search_debt':
        # Эта логика остается полностью вашей, я ее не трогал
        search_query = text.strip()
        context.user_data.pop('search_debt', None)
        rows = get_cached_sheet_data(context, SHEET_DEBTS)
        if rows is None:
             await update.message.reply_text(f"❌ Ошибка чтения таблицы долгов.")
             return
        matches = []
        for i, row in enumerate(rows):
            if len(row) < 7: continue
            date_str, name_str, amount_str = row[0].strip(), row[1].strip().lower(), row[2].replace(',', '.')
            if (search_query == date_str or search_query.lower() in name_str or (search_query.replace(',', '.').isdigit() and search_query == amount_str)):
                matches.append(row + [i+2])
        if not matches:
            await update.message.reply_text("🚫 Ничего не найдено.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")]]))
        else:
            msg = f"<b>🔎 Результаты поиска по '{search_query}':</b>\n"
            kb = []
            for debt in matches:
                supplier, total, to_pay, due_date, status, row_index = debt[1], parse_float(debt[2]), parse_float(debt[4]), debt[5], debt[6], debt[-1]
                status_icon = "✅" if status.lower() == 'да' else "❌"
                msg += f"\n──────────────────\n{status_icon} <b>{supplier}</b>\n  <b>Сумма:</b> {total:.2f}₴ | <b>Остаток:</b> {to_pay:.2f}₴\n  <b>Срок:</b> {due_date}"
                if status.lower() != 'да':
                    kb.append([InlineKeyboardButton(f"✅ Погасить для {supplier} ({to_pay:.2f}₴)", callback_data=f"repay_confirm_{row_index}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="debts_menu")])
            await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
        return

    elif state_key == 'safe_op':
        if user_data['safe_op'].get('step') == 'amount': return await handle_safe_amount(update, context)

    elif state_key == 'inventory_expense':
        step = user_data['inventory_expense'].get('step')
        if step == 'amount': return await handle_inventory_expense(update, context)
        elif step == 'comment': return await save_inventory_expense(update, context)

    elif state_key == 'repay':
        if user_data['repay'].get('step') == 'amount': return await repay_debt(update, context)

    elif state_key == 'shift':
        if user_data['shift'].get('step') == 'date': await handle_shift_date(update, context)

    elif state_key == 'report_period':
        step = user_data['report_period'].get('step')
        if step == 'start_date': return await handle_report_start_date(update, context)
        elif step == 'end_date': return await handle_report_end_date(update, context)
            
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    try:
        # --- 1. ОСНОВНЫЕ МЕНЮ ---
        if data == "main_menu": await main_menu(update, context)
        elif data == "close": await close_menu(update, context)
        elif data == "finance_menu": await finance_menu(update, context)
        elif data == "suppliers_menu": await suppliers_menu(update, context)
        elif data == "debts_menu": await debts_menu(update, context)
        elif data == "admin_panel": await admin_panel(update, context)
        elif data == "staff_management": await staff_management_menu(update, context)
        elif data == "stock_safe_menu": await stock_safe_menu(update, context)
        elif data == "staff_menu": await staff_menu(update, context)
        
        # --- 2. ПЛАНИРОВАНИЕ ---
        elif data == "planning": await start_planning(update, context)
        elif data.startswith("plan_nav_"):
            target_date = pdate(data.split('_')[-1])
            await start_planning(update, context, target_date=target_date)
        elif data.startswith("plan_delete_"):
            _, _, row_index_str, date_str = data.split('_')
            if delete_plan_by_row_index(int(row_index_str)):
                await query.answer("План удален!")
            else:
                await query.answer("❌ Ошибка удаления", show_alert=True)
            # Обновляем меню планирования для того же дня
            await start_planning(update, context, target_date=pdate(date_str))
        
        elif data.startswith("plan_sup_"): await handle_planning_supplier_choice(update, context)
        elif data.startswith("plan_pay_"): await handle_planning_pay_type(update, context)
        
        # --- 3. ЖУРНАЛ ПРИБЫТИЯ И РЕДАКТИРОВАНИЕ ПЛАНОВ ---
        elif data == "view_suppliers": await show_arrivals_journal(update, context)
        elif data.startswith("toggle_arrival_"): await toggle_arrival_status(update, context)
        elif data.startswith("edit_plan_field_"): await edit_plan_choose_field(update, context)
        elif data.startswith("edit_plan_value_"): await edit_plan_save_value(update, context)
        elif data.startswith("journal_nav_"):
            target_date = pdate(data.split('_')[-1])
            await show_arrivals_journal(update, context, target_date=target_date)
        elif data.startswith("edit_plan_"): await edit_plan_start(update, context)

        # --- 4. РЕДАКТИРОВАНИЕ НАКЛАДНОЙ (НОВОЕ) ---
        elif data.startswith("edit_invoice_start_"):
            # Начинаем процесс редактирования
            row_index = int(data.split('_')[-1])
            
            # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Правильно инициализируем ВСЕ нужные поля ---
            context.user_data['edit_invoice'] = {
                'row_index': row_index,
                'selected_fields': {}, # Поля, которые пользователь выберет для редактирования
                'new_values': {}       # Словарь для хранения новых введенных значений
            }
            
            all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
            # Проверяем, что индекс не выходит за пределы списка
            if row_index - 2 < len(all_invoices):
                invoice_data = all_invoices[row_index - 2]
                kb = build_edit_invoice_keyboard(invoice_data, {}, row_index)
                await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля для изменения и нажмите 'Сохранить'.",
                                              parse_mode=ParseMode.HTML, reply_markup=kb)
            else:
                await query.message.edit_text("❌ Ошибка: не удалось найти накладную для редактирования.")


        elif data.startswith("edit_invoice_toggle_"):
            parts = data.split('_')
            row_index = int(parts[3])
            field = "_".join(parts[4:])
            
            edit_state = context.user_data.get('edit_invoice', {})
            if edit_state.get('row_index') != row_index: return

            if field in edit_state['selected_fields']:
                del edit_state['selected_fields'][field]
            else:
                edit_state['selected_fields'][field] = None
            
            all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
            invoice_data = all_invoices[row_index - 2]
            kb = build_edit_invoice_keyboard(invoice_data, edit_state['selected_fields'], row_index)
            await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля для изменения и нажмите 'Сохранить'.",
                                          parse_mode=ParseMode.HTML, reply_markup=kb)

        elif data.startswith("edit_invoice_save_"):
             await ask_for_invoice_edit_value(update, context)

        elif data.startswith("edit_invoice_cancel_"):
            row_index = int(data.split('_')[-1])
            day_invoice_rows = context.user_data.get('day_invoice_rows', [])
            try:
                list_index = day_invoice_rows.index(row_index)
                all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
                date_str = sdate(pdate(all_invoices[row_index-2][0]))
                query.data = f"view_single_invoice_{date_str}_{list_index}"
                await show_single_invoice(update, context)
            except (ValueError, IndexError):
                await suppliers_menu(update, context)
            context.user_data.pop('edit_invoice', None)
            
        elif data.startswith("invoice_edit_value_"):
            value = data.replace("invoice_edit_value_", "")
            edit_state = context.user_data.get('edit_invoice', {})
            fields_to_edit = edit_state.get('fields_to_edit_list', [])
            current_index = edit_state.get('current_field_index', 0)
            if fields_to_edit and current_index < len(fields_to_edit):
                current_field_key = fields_to_edit[current_index]
                edit_state['new_values'][current_field_key] = value
                edit_state['current_field_index'] += 1
                if current_field_key == 'pay_type' and value == 'Долг' and 'due_date' not in fields_to_edit:
                    fields_to_edit.append('due_date')
            await ask_for_invoice_edit_value(update, context)
            
        elif data.startswith("execute_invoice_edit_"):
            await execute_invoice_edit(update, context)

        # --- 5. ДОБАВЛЕНИЕ НАКЛАДНОЙ ---
        elif data == "add_supplier": await start_supplier(update, context)
        elif data.startswith("add_sup_"): await handle_add_supplier_choice(update, context)
        elif data.startswith("pay_"): await handle_supplier_pay_type(update, context)
        elif data == "skip_comment_supplier": await save_supplier(update, context)

        # --- 6. СДАЧА СМЕНЫ ---
        elif data == "add_report": await start_report(update, context)
        elif data.startswith("report_seller_"):  # Новый обработчик для сдачи смены
            await handle_report_seller(update, context)
        elif data in ("exp_yes", "exp_no"): await handle_report_expenses_ask(update, context)
        elif data in ("more_yes", "more_no"): await handle_expense_more(update, context)
        elif data == "skip_comment": await save_report(update, context)
        
        # --- 7. ПРОСМОТР ОТЧЕТОВ ---
        elif data == "view_reports_menu": await view_reports_menu(update, context)
        elif data == "report_today": await get_report_today(update, context)
        elif data == "report_yesterday": await get_report_yesterday(update, context)
        elif data.startswith("report_week_"):
            if data == "report_week_current": await get_report_week(update, context)
            else: _, _, start_str, end_str = data.split('_', 3); await show_report(update, context, pdate(start_str), pdate(end_str))
        elif data.startswith("report_month_"):
            if data == "report_month_current": await get_report_month(update, context)
            else: _, _, start_str, end_str = data.split('_', 3); await show_report(update, context, pdate(start_str), pdate(end_str))
        elif data == "report_custom": 
            await get_report_custom(update, context)
        elif data == "daily_summary": await show_daily_dashboard(update, context)
        
        # --- 8. ДЕТАЛИЗАЦИЯ ОТЧЕТОВ ---
        elif data == "view_today_invoices": await show_today_invoices(update, context)
        elif data.startswith("choose_date_"): await choose_details_date(update, context)
        elif data.startswith("details_exp_"): await show_expenses_detail(update, context)
        elif data.startswith("details_sup_"): await show_suppliers_detail(update, context)
        elif data.startswith("detail_report_nav_"): await show_detailed_report(update, context)
        elif data.startswith("invoices_list_"): await show_invoices_list(update, context)
        elif data.startswith("view_single_invoice_"): await show_single_invoice(update, context)
        
        # --- 9. ДОЛГИ ---
        elif data.startswith("current_debts_"):
            await show_current_debts(update, context, page=int(data.split('_')[-1]))
        elif data == "upcoming_payments": await show_upcoming_payments(update, context)
        elif data == "close_debt": await view_repayable_debts(update, context)
        elif data == "search_debts": await search_debts_start(update, context)
        elif data.startswith("repay_confirm_"):
            await repay_confirm(update, context, int(data.split('_')[2]))
        elif data.startswith("repay_final_"):
            await repay_final(update, context, int(data.split('_')[2]))
        elif data.startswith("debts_history_"):
            page = 0
            if data == "debts_history_prev":
                page = max(context.user_data.get('debts_history_page', 0) - 1, 0)
            elif data == "debts_history_next":
                page = context.user_data.get('debts_history_page', 0) + 1
            else: # Обрабатывает первый вызов, например 'debts_history_0'
                try:
                    page = int(data.split('_')[-1])
                except (ValueError, IndexError):
                    page = 0
            
            context.user_data['debts_history_page'] = page
            await view_debts_history(update, context, page=page)
            
        # --- 10. УПРАВЛЕНИЕ ПЕРСОНАЛОМ (АДМИН) ---
        elif data.startswith("view_salary_"): await show_seller_salary_details(update, context)
        elif data.startswith("confirm_payout_"): await confirm_payout(update, context)
        elif data.startswith("execute_payout_"): await execute_payout(update, context)
        elif data.startswith("salary_history_"): await show_salary_history(update, context)
        elif data == "view_shifts":
            await view_shifts_calendar(update, context)
        elif data == "edit_shifts":
            await edit_shifts_calendar(update, context)
        elif data.startswith("shift_nav_"):
            _, _, year, month = data.split('_')
        # Определяем, в каком режиме мы были
        # Мы можем сохранить это в user_data или просто решить по-умолчанию
        # Для простоты, пусть навигация всегда ведет в режим админа, если он админ
            if str(query.from_user.id) in ADMINS:
                await edit_shifts_calendar(update, context, int(year), int(month))
            else:
                await view_shifts_calendar(update, context, int(year), int(month))
        elif data.startswith("edit_shift_"):
            await edit_single_shift(update, context)
        elif data.startswith("toggle_seller_"):
            await toggle_seller_for_shift(update, context)
        elif data == "save_shift":
            await save_shift_changes(update, context)
        elif data.startswith("view_shift_"):
            await show_shift_details(update, context)
        elif data == "seller_stats":
            await show_seller_stats_menu(update, context)
        elif data.startswith("view_seller_stats_"):
            await show_seller_stats(update, context)
        elif data == "compare_sellers": await show_sellers_comparison(update, context)
    
        # --- 11. СЕЙФ И ОСТАТОК ---
        elif data == "inventory_balance": await inventory_balance(update, context)
        elif data == "safe_balance": await safe_balance(update, context)
        elif data == "safe_history": await safe_history(update, context)
        elif data == "inventory_history": await inventory_history(update, context)
        elif data == "safe_deposit": await start_safe_deposit(update, context)
        elif data == "safe_withdraw": await start_safe_withdraw(update, context)
        elif data == "add_inventory_expense": await start_inventory_expense(update, context)
        elif data == "admin_revision": await start_revision(update, context)

        # --- 12. ПРОЧЕЕ ---
        elif data == "noop": pass
        else:
            await query.answer("Команда не реализована.", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка обработки callback: {data}. Ошибка: {e}", exc_info=True)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Произошла критическая ошибка: {e}")
        



async def error_handler(update, context):
    import traceback
    tb = traceback.format_exc()
    logging.error(f"Exception: {tb}")
    if update and hasattr(update, "effective_chat") and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"❌ Произошла ошибка!\n<pre>{tb}</pre>",
            parse_mode="HTML"
        )


# --- ЗАПУСК ---
def main():
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Основные обработчики
    app.add_handler(CallbackQueryHandler(cancel_report, pattern="^cancel_report$"))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)
    logging.info("Бот запущен!")
    app.run_polling()
    
if __name__ == "__main__":
    main()

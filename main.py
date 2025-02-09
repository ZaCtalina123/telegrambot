import asyncio
import logging
import os
import random
import sqlite3
import pandas as pd
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from fpdf import FPDF

from prettytable import PrettyTable
from typing import List

from aiogram import Bot, Dispatcher, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram import BaseMiddleware
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if TELEGRAM_BOT_TOKEN is None:
    raise ValueError("TELEGRAM_BOT_TOKEN не задан в переменных окружения.")

ALLOWED_USERS_ENV = os.environ.get("TELEGRAM_ALLOWED_USERS")
if ALLOWED_USERS_ENV is None:
    raise ValueError("TELEGRAM_ALLOWED_USERS не задан в переменных окружения.")
ALLOWED_USERS = set(int(user.strip()) for user in ALLOWED_USERS_ENV.split(','))

DATABASE_FILE = "bot_data.db"

class DatabaseManager:
    """
    Обрабатывает операции с базой данных, в том числе инициализацию, хранение списка учащихся,
    а также историю сгенерированных расписаний.
    """
    def __init__(self, db_file: str = DATABASE_FILE) -> None:
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.init_db()

    def init_db(self) -> None:
        cursor = self.conn.cursor()
        # Таблица для хранения списка учащихся.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                surname TEXT UNIQUE NOT NULL
            )
        """)
        # Таблица для хранения истории расписаний.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedule_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule TEXT NOT NULL,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def get_all_students(self) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT surname FROM students ORDER BY surname ASC")
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    def add_students(self, surnames: List[str]) -> None:
        cursor = self.conn.cursor()
        for surname in surnames:
            try:
                cursor.execute("INSERT INTO students (surname) VALUES (?)", (surname,))
            except sqlite3.IntegrityError:
                pass
        self.conn.commit()

    def remove_student(self, surname: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM students WHERE surname = ?", (surname,))
        self.conn.commit()
        return cursor.rowcount > 0

    def add_schedule_history(self, schedule: str) -> None:
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO schedule_history (schedule) VALUES (?)", (schedule,))
        self.conn.commit()

db_manager = DatabaseManager()
if not db_manager.get_all_students():
    initial_students = [
        "Атаманова", "Бабенков", "Бендусов", "Вертакова", "Выродова",
        "Герасимова", "Гиренко", "Иванов", "Киртока", "Ковалёва",
        "Коновалов", "Куликова", "Минаева", "Митюшин", "Мурашова",
        "Мягкова", "Номашко", "Петрова", "Романова-Саваренская", 
        "Сигарёв", "Соколов", "Солдатова", "Соловьёв", "Трошин",
        "Ходунова", "Черняев", "Чуб", "Шалаев", "Шубин"
    ]
    db_manager.add_students(initial_students)

class AllowedUsersMiddleware(BaseMiddleware):
    """
    Middleware для проверки, что пользователь входит в список разрешенных.
    Если нет, дальнейшая обработка сообщения прекращается.
    """
    async def __call__(self, handler, event, data):
        user = None
        if hasattr(event, "from_user") and event.from_user:
            user = event.from_user
        elif hasattr(event, "message") and event.message and event.message.from_user:
            user = event.message.from_user
        if user and user.id not in ALLOWED_USERS:
            if isinstance(event, types.Message):
                await event.reply("Access denied.")
            elif isinstance(event, types.CallbackQuery):
                await event.answer("Access denied.", show_alert=True)
            return
        return await handler(event, data)

class GlobalErrorHandler(BaseMiddleware):
    """
    Глобальный обработчик ошибок, который перехватывает все исключения,
    логгирует их с подробностями и отправляет пользователю сообщение об ошибке.
    """
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logging.exception("Необработанная ошибка: %s", e)
            if isinstance(event, types.Message):
                await event.reply("Произошла ошибка, попробуйте позже.")
            elif isinstance(event, types.CallbackQuery):
                await event.answer("Произошла ошибка, попробуйте позже.", show_alert=True)
            return

class StudentSurnames:
    """
    Класс для управления списком фамилий учащихся с сохранением их в базе данных.
    """
    def get_all(self) -> List[str]:
        return db_manager.get_all_students()

    def add(self, new_students: List[str]) -> None:
        db_manager.add_students(new_students)

    def remove(self, student: str) -> bool:
        return db_manager.remove_student(student)

STUDENTS = StudentSurnames()

class ScheduleGenerator:
    """
    Генерирует расписание, распределяя участников по этажам и секциям.
    Отображает расписание в виде таблицы и сохраняет его в Excel с улучшенным форматированием.
    """
    def __init__(self, surnames: List[str], places: List[str], num_floors: int) -> None:
        if num_floors <= 0:
            raise ValueError("Number of floors must be positive.")
        if not places:
            raise ValueError("Places list cannot be empty.")
        self.surnames: List[str] = surnames.copy()
        self.places: List[str] = places
        self.num_floors: int = num_floors
        self.sections: List[List[str]] = [[] for _ in range(len(self.places))]
        self.table = PrettyTable()
        self.table.field_names = ['Этаж'] + self.places
        self.table.hrules = True

    def shuffle_surnames(self) -> None:
        random.shuffle(self.surnames)

    def distribute_participants(self) -> None:
        for i, surname in enumerate(self.surnames):
            self.sections[i % len(self.places)].append(surname)

    def create_schedule(self) -> None:
        total_cells = self.num_floors * len(self.places)
        participants_per_cell = len(self.surnames) // total_cells if total_cells > 0 else 0
        for floor in range(self.num_floors):
            row: List[str] = [f"{floor + 1}"]
            for i in range(len(self.places)):
                start_index = floor * participants_per_cell
                end_index = start_index + participants_per_cell
                participants = self.sections[i][start_index:end_index]
                cell_text = '\n'.join(participants) if participants else 'Нет участников'
                row.append(cell_text)
            self.table.add_row(row)

    def save_to_excel(self, filename: str) -> None:
        total_cells = self.num_floors * len(self.places)
        participants_per_cell = len(self.surnames) // total_cells if total_cells > 0 else 0
        data = {'Этаж': [f"{floor + 1}" for floor in range(self.num_floors)]}
        for i, place in enumerate(self.places):
            col_data: List[str] = []
            for floor in range(self.num_floors):
                start_index = floor * participants_per_cell
                end_index = start_index + participants_per_cell
                participants = self.sections[i][start_index:end_index]
                cell_text = '\n'.join(participants) if participants else 'Нет участников'
                col_data.append(cell_text)
            data[place] = col_data
        df = pd.DataFrame(data)
        column_order = ['Этаж'] + self.places
        df = df[column_order]
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Schedule')
            workbook = writer.book
            worksheet = writer.sheets['Schedule']
            header_format = workbook.add_format({
                'bold': True,
                'font_color': 'white',
                'bg_color': '#1F4E78',
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                series = df[value].astype(str)
                max_len = max(series.map(len).max(), len(value)) + 2
                worksheet.set_column(col_num, col_num, max_len)
            cell_format = workbook.add_format({
                'border': 1,
                'align': 'center',
                'valign': 'vcenter',
                'text_wrap': True,
                'bg_color': '#F2F2F2'
            })
            for row_num in range(1, len(df) + 1):
                if row_num % 2 == 0:
                    row_format = workbook.add_format({
                        'border': 1,
                        'align': 'center',
                        'valign': 'vcenter',
                        'text_wrap': True,
                        'bg_color': '#E7E6E6'
                    })
                else:
                    row_format = cell_format
                for col_num in range(len(df.columns)):
                    worksheet.write(row_num, col_num, df.iat[row_num-1, col_num], row_format)

def store_schedule_history(schedule_text: str) -> None:
    """
    Сохраняет сгенерированное расписание в базу данных.
    """
    db_manager.add_schedule_history(schedule_text)

class StudentUpdate(StatesGroup):
    adding_students = State()

storage = MemoryStorage()
bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
router = Router()

@router.message(Command("start"))
async def start_command(message: types.Message):
    welcome_text = (
        "Добро пожаловать! Я бот для генерации расписаний и управления списком учащихся.\n\n"
        "Доступные команды:\n"
        "/start - Запуск бота\n"
        "/help - Список команд\n"
        "/schedule - Генерация расписания\n"
        "/edit_students - Редактирование списка учащихся\n"
        "/stats - Статистика использования\n"
        "/audit - Просмотр логов\n"
    )
    await message.reply(welcome_text)

@router.message(Command("help"))
async def help_command(message: types.Message):
    help_text = (
        "<b>Доступные команды:</b>\n\n"
        "<b>/start</b> - Запускает бота и выводит приветственное сообщение.\n\n"
        "<b>/help</b> - Выводит это сообщение.\n\n"
        "<b>/schedule</b> - Генерирует расписание и отправляет его в виде таблицы, Excel-файла.\n\n"
        "<b>/edit_students</b> - Редактирование списка учащихся.\n\n"
        "<b>/stats</b> - Статистика использования бота.\n\n"
        "<b>/audit</b> - Просмотр последних логов событий.\n\n"
    )
    await message.reply(help_text, parse_mode="HTML")

@router.message(Command("schedule"))
async def schedule_command(message: types.Message):
    surnames = STUDENTS.get_all()
    places = ["Начальная", "Центр", "Старшая"]
    num_floors = 3
    generator = ScheduleGenerator(surnames, places, num_floors)
    generator.shuffle_surnames()
    generator.distribute_participants()
    generator.create_schedule()
    schedule_text = f"<pre>{generator.table.get_string()}</pre>"
    await message.reply(schedule_text, parse_mode="HTML")
    excel_filename = "schedule.xlsx"
    generator.save_to_excel(excel_filename)
    store_schedule_history(generator.table.get_string())
    document = FSInputFile(excel_filename)
    await message.reply_document(document=document, caption="Расписание в Excel формате.")

def build_students_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    current_students = STUDENTS.get_all()
    row = []
    for idx, student in enumerate(current_students):
        button = InlineKeyboardButton(text=f"Удалить: {student}", callback_data=f"delete:{student}")
        row.append(button)
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="Добавить ученика", callback_data="add_student")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def build_confirm_deletion_keyboard(student: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text="Подтвердить", callback_data=f"confirm_delete:{student}"),
            InlineKeyboardButton(text="Отмена", callback_data=f"cancel_delete:{student}")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@router.message(Command("edit_students"))
async def edit_students_command(message: types.Message):
    keyboard = build_students_keyboard()
    await message.reply("Редактирование списка учащихся:", reply_markup=keyboard)

@router.callback_query(lambda c: c.data and c.data.startswith("delete:"))
async def request_delete(callback: types.CallbackQuery):
    student_to_delete = callback.data.split("delete:")[1]
    confirm_keyboard = build_confirm_deletion_keyboard(student_to_delete)
    await callback.answer()
    await callback.message.edit_text(
        f"Вы действительно хотите удалить ученика: {student_to_delete}?",
        reply_markup=confirm_keyboard
    )

@router.callback_query(lambda c: c.data and c.data.startswith("confirm_delete:"))
async def confirm_delete(callback: types.CallbackQuery):
    student_to_delete = callback.data.split("confirm_delete:")[1]
    if STUDENTS.remove(student_to_delete):
        await callback.answer(f"Удалено: {student_to_delete}", show_alert=True)
    else:
        await callback.answer("Студент не найден.", show_alert=True)
    keyboard = build_students_keyboard()
    await callback.message.edit_text("Редактирование списка учащихся:", reply_markup=keyboard)

@router.callback_query(lambda c: c.data and c.data.startswith("cancel_delete:"))
async def cancel_delete(callback: types.CallbackQuery):
    keyboard = build_students_keyboard()
    await callback.answer("Удаление отменено.", show_alert=True)
    await callback.message.edit_text("Редактирование списка учащихся:", reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "add_student")
async def process_add_request(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ФИО нового ученика(ов) через запятую:")
    await state.set_state(StudentUpdate.adding_students)

@router.message(StudentUpdate.adding_students)
async def add_student_handler(message: types.Message, state: FSMContext):
    new_students = [name.strip() for name in message.text.split(',') if name.strip()]
    if not new_students:
        await message.reply("Список учащихся не может быть пустым. Попробуйте еще раз.")
        return
    STUDENTS.add(new_students)
    await message.reply("Новые ученики успешно добавлены.")
    await state.clear()
    keyboard = build_students_keyboard()
    await message.reply("Обновленный список учащихся:", reply_markup=keyboard)

@router.message(Command("stats"))
async def stats_command(message: types.Message):
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM schedule_history")
        schedule_count = cursor.fetchone()[0]
        students_count = len(STUDENTS.get_all())
        response = (
            f"Статистика использования бота:\n"
            f"Количество учеников: {students_count}\n"
            f"Количество сгенерированных расписаний: {schedule_count}"
        )
        await message.reply(response)
    except Exception as e:
        logging.exception("Ошибка при получении статистики.")
        await message.reply("Произошла ошибка при получении статистики.")

@router.message(Command("audit"))
async def audit_command(message: types.Message):
    try:
        log_file = "bot.log"
        if not os.path.exists(log_file):
            await message.reply("Файл логов не найден.")
            return
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        last_lines = lines[-20:] if len(lines) >= 20 else lines
        text = "Логи последних событий:\n" + "".join(last_lines)
        if len(text) > 4000:
            text = text[-4000:]
        await message.reply(text)
    except Exception as e:
        logging.exception("Ошибка при получении логов.")
        await message.reply("Произошла ошибка при получении логов.")



async def main():
    dp = Dispatcher(storage=storage)
    dp.update.middleware.register(AllowedUsersMiddleware())
    dp.update.middleware.register(GlobalErrorHandler())
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
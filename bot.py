import html
import uuid
import re
import sqlite3

from math import ceil

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from config import BOT_TOKEN

from database import (
    init_db, upsert_entry,
    list_entries, list_by_letter, find_entries,
    count_all, count_by_letter, count_find,
    delete_by_en, delete_by_id,
    get_random_entry, get_entry_by_id,
    update_entry, search_entries_both
)

from parser import parse_entry

PAGE_SIZE = 15

# {user_id: {token: query}}
FIND_CACHE: dict[int, dict[str, str]] = {}


# -------------------- FSM --------------------

class UiState(StatesGroup):
    waiting_bulk_add = State()
    waiting_delete = State()
    waiting_edit_query = State()
    waiting_edit_value = State()
    waiting_find = State()


# -------------------- formatting --------------------

def esc(s: str) -> str:
    return html.escape(s or "")


def format_item(row, reveal_ru: bool = True) -> str:
    """
    row: (id, en, ru, example, tags)
    """
    entry_id, en, ru, example, tags = row
    parts = [f"• <b>{esc(en)}</b>"]
    if reveal_ru:
        parts[0] += f" — {esc(ru)}"
    if example:
        parts.append(f"  <i>{esc(example)}</i>")
    if tags:
        parts.append(f"  <code>{esc(tags)}</code>")
    return "\n".join(parts)


def format_list(rows, title: str | None = None) -> str:
    if not rows:
        return (f"{esc(title)}\n\n" if title else "") + \
            "Пока пусто.\nДобавляй так:\n<b>word — перевод | ex: пример | tag: тег</b>"
    body = "\n\n".join(format_item(r) for r in rows)
    if title:
        return f"{esc(title)}\n\n{body}"
    return body


def pages(total: int) -> int:
    return max(1, ceil(total / PAGE_SIZE))


# -------------------- keyboards --------------------

def kb_menu() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="📚 Список", callback_data="MENU|LIST")
    b.button(text="🔤 Буквы", callback_data="MENU|LETTERS")
    b.button(text="➕ Массово", callback_data="MENU|BULK")
    b.button(text="🔎 Найти", callback_data="MENU|FIND")
    b.button(text="✏️ Правка", callback_data="MENU|EDIT")
    b.button(text="🗑 Удалить", callback_data="MENU|DELETE")
    b.button(text="🧠 Квиз", callback_data="MENU|QUIZ")
    b.adjust(2, 2, 2, 1)
    return b


def kb_menu_row() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="🏠 Меню", callback_data="MENU|HOME")
    return b


def kb_cancel() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="CANCEL")
    b.button(text="🏠 Меню", callback_data="MENU|HOME")
    return b


def kb_letters() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    letters = [chr(c) for c in range(ord("A"), ord("Z") + 1)]
    for i, L in enumerate(letters, start=1):
        b.button(text=L, callback_data=f"LET|{L}|0")
        if i % 6 == 0:
            b.row()
    b.row()
    b.attach(kb_menu_row())
    return b


def kb_pager(mode: str, page: int, total_pages: int, extra: str = "") -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()

    left_page = max(0, page - 1)
    right_page = min(total_pages - 1, page + 1)

    if page > 0:
        b.button(text="◀️", callback_data=f"{mode}|{extra}{left_page}")
    else:
        b.button(text=" ", callback_data="NOP")

    b.button(text=f"{page + 1}/{total_pages}", callback_data="NOP")

    if page < total_pages - 1:
        b.button(text="▶️", callback_data=f"{mode}|{extra}{right_page}")
    else:
        b.button(text=" ", callback_data="NOP")

    b.row()
    b.attach(kb_menu_row())
    return b


def kb_all(page: int, total_pages: int) -> InlineKeyboardBuilder:
    b = kb_pager("ALL", page, total_pages)
    b.attach(kb_letters())
    return b


def kb_letter(letter: str, page: int, total_pages: int) -> InlineKeyboardBuilder:
    b = kb_pager("LET", page, total_pages, extra=f"{letter}|")
    b.attach(kb_letters())
    return b


def kb_find(token: str, page: int, total_pages: int) -> InlineKeyboardBuilder:
    b = kb_pager("FIND", page, total_pages, extra=f"{token}|")
    return b


def kb_quiz(entry_id: int, revealed: bool) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    if not revealed:
        b.button(text="Показать перевод ✅", callback_data=f"QUIZ|SHOW|{entry_id}")
        b.row()
    b.button(text="Следующее ➡️", callback_data="QUIZ|NEXT|0")
    b.button(text="Удалить 🗑️", callback_data=f"QUIZ|DEL|{entry_id}")
    b.row()
    b.attach(kb_menu_row())
    return b


def kb_edit_pick(rows) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    for (entry_id, en, ru, example, tags) in rows:
        label = f"{en} — {ru}"
        if len(label) > 45:
            label = label[:42] + "..."
        b.button(text=label, callback_data=f"EDIT|PICK|{entry_id}")
        b.row()
    b.row()
    b.attach(kb_menu_row())
    return b


def kb_edit_fields(entry_id: int) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="EN ✏️", callback_data=f"EDIT|FIELD|en|{entry_id}")
    b.button(text="RU ✏️", callback_data=f"EDIT|FIELD|ru|{entry_id}")
    b.button(text="EXAMPLE ✏️", callback_data=f"EDIT|FIELD|example|{entry_id}")
    b.button(text="TAG ✏️", callback_data=f"EDIT|FIELD|tags|{entry_id}")
    b.button(text="❌ Отмена", callback_data="CANCEL")
    b.button(text="🏠 Меню", callback_data="MENU|HOME")
    b.adjust(2, 2, 2)
    return b


# -------------------- render pages --------------------

async def send_all(message: Message, user_id: int, page: int = 0):
    total = count_all(user_id)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = list_entries(user_id, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    text = format_list(rows, title=f"Словарь (всего: {total})")
    await message.answer(text, parse_mode="HTML", reply_markup=kb_all(page, tp).as_markup())


async def edit_all(call: CallbackQuery, page: int):
    uid = call.from_user.id
    total = count_all(uid)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = list_entries(uid, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    text = format_list(rows, title=f"Словарь (всего: {total})")
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_all(page, tp).as_markup())
    await call.answer()


async def edit_letter(call: CallbackQuery, letter: str, page: int):
    uid = call.from_user.id
    total = count_by_letter(uid, letter)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = list_by_letter(uid, letter, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    text = format_list(rows, title=f"Буква {letter} (всего: {total})")
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_letter(letter, page, tp).as_markup())
    await call.answer()


async def send_find(message: Message, q: str, page: int = 0):
    uid = message.from_user.id
    token = uuid.uuid4().hex[:8]
    FIND_CACHE.setdefault(uid, {})[token] = q

    total = count_find(uid, q)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = find_entries(uid, q, limit=PAGE_SIZE, offset=page * PAGE_SIZE)

    text = format_list(rows, title=f"Поиск: “{q}” (всего: {total})")
    await message.answer(text, parse_mode="HTML", reply_markup=kb_find(token, page, tp).as_markup())


async def edit_find(call: CallbackQuery, token: str, page: int):
    uid = call.from_user.id
    q = FIND_CACHE.get(uid, {}).get(token)
    if not q:
        await call.answer("Поиск устарел. Нажми «Найти слово» ещё раз.", show_alert=True)
        return

    total = count_find(uid, q)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = find_entries(uid, q, limit=PAGE_SIZE, offset=page * PAGE_SIZE)

    text = format_list(rows, title=f"Поиск: “{q}” (всего: {total})")
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_find(token, page, tp).as_markup())
    await call.answer()


# -------------------- menu actions --------------------

async def show_menu(message: Message, state: FSMContext | None = None):
    if state:
        await state.clear()
    await message.answer("Выбери действие:", reply_markup=kb_menu().as_markup())


async def cb_menu(call: CallbackQuery, state: FSMContext):
    _, action = call.data.split("|", 1)

    # сбрасываем ожидания ввода
    await state.clear()

    if action == "HOME":
        await call.message.answer("Выбери действие:", reply_markup=kb_menu().as_markup())
        await call.answer()
        return

    if action == "LIST":
        await call.answer()
        await send_all(call.message, user_id=call.from_user.id, page=0)
        return

    if action == "LETTERS":
        await call.answer()
        await call.message.answer("Выбери букву:", reply_markup=kb_letters().as_markup())
        return

    if action == "FIND":
        await call.answer()
        await state.set_state(UiState.waiting_find)
        await call.message.answer(
            "Введите слово <b>на русском или английском</b>, а я найду его в словаре:",
            parse_mode="HTML",
            reply_markup=kb_cancel().as_markup()
        )
        return

    if action == "DELETE":
        await call.answer()
        await state.set_state(UiState.waiting_delete)
        await call.message.answer(
            "Введите <b>английское слово/фразу</b>, которую нужно удалить (точно как в словаре):",
            parse_mode="HTML",
            reply_markup=kb_cancel().as_markup()
        )
        return

    if action == "EDIT":
        await call.answer()
        await state.set_state(UiState.waiting_edit_query)
        await call.message.answer(
            "Введите слово <b>на русском или английском</b>, которое хотите отредактировать:",
            parse_mode="HTML",
            reply_markup=kb_cancel().as_markup()
        )
        return

    if action == "QUIZ":
        await call.answer()
        row = get_random_entry(call.from_user.id)
        if not row:
            await call.message.answer("Словарь пуст — сначала добавь пару слов.", parse_mode="HTML")
            return
        text = "Карточка:\n\n" + format_item(row, reveal_ru=False)
        await call.message.answer(text, parse_mode="HTML", reply_markup=kb_quiz(row[0], revealed=False).as_markup())
        return

    if action == "BULK":
        await call.answer()
        await state.set_state(UiState.waiting_bulk_add)
        await call.message.answer(
            "Вставь список слов (по одной строке):\n"
            "<b>word — перевод</b>\n\n"
            "Можно также:\n"
            "• <b>word — перевод | ex: пример | tag: тег</b>\n"
            "• или просто <b>word — перевод</b>\n\n"
            "Чтобы очистить поле example/tags — используй '-' в режиме редактирования.\n\n"
            "Отправь одним сообщением 👇",
            parse_mode="HTML",
            reply_markup=kb_cancel().as_markup()
        )
        return

    await call.answer()


async def cb_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer("Отменено")
    await call.message.answer("Ок, отмена. Выбери действие:", reply_markup=kb_menu().as_markup())


# -------------------- state text handlers --------------------

async def on_find_query(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    await state.clear()
    if not q:
        await message.answer("Пустой запрос. Попробуй ещё раз.", reply_markup=kb_menu().as_markup())
        return
    await send_find(message, q, page=0)


async def on_delete_query(message: Message, state: FSMContext):
    en = (message.text or "").strip()
    await state.clear()
    if not en:
        await message.answer("Пустой ввод. Попробуй ещё раз.", reply_markup=kb_menu().as_markup())
        return

    n = delete_by_en(message.from_user.id, en)
    if n:
        await message.answer(f"Удалено ✅: <b>{esc(en)}</b>", parse_mode="HTML", reply_markup=kb_menu().as_markup())
    else:
        await message.answer(f"Не найдено: <b>{esc(en)}</b>", parse_mode="HTML", reply_markup=kb_menu().as_markup())


async def on_edit_query(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    await state.clear()

    if not q:
        await message.answer("Пустой запрос. Попробуй ещё раз.", reply_markup=kb_menu().as_markup())
        return

    rows = search_entries_both(message.from_user.id, q, limit=10)
    if not rows:
        await message.answer("Ничего не нашла. Попробуй другое слово.", reply_markup=kb_menu().as_markup())
        return

    await message.answer("Выбери запись для редактирования:", reply_markup=kb_edit_pick(rows).as_markup())


async def on_edit_value(message: Message, state: FSMContext):
    data = await state.get_data()
    entry_id = data.get("edit_entry_id")
    field = data.get("edit_field")

    value = (message.text or "").strip()
    await state.clear()

    if not entry_id or not field:
        await message.answer("Не смогла понять, что редактируем. Открой меню и попробуй снова.",
                             reply_markup=kb_menu().as_markup())
        return

    if value == "-":
        value = ""

    kwargs = {}
    if field in ("en", "ru", "example", "tags"):
        kwargs[field] = value
    else:
        await message.answer("Неизвестное поле.", reply_markup=kb_menu().as_markup())
        return

    try:
        n = update_entry(message.from_user.id, entry_id, **kwargs)
    except sqlite3.IntegrityError:
        # скорее всего конфликт UNIQUE(user_id, en_norm) при смене EN
        if field == "en":
            await message.answer(
                "Такое <b>английское слово</b> уже есть в словаре. "
                "Выбери другое значение.",
                parse_mode="HTML",
                reply_markup=kb_menu().as_markup()
            )
            return
        await message.answer("Не удалось обновить из-за ограничения базы данных.", reply_markup=kb_menu().as_markup())
        return

    if not n:
        await message.answer("Не удалось обновить (возможно, запись удалена).", reply_markup=kb_menu().as_markup())
        return

    row = get_entry_by_id(message.from_user.id, entry_id)
    if not row:
        await message.answer("Обновила, но запись не нашла.", reply_markup=kb_menu().as_markup())
        return

    await message.answer(
        "Обновлено ✅\n\n" + format_item(row, reveal_ru=True),
        parse_mode="HTML",
        reply_markup=kb_menu().as_markup()
    )


# -------------------- callbacks (pagination + quiz + letters + edit) --------------------

async def cb_nop(call: CallbackQuery):
    await call.answer()


async def cb_all(call: CallbackQuery):
    _, page_s = call.data.split("|", 1)
    await edit_all(call, int(page_s))


async def cb_letter(call: CallbackQuery):
    _, letter, page_s = call.data.split("|", 2)
    await edit_letter(call, letter, int(page_s))


async def cb_find(call: CallbackQuery):
    _, token, page_s = call.data.split("|", 2)
    await edit_find(call, token, int(page_s))


async def cb_quiz(call: CallbackQuery):
    _, action, id_s = call.data.split("|", 2)
    uid = call.from_user.id

    if action == "NEXT":
        row = get_random_entry(uid)
        if not row:
            await call.answer("Словарь пуст.", show_alert=True)
            return
        text = "Карточка:\n\n" + format_item(row, reveal_ru=False)
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_quiz(row[0], revealed=False).as_markup())
        await call.answer()
        return

    entry_id = int(id_s)

    if action == "SHOW":
        row = get_entry_by_id(uid, entry_id)
        if not row:
            await call.answer("Карточка не найдена (возможно, удалена).", show_alert=True)
            return
        text = "Карточка:\n\n" + format_item(row, reveal_ru=True)
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_quiz(entry_id, revealed=True).as_markup())
        await call.answer()
        return

    if action == "DEL":
        n = delete_by_id(uid, entry_id)
        if n:
            await call.answer("Удалено ✅")
        else:
            await call.answer("Не найдено (уже удалено).", show_alert=True)

        row = get_random_entry(uid)
        if not row:
            await call.message.edit_text("Словарь пуст.", parse_mode="HTML")
            return
        text = "Карточка:\n\n" + format_item(row, reveal_ru=False)
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_quiz(row[0], revealed=False).as_markup())
        return


async def cb_edit(call: CallbackQuery, state: FSMContext):
    # EDIT|PICK|<id>
    # EDIT|FIELD|<field>|<id>
    parts = call.data.split("|")
    action = parts[1]

    # при навигации редактирования — не чистим state без нужды
    if action == "PICK":
        entry_id = int(parts[2])
        row = get_entry_by_id(call.from_user.id, entry_id)
        if not row:
            await call.answer("Запись не найдена.", show_alert=True)
            return

        await call.message.answer(
            "Что редактируем?\n\n" + format_item(row, reveal_ru=True),
            parse_mode="HTML",
            reply_markup=kb_edit_fields(entry_id).as_markup()
        )
        await call.answer()
        return

    if action == "FIELD":
        field = parts[2]
        entry_id = int(parts[3])

        await state.set_state(UiState.waiting_edit_value)
        await state.update_data(edit_entry_id=entry_id, edit_field=field)

        prompt = {
            "en": "Введите новое <b>английское</b> слово/фразу:",
            "ru": "Введите новый <b>русский перевод</b>:",
            "example": "Введите новый <b>пример</b> (или '-' чтобы очистить):",
            "tags": "Введите новые <b>теги</b> (или '-' чтобы очистить):",
        }.get(field, "Введите новое значение:")

        await call.message.answer(prompt, parse_mode="HTML", reply_markup=kb_cancel().as_markup())
        await call.answer()
        return

    await call.answer()


# -------------------- adding by plain text --------------------

def parse_bulk_lines(text: str):
    items = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue

        # убираем маркеры/нумерацию
        line = line.lstrip("•*-").strip()
        line = re.sub(r"^\d+\s*[).\-]\s*", "", line)

        parsed = parse_entry(line)
        if parsed:
            items.append(parsed)
            continue

        # fallback: "en — ru" или "en - ru"
        if "—" in line:
            en, ru = line.split("—", 1)
            en, ru = en.strip(), ru.strip()
            if en and ru:
                items.append({"en": en, "ru": ru, "example": None, "tags": None})
                continue

        if " - " in line:
            en, ru = line.split(" - ", 1)
            en, ru = en.strip(), ru.strip()
            if en and ru:
                items.append({"en": en, "ru": ru, "example": None, "tags": None})
                continue

        items.append({"_error": raw})

    return items


async def on_bulk_add(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    await state.clear()

    if not text:
        await message.answer(
            "Пусто. Вставь список и отправь одним сообщением.",
            reply_markup=kb_menu().as_markup()
        )
        return

    items = parse_bulk_lines(text)
    uid = message.from_user.id

    saved = 0
    skipped = 0
    errors = []

    for it in items:
        if "_error" in it:
            skipped += 1
            errors.append(it["_error"])
            continue

        try:
            upsert_entry(
                user_id=uid,
                en=it["en"],
                ru=it["ru"],
                example=it.get("example"),
                tags=it.get("tags")
            )
            saved += 1
        except Exception as e:
            skipped += 1
            errors.append(f"{it.get('en', '?')} — {it.get('ru', '?')} ({e})")

    await message.answer(
        f"Готово ✅\nСохранено/обновлено: <b>{saved}</b>\nПропущено: <b>{skipped}</b>",
        parse_mode="HTML",
        reply_markup=kb_menu().as_markup()
    )

    if errors:
        preview = "\n".join(f"• {esc(str(x))}" for x in errors[:10])
        tail = "\n<i>…и ещё есть</i>" if len(errors) > 10 else ""
        await message.answer("Не распознала строки:\n" + preview + tail, parse_mode="HTML")


async def on_text_add(message: Message):
    """
    Добавление слова/фразы обычным сообщением: EN — RU | ex: ... | tag: ...
    """
    parsed = parse_entry(message.text)
    if not parsed:
        await message.answer(
            "Не понял формат.\n"
            "Добавляй так:\n<b>word — перевод | ex: пример | tag: тег</b>\n\n"
            "Или пользуйся кнопками 👇",
            parse_mode="HTML",
            reply_markup=kb_menu().as_markup()
        )
        return

    upsert_entry(
        user_id=message.from_user.id,
        en=parsed["en"],
        ru=parsed["ru"],
        example=parsed["example"],
        tags=parsed["tags"]
    )

    preview = "Сохранено ✅\n\n" + format_item((0, parsed["en"], parsed["ru"], parsed["example"], parsed["tags"]))
    await message.answer(preview, parse_mode="HTML", reply_markup=kb_menu().as_markup())


# -------------------- commands --------------------

async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Привет! Я твой личный словарь.\n\n"
        "Добавляй слова сообщением:\n"
        "<b>word — перевод | ex: пример | tag: тег</b>\n\n"
        "Или пользуйся кнопками 👇",
        parse_mode="HTML",
        reply_markup=kb_menu().as_markup()
    )


# -------------------- main --------------------

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    # /start
    dp.message.register(cmd_start, Command("start"))

    # меню
    dp.callback_query.register(cb_menu, F.data.startswith("MENU|"))
    dp.callback_query.register(cb_cancel, F.data == "CANCEL")

    # пагинация/квиз/редактирование callbacks
    dp.callback_query.register(cb_nop, F.data == "NOP")
    dp.callback_query.register(cb_all, F.data.startswith("ALL|"))
    dp.callback_query.register(cb_letter, F.data.startswith("LET|"))
    dp.callback_query.register(cb_find, F.data.startswith("FIND|"))
    dp.callback_query.register(cb_quiz, F.data.startswith("QUIZ|"))
    dp.callback_query.register(cb_edit, F.data.startswith("EDIT|"))

    # FSM: ввод по сценариям
    dp.message.register(on_find_query, UiState.waiting_find, F.text)
    dp.message.register(on_delete_query, UiState.waiting_delete, F.text)
    dp.message.register(on_edit_query, UiState.waiting_edit_query, F.text)
    dp.message.register(on_edit_value, UiState.waiting_edit_value, F.text)
    dp.message.register(on_bulk_add, UiState.waiting_bulk_add, F.text)

    # добавление по умолчанию
    dp.message.register(on_text_add, F.text)

    dp.run_polling(bot)


if __name__ == "__main__":
    main()

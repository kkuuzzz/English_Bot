import html
import uuid
from math import ceil

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import BOT_TOKEN

from database import (
    init_db, upsert_entry,
    list_entries, list_by_letter, find_entries,
    count_all, count_by_letter, count_find,
    delete_by_en, delete_by_id,
    get_random_entry, get_entry_by_id
)
from parser import parse_entry

PAGE_SIZE = 15  # можно поменять

# Хранилище поисковых запросов для пагинации /find
# {user_id: {token: query}}
FIND_CACHE: dict[int, dict[str, str]] = {}


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
        # можно сделать '#tag', но пусть будет code
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

def kb_letters() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    letters = [chr(c) for c in range(ord("A"), ord("Z") + 1)]
    for i, L in enumerate(letters, start=1):
        b.button(text=L, callback_data=f"LET|{L}|0")  # page 0
        if i % 6 == 0:
            b.row()
    return b


def kb_pager(mode: str, page: int, total_pages: int, extra: str = "") -> InlineKeyboardBuilder:
    """
    mode:
      ALL -> "ALL|page"
      LET -> "LET|A|page"
      FIND -> "FIND|token|page"
    extra is already included in callback_data by caller, here we just build arrows.
    """
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
    return b


def kb_all(page: int, total_pages: int) -> InlineKeyboardBuilder:
    b = kb_pager("ALL", page, total_pages)
    # добавим A–Z ниже
    b.attach(kb_letters())
    return b


def kb_letter(letter: str, page: int, total_pages: int) -> InlineKeyboardBuilder:
    # extra: "A|"
    b = kb_pager("LET", page, total_pages, extra=f"{letter}|")
    b.attach(kb_letters())
    return b


def kb_find(token: str, page: int, total_pages: int) -> InlineKeyboardBuilder:
    # extra: "token|"
    b = kb_pager("FIND", page, total_pages, extra=f"{token}|")
    return b


def kb_quiz(entry_id: int, revealed: bool) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    if not revealed:
        b.button(text="Показать перевод ✅", callback_data=f"QUIZ|SHOW|{entry_id}")
        b.row()
    b.button(text="Следующее ➡️", callback_data="QUIZ|NEXT|0")
    b.button(text="Удалить 🗑️", callback_data=f"QUIZ|DEL|{entry_id}")
    return b


# -------------------- render pages --------------------

async def send_all(message: Message, page: int = 0):
    total = count_all(message.from_user.id)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = list_entries(message.from_user.id, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
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


async def send_letter(message: Message, letter: str, page: int = 0):
    total = count_by_letter(message.from_user.id, letter)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = list_by_letter(message.from_user.id, letter, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    text = format_list(rows, title=f"Буква {letter} (всего: {total})")
    await message.answer(text, parse_mode="HTML", reply_markup=kb_letter(letter, page, tp).as_markup())


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
        await call.answer("Поиск устарел. Повтори /find …", show_alert=True)
        return

    total = count_find(uid, q)
    tp = pages(total)
    page = min(max(page, 0), tp - 1)
    rows = find_entries(uid, q, limit=PAGE_SIZE, offset=page * PAGE_SIZE)

    text = format_list(rows, title=f"Поиск: “{q}” (всего: {total})")
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_find(token, page, tp).as_markup())
    await call.answer()


# -------------------- commands --------------------

async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я твой личный словарь.\n\n"
        "Добавляй так:\n"
        "<b>word — перевод | ex: пример | tag: тег</b>\n\n"
        "Команды:\n"
        "/list — список + кнопки A–Z\n"
        "/letter A — слова на букву\n"
        "/find apple — поиск\n"
        "/delete apple — удалить\n"
        "/quiz — режим повторения\n",
        parse_mode="HTML"
    )


async def cmd_list(message: Message):
    await send_all(message, page=0)


async def cmd_letter(message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Напиши так: /letter A", parse_mode="HTML")
        return
    letter = parts[1].strip()[:1].upper()
    if not ("A" <= letter <= "Z"):
        await message.answer("Нужна латинская буква A–Z.", parse_mode="HTML")
        return
    await send_letter(message, letter, page=0)


async def cmd_find(message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Напиши так: /find apple", parse_mode="HTML")
        return
    q = parts[1].strip()
    await send_find(message, q, page=0)


async def cmd_delete(message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Напиши так: /delete apple", parse_mode="HTML")
        return
    en = parts[1].strip()
    n = delete_by_en(message.from_user.id, en)
    if n:
        await message.answer(f"Удалено ✅: <b>{esc(en)}</b>", parse_mode="HTML")
    else:
        await message.answer(f"Не найдено слово: <b>{esc(en)}</b>", parse_mode="HTML")


async def cmd_quiz(message: Message):
    row = get_random_entry(message.from_user.id)
    if not row:
        await message.answer("Словарь пуст — сначала добавь пару слов", parse_mode="HTML")
        return
    text = "Карточка:\n\n" + format_item(row, reveal_ru=False)
    await message.answer(text, parse_mode="HTML", reply_markup=kb_quiz(row[0], revealed=False).as_markup())


# -------------------- adding by plain text --------------------

async def on_text(message: Message):
    parsed = parse_entry(message.text)
    if not parsed:
        await message.answer(
            "Не понял формат\n"
            "Попробуй так:\n<b>word — перевод | ex: пример | tag: тег</b>",
            parse_mode="HTML"
        )
        return

    upsert_entry(
        user_id=message.from_user.id,
        en=parsed["en"],
        ru=parsed["ru"],
        example=parsed["example"],
        tags=parsed["tags"]
    )

    preview = f"Сохранено ✅\n\n" + format_item((0, parsed["en"], parsed["ru"], parsed["example"], parsed["tags"]))
    await message.answer(preview, parse_mode="HTML")


# -------------------- callbacks --------------------

async def cb_nop(call: CallbackQuery):
    await call.answer()


async def cb_all(call: CallbackQuery):
    # ALL|page
    _, page_s = call.data.split("|", 1)
    await edit_all(call, int(page_s))


async def cb_letter(call: CallbackQuery):
    # LET|A|page
    _, letter, page_s = call.data.split("|", 2)
    await edit_letter(call, letter, int(page_s))


async def cb_find(call: CallbackQuery):
    # FIND|token|page
    _, token, page_s = call.data.split("|", 2)
    await edit_find(call, token, int(page_s))


async def cb_quiz(call: CallbackQuery):
    # QUIZ|SHOW|id
    # QUIZ|NEXT|0
    # QUIZ|DEL|id
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
            await call.answer("Не найдена эта карточка (возможно, удалена).", show_alert=True)
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

        # после удаления — сразу следующая карточка
        row = get_random_entry(uid)
        if not row:
            await call.message.edit_text("Словарь пуст.", parse_mode="HTML")
            return
        text = "Карточка:\n\n" + format_item(row, reveal_ru=False)
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_quiz(row[0], revealed=False).as_markup())
        return


# -------------------- main --------------------

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_list, Command("list"))
    dp.message.register(cmd_letter, Command("letter"))
    dp.message.register(cmd_find, Command("find"))
    dp.message.register(cmd_delete, Command("delete"))
    dp.message.register(cmd_quiz, Command("quiz"))

    dp.callback_query.register(cb_nop, F.data == "NOP")
    dp.callback_query.register(cb_all, F.data.startswith("ALL|"))
    dp.callback_query.register(cb_letter, F.data.startswith("LET|"))
    dp.callback_query.register(cb_find, F.data.startswith("FIND|"))
    dp.callback_query.register(cb_quiz, F.data.startswith("QUIZ|"))

    dp.message.register(on_text, F.text)

    dp.run_polling(bot)


if __name__ == "__main__":
    main()

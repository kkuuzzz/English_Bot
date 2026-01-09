import os
import sqlite3
from datetime import datetime

# Путь к файлу базы данных.
# Можно переопределить через переменную окружения DB_PATH
DB_PATH = os.getenv("DB_PATH", "dict.db")


def get_conn():
    """
    Создаёт и возвращает новое соединение с SQLite-базой данных.

    Используется во всех запросах.
    Соединение открывается и закрывается автоматически через контекстный менеджер.
    """
    return sqlite3.connect(DB_PATH)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """
    Проверяет, существует ли указанный столбец в таблице SQLite.

    Используется для миграций схемы БД (например, при добавлении новых колонок).
    Возвращает True, если колонка существует, иначе False.
    """
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]  # row[1] — имя колонки
    return column in cols


def init_db():
    """
    Инициализирует базу данных словаря.

    Выполняет:
    - создание таблицы entries (если она ещё не существует)
    - миграцию старых БД (добавление ru_norm)
    - создание индексов для ускорения поиска и фильтрации
    """
    with get_conn() as conn:
        # Основная таблица словаря
        conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            en TEXT NOT NULL,
            en_norm TEXT NOT NULL,
            ru TEXT NOT NULL,
            -- ru_norm может отсутствовать в старых БД, добавим миграцией ниже
            example TEXT,
            tags TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, en_norm)
        );
        """)

        # ---- migration: add ru_norm if missing ----
        # Поиск по русскому переводу
        if not _column_exists(conn, "entries", "ru_norm"):
            conn.execute("ALTER TABLE entries ADD COLUMN ru_norm TEXT;")
            conn.execute("UPDATE entries SET ru_norm=lower(ru) WHERE ru_norm IS NULL;")

        # Индексы для быстрого поиска и сортировки
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en ON entries(user_id, en_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_ru ON entries(user_id, ru_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en_first ON entries(user_id, substr(en_norm,1,1));")


def upsert_entry(user_id: int, en: str, ru: str, example: str | None, tags: str | None):
    """
    Добавляет новое слово в словарь или обновляет существующее.

    Уникальность определяется по паре (user_id, en_norm).
    Если слово с таким английским написанием уже существует —
    запись будет обновлена (перевод, пример, теги).
    """
    en_norm = en.strip().lower()
    ru_norm = ru.strip().lower()
    now = datetime.utcnow().isoformat(timespec="seconds")

    with get_conn() as conn:
        conn.execute("""
        INSERT INTO entries (user_id, en, en_norm, ru, ru_norm, example, tags, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, en_norm) DO UPDATE SET
            en=excluded.en,
            ru=excluded.ru,
            ru_norm=excluded.ru_norm,
            example=excluded.example,
            tags=excluded.tags
        """, (user_id, en.strip(), en_norm, ru.strip(), ru_norm, example, tags, now))


def delete_by_en(user_id: int, en: str) -> int:
    """
    Удаляет запись по английскому слову.

    Удаление производится с учётом user_id.
    Возвращает количество удалённых строк (0 или 1).
    """
    en_norm = en.strip().lower()
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM entries WHERE user_id=? AND en_norm=?",
            (user_id, en_norm)
        )
        return cur.rowcount


def delete_by_id(user_id: int, entry_id: int) -> int:
    """
    Удаляет запись по её ID.

    Используется, например, в квизе или при точечном удалении.
    Возвращает количество удалённых строк (0 или 1).
    """
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM entries WHERE user_id=? AND id=?",
            (user_id, entry_id)
        )
        return cur.rowcount


def count_all(user_id: int) -> int:
    """
    Возвращает общее количество слов в словаре пользователя.

    Используется для отображения статистики и пагинации.
    """
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE user_id=?",
            (user_id,)
        )
        return int(cur.fetchone()[0])


def count_by_letter(user_id: int, letter: str) -> int:
    """
    Возвращает количество слов пользователя,
    начинающихся на указанную букву (A–Z).

    Сравнение идёт по нормализованному английскому слову (en_norm).
    """
    letter_norm = letter.strip().lower()[:1]
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT COUNT(*) FROM entries
        WHERE user_id=? AND substr(en_norm,1,1)=?
        """, (user_id, letter_norm))
        return int(cur.fetchone()[0])


def count_find(user_id: int, q: str) -> int:
    """
    Возвращает количество слов, подходящих под поисковый запрос.

    Поиск выполняется по:
    - en_norm (английское слово)
    - ru_norm (русский перевод)
    """
    qn = f"%{q.strip().lower()}%"
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT COUNT(*) FROM entries
        WHERE user_id=? AND (en_norm LIKE ? OR ru_norm LIKE ?)
        """, (user_id, qn, qn))
        return int(cur.fetchone()[0])


def list_entries(user_id: int, limit: int = 20, offset: int = 0):
    """
    Возвращает список слов пользователя с пагинацией.

    Сортировка — по алфавиту (en_norm).
    """
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=?
        ORDER BY en_norm ASC
        LIMIT ? OFFSET ?
        """, (user_id, limit, offset))
        return cur.fetchall()


def list_by_letter(user_id: int, letter: str, limit: int = 20, offset: int = 0):
    """
    Возвращает список слов пользователя,
    начинающихся на заданную букву.

    Поддерживает пагинацию.
    """
    letter_norm = letter.strip().lower()[:1]
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=? AND substr(en_norm,1,1)=?
        ORDER BY en_norm ASC
        LIMIT ? OFFSET ?
        """, (user_id, letter_norm, limit, offset))
        return cur.fetchall()


def find_entries(user_id: int, q: str, limit: int = 20, offset: int = 0):
    """
    Возвращает список слов, соответствующих поисковому запросу.

    Поиск выполняется по английскому слову и русскому переводу.
    """
    qn = f"%{q.strip().lower()}%"
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=? AND (en_norm LIKE ? OR ru_norm LIKE ?)
        ORDER BY en_norm ASC
        LIMIT ? OFFSET ?
        """, (user_id, qn, qn, limit, offset))
        return cur.fetchall()


def get_random_entry(user_id: int):
    """
    Возвращает случайное слово из словаря пользователя.

    Используется в режиме квиза.
    Если слов нет — возвращает None.
    """
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=?
        ORDER BY RANDOM()
        LIMIT 1
        """, (user_id,))
        return cur.fetchone()


def get_entry_by_id(user_id: int, entry_id: int):
    """
    Возвращает одну запись по её ID.

    Используется при редактировании и отображении карточек.
    """
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=? AND id=?
        """, (user_id, entry_id))
        return cur.fetchone()


def update_entry(
    user_id: int,
    entry_id: int,
    en: str | None = None,
    ru: str | None = None,
    example: str | None = None,
    tags: str | None = None
) -> int:
    """
    Частично обновляет запись словаря.

    Обновляются только те поля, которые переданы (не None).
    Поля, равные None, остаются без изменений.

    Возвращает количество обновлённых строк (0 или 1).
    """
    sets = []
    params = []

    if en is not None:
        sets += ["en=?", "en_norm=?"]
        params += [en.strip(), en.strip().lower()]

    if ru is not None:
        sets += ["ru=?", "ru_norm=?"]
        params += [ru.strip(), ru.strip().lower()]

    if example is not None:
        sets.append("example=?")
        params.append(example.strip() if example.strip() else None)

    if tags is not None:
        sets.append("tags=?")
        params.append(tags.strip() if tags.strip() else None)

    if not sets:
        return 0

    params += [user_id, entry_id]

    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE entries SET {', '.join(sets)} WHERE user_id=? AND id=?",
            params
        )
        return cur.rowcount


def search_entries_both(user_id: int, q: str, limit: int = 10):
    """
    Выполняет быстрый поиск слов по английскому и русскому значению.

    Используется при редактировании для выбора записи.
    """
    qn = f"%{q.strip().lower()}%"
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=? AND (en_norm LIKE ? OR ru_norm LIKE ?)
        ORDER BY en_norm ASC
        LIMIT ?
        """, (user_id, qn, qn, limit))
        return cur.fetchall()


def delete_all_entries(user_id: int) -> int:
    """
    Удаляет ВСЕ записи пользователя из словаря.

    Используется при полной очистке словаря.
    Возвращает количество удалённых строк.
    """
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM entries WHERE user_id=?",
            (user_id,)
        )
        return cur.rowcount

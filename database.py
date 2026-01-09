import os
import sqlite3
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "dict.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]  # row[1] = name
    return column in cols


def init_db():
    with get_conn() as conn:
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
        if not _column_exists(conn, "entries", "ru_norm"):
            conn.execute("ALTER TABLE entries ADD COLUMN ru_norm TEXT;")
            conn.execute("UPDATE entries SET ru_norm=lower(ru) WHERE ru_norm IS NULL;")

        # indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en ON entries(user_id, en_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_ru ON entries(user_id, ru_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en_first ON entries(user_id, substr(en_norm,1,1));")


def upsert_entry(user_id: int, en: str, ru: str, example: str | None, tags: str | None):
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
    en_norm = en.strip().lower()
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM entries WHERE user_id=? AND en_norm=?", (user_id, en_norm))
        return cur.rowcount


def delete_by_id(user_id: int, entry_id: int) -> int:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM entries WHERE user_id=? AND id=?", (user_id, entry_id))
        return cur.rowcount


def count_all(user_id: int) -> int:
    with get_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM entries WHERE user_id=?", (user_id,))
        return int(cur.fetchone()[0])


def count_by_letter(user_id: int, letter: str) -> int:
    letter_norm = letter.strip().lower()[:1]
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT COUNT(*) FROM entries
        WHERE user_id=? AND substr(en_norm,1,1)=?
        """, (user_id, letter_norm))
        return int(cur.fetchone()[0])


def count_find(user_id: int, q: str) -> int:
    qn = f"%{q.strip().lower()}%"
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT COUNT(*) FROM entries
        WHERE user_id=? AND (en_norm LIKE ? OR ru_norm LIKE ?)
        """, (user_id, qn, qn))
        return int(cur.fetchone()[0])


def list_entries(user_id: int, limit: int = 20, offset: int = 0):
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
    with get_conn() as conn:
        cur = conn.execute("""
        SELECT id, en, ru, example, tags
        FROM entries
        WHERE user_id=? AND id=?
        """, (user_id, entry_id))
        return cur.fetchone()


def update_entry(user_id: int, entry_id: int, en: str | None = None, ru: str | None = None,
                 example: str | None = None, tags: str | None = None) -> int:
    """
    Частичное обновление записи. None = поле не трогаем.
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
    Быстрый поиск для выбора записи при редактировании: по en_norm и ru_norm.
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

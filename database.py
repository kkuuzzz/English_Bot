import os
import sqlite3
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "dict.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            en TEXT NOT NULL,
            en_norm TEXT NOT NULL,
            ru TEXT NOT NULL,
            example TEXT,
            tags TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, en_norm)
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en ON entries(user_id, en_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_en_first ON entries(user_id, substr(en_norm,1,1));")


def upsert_entry(user_id: int, en: str, ru: str, example: str | None, tags: str | None):
    en_norm = en.strip().lower()
    now = datetime.utcnow().isoformat(timespec="seconds")

    with get_conn() as conn:
        conn.execute("""
        INSERT INTO entries (user_id, en, en_norm, ru, example, tags, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, en_norm) DO UPDATE SET
            en=excluded.en,
            ru=excluded.ru,
            example=excluded.example,
            tags=excluded.tags
        """, (user_id, en.strip(), en_norm, ru.strip(), example, tags, now))


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
        WHERE user_id=? AND en_norm LIKE ?
        """, (user_id, qn))
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
        WHERE user_id=? AND en_norm LIKE ?
        ORDER BY en_norm ASC
        LIMIT ? OFFSET ?
        """, (user_id, qn, limit, offset))
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

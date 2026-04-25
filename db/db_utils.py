"""
Database utility module.
Handles connection, initialization, and auto-creation of the schema.
"""

import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "pipeline.db"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def get_db() -> sqlite3.Connection:
    """
    Return a connection to the SQLite database.
    If the database file doesn't exist yet, create it and apply the schema.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new = not DB_PATH.exists()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")       # better concurrent read perf
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")       # wait up to 5s if locked

    if is_new:
        with open(SCHEMA_PATH) as f:
            conn.executescript(f.read())
        print(f"[db] Initialized new database at {DB_PATH}")

    return conn


def ensure_schema(conn: sqlite3.Connection):
    """
    Idempotently apply the schema (all statements use IF NOT EXISTS).
    Safe to call on every run.
    """
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())

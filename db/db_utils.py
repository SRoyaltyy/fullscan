"""
Database utility module.
Handles connection, schema initialization, and one-time migrations
from the old split-database layout to the unified pipeline.db.
"""

import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "pipeline.db"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def get_db() -> sqlite3.Connection:
    """
    Return a connection to the SQLite database.
    Creates the file and applies the schema if it doesn't exist yet.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new = not DB_PATH.exists()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    if is_new:
        _apply_schema(conn)
        print(f"[db] Initialized new database at {DB_PATH}")
    else:
        _migrate_if_needed(conn)

    return conn


def ensure_schema(conn: sqlite3.Connection):
    """
    Idempotently apply the schema (all statements use IF NOT EXISTS).
    Safe to call on every collector run.
    """
    _migrate_if_needed(conn)
    _apply_schema(conn)


def _apply_schema(conn: sqlite3.Connection):
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())


# ── One-time migrations ────────────────────────────────────────
# These handle the transition from the old split-database layout
# (db/schema.sql → pipeline.db  +  database/init_db.py → fullscan.db)
# to the single unified pipeline.db.

def _migrate_if_needed(conn: sqlite3.Connection):
    _migrate_macro_indicators(conn)
    _migrate_company_financials(conn)


def _migrate_macro_indicators(conn: sqlite3.Connection):
    """
    Old schema: UNIQUE(series_id, date), no 'indicator' column.
    New schema: PRIMARY KEY(indicator, date).
    Preserves existing FRED data by mapping series_id → indicator.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(macro_indicators)").fetchall()}
    if not cols:                    # table doesn't exist yet — nothing to migrate
        return
    if "indicator" in cols:         # already on new schema
        return

    print("[db] Migrating macro_indicators to unified schema...")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS _macro_new (
            indicator    TEXT NOT NULL,
            series_id    TEXT,
            series_name  TEXT,
            date         TEXT NOT NULL,
            value        REAL,
            source       TEXT,
            collected_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (indicator, date)
        );

        INSERT OR IGNORE INTO _macro_new
            (indicator, series_id, series_name, date, value, source, collected_at)
        SELECT
            series_id, series_id, series_name, date, value, 'FRED', updated_at
        FROM macro_indicators;

        DROP TABLE macro_indicators;
        ALTER TABLE _macro_new RENAME TO macro_indicators;
    """)
    print("[db] macro_indicators migrated.")


def _migrate_company_financials(conn: sqlite3.Connection):
    """
    Old schema: (ticker, period, revenue, net_income, …) — per-period statements.
    New schema: (ticker PRIMARY KEY, company, sector, …) — Finviz screener snapshot.
    The old table was never populated, so it's safe to drop.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(company_financials)").fetchall()}
    if not cols:
        return
    if "period" in cols and "sector" not in cols:
        row_count = conn.execute("SELECT COUNT(*) FROM company_financials").fetchone()[0]
        if row_count == 0:
            print("[db] Replacing empty company_financials with Finviz screener schema...")
            conn.execute("DROP TABLE company_financials")
            conn.commit()
        else:
            print("[db] WARNING: company_financials has the old schema with data in it.")
            print("     Back up data/pipeline.db, then delete it and re-run collectors.")

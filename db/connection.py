"""
Unified PostgreSQL connection for all collectors.
Every collector imports: from db.connection import get_connection

Supabase Seoul pooler times out from GitHub-hosted runners. Collectors
must write local files even when the pooler is dead — never sys.exit(1).
"""

import os
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None


class DatabaseUnavailable(Exception):
    """Pooler down or DATABASE_URL missing. Callers should skip the DB write."""


def get_connection(retries: int = 3, required: bool = False):
    """Return a psycopg2 connection, or None after retries.

    required=True raises DatabaseUnavailable instead of returning None
    (legacy callers that cannot degrade). Default is optional.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("[db] DATABASE_URL not set — skipping Postgres write")
        if required:
            raise DatabaseUnavailable("DATABASE_URL not set")
        return None
    if psycopg2 is None:
        print("[db] psycopg2 not installed — skipping Postgres write")
        if required:
            raise DatabaseUnavailable("psycopg2 not installed")
        return None
    last = None
    for attempt in range(max(1, retries)):
        try:
            return psycopg2.connect(database_url, connect_timeout=10)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[db] connect failed (try {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    print(f"[db] giving up after {retries} tries — continuing without Postgres ({last})")
    if required:
        raise DatabaseUnavailable(str(last))
    return None

"""
Unified PostgreSQL connection for all collectors.
Every collector imports: from db.connection import get_connection
"""

import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2


def get_connection():
    """Return a new psycopg2 connection to the Supabase PostgreSQL database."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("FATAL: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    return psycopg2.connect(database_url, connect_timeout=10)

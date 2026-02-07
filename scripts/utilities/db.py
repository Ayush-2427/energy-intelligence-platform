import os

import psycopg2


def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


def get_conn():
    return psycopg2.connect(get_database_url())

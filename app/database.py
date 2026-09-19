from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from .config import settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"


def utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def tx(conn: aiosqlite.Connection):
    """Explicit IMMEDIATE transaction over a shared autocommit connection.

    All writers (API request handlers and the delivery worker) run on the
    same connection, so transaction blocks are serialized with a lock:
    without it, awaits between BEGIN/COMMIT would let another coroutine
    interleave its statements (or a second BEGIN) into the open transaction.
    """
    async with conn.tx_lock:
        try:
            await conn.execute("BEGIN IMMEDIATE")
            yield
            await conn.execute("COMMIT")
        except Exception:
            await conn.execute("ROLLBACK")
            raise


async def init_db() -> aiosqlite.Connection:
    db_path = Path(settings.database_path)
    if db_path.parent != Path("."):
        db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = await aiosqlite.connect(
        settings.database_path,
        isolation_level=None,
        timeout=30.0,
    )
    conn.row_factory = aiosqlite.Row
    conn.tx_lock = asyncio.Lock()
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA busy_timeout=30000")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.executescript(SCHEMA_PATH.read_text())

    # Lightweight migration for databases created before filters existed:
    # schema.sql only runs CREATE TABLE IF NOT EXISTS, so add the column to
    # pre-existing subscription tables.
    async with tx(conn):
        async with conn.execute("PRAGMA table_info(subscriptions)") as cur:
            columns = {row["name"] for row in await cur.fetchall()}
        if "filter" not in columns:
            await conn.execute(
                'ALTER TABLE subscriptions ADD COLUMN "filter" TEXT'
            )

    # Crash recovery: a process dying mid-send leaves in_flight rows.
    # Re-arm them so the head is retried instead of stalling forever.
    async with tx(conn):
        await conn.execute(
            """
            UPDATE deliveries
               SET status = 'pending', next_attempt_at = NULL, updated_at = ?
             WHERE status = 'in_flight'
            """,
            (utcnow_iso(),),
        )
    return conn

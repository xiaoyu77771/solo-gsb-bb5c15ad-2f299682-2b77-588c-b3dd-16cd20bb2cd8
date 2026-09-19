from __future__ import annotations

import json
from contextlib import asynccontextmanager

import aiosqlite
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response

from .database import init_db, tx, utcnow_iso
from .filtering import matches as filter_matches
from .models import (
    DeliveryOut,
    EventAck,
    EventIn,
    GapSkipIn,
    GapSkipOut,
    ReplayBulkBody,
    ReplayResult,
    SubscriptionIn,
    SubscriptionOut,
)
from .worker import DeliveryWorker


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db()
    app.state.worker = DeliveryWorker(app.state.db)
    await app.state.worker.start()
    try:
        yield
    finally:
        await app.state.worker.stop()
        await app.state.db.close()


app = FastAPI(
    title="Ordered Webhook Delivery API",
    version="1.0.0",
    description=(
        "Register callbacks per source and ingest events carrying a "
        "monotonic sequence. Events are delivered to each subscription "
        "strictly in order, with exponential-backoff retries, dead-letter "
        "parking and manual replay."
    ),
    lifespan=lifespan,
)


def db(request: Request) -> aiosqlite.Connection:
    return request.app.state.db


def worker(request: Request) -> DeliveryWorker:
    return request.app.state.worker


# --------------------------------------------------------------- health/root

@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok"}


@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "ordered-webhook",
        "docs": "/docs",
        "endpoints": [
            "POST   /subscriptions",
            "GET    /subscriptions",
            "DELETE /subscriptions/{id}",
            "POST   /events",
            "POST   /sources/{source}/gaps/{sequence}/skip",
            "GET    /backlog",
            "GET    /dead-letters",
            "POST   /dead-letters/{id}/replay",
            "POST   /dead-letters/replay",
        ],
    }


# ------------------------------------------------------------- subscriptions

@app.post(
    "/subscriptions",
    response_model=SubscriptionOut,
    status_code=201,
    tags=["subscriptions"],
)
async def create_subscription(
    body: SubscriptionIn, conn: aiosqlite.Connection = Depends(db)
) -> SubscriptionOut:
    url = str(body.callback_url)
    if not url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=422, detail="callback_url must be http(s)")

    now = utcnow_iso()
    filter_json = json.dumps(body.filter) if body.filter is not None else None
    async with tx(conn):
        # New subscribers only see events ingested from now on. Start their
        # watermark just beyond the highest sequence already known for the
        # source (or at 1 for a brand new source).
        async with conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE source = ?",
            (body.source,),
        ) as cur:
            next_seq = (await cur.fetchone())[0]

        try:
            cur = await conn.execute(
                """
                INSERT INTO subscriptions
                    (source, callback_url, "filter", created_at, cursor_seq, next_seq)
                VALUES (?, ?, ?, ?, NULL, ?)
                """,
                (body.source, url, filter_json, now, next_seq),
            )
        except aiosqlite.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="subscription for this source and callback_url already exists",
            )
        sub_id = cur.lastrowid

    return SubscriptionOut(
        id=sub_id,
        source=body.source,
        callback_url=url,
        next_seq=next_seq,
        cursor_seq=None,
        filter=body.filter,
        created_at=now,
    )


@app.get(
    "/subscriptions",
    response_model=list[SubscriptionOut],
    tags=["subscriptions"],
)
async def list_subscriptions(
    source: str | None = None,
    conn: aiosqlite.Connection = Depends(db),
) -> list[SubscriptionOut]:
    if source is not None:
        sql = (
            "SELECT * FROM subscriptions WHERE source = ? ORDER BY id"
        )
        args: tuple = (source,)
    else:
        sql = "SELECT * FROM subscriptions ORDER BY id"
        args = ()
    async with conn.execute(sql, args) as cur:
        rows = await cur.fetchall()
    return [_subscription_out(r) for r in rows]


def _subscription_out(row: aiosqlite.Row) -> SubscriptionOut:
    data = dict(row)
    raw = data.get("filter")
    data["filter"] = json.loads(raw) if raw else None
    return SubscriptionOut(**data)


@app.delete(
    "/subscriptions/{subscription_id}",
    status_code=204,
    tags=["subscriptions"],
)
async def delete_subscription(
    subscription_id: int, conn: aiosqlite.Connection = Depends(db)
) -> None:
    async with tx(conn):
        cur = await conn.execute(
            "DELETE FROM subscriptions WHERE id = ?", (subscription_id,)
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="subscription not found")


# -------------------------------------------------------------------- events

@app.post(
    "/events",
    response_model=EventAck,
    status_code=202,
    tags=["events"],
)
async def ingest_event(
    body: EventIn,
    conn: aiosqlite.Connection = Depends(db),
    delivery_worker: DeliveryWorker = Depends(worker),
) -> EventAck:
    now = utcnow_iso()
    payload = json.dumps(body.payload) if body.payload is not None else None

    # Idempotency first: same (source, event_id) always returns the same ack,
    # regardless of whether the original POST finished ingesting rows.
    async with tx(conn):
        # A permanently skipped gap can never be filled: reject the event
        # instead of resurrecting the sequence.
        async with conn.execute(
            "SELECT reason FROM gap_skips WHERE source = ? AND sequence = ?",
            (body.source, body.sequence),
        ) as cur:
            gap = await cur.fetchone()
        if gap is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"sequence {body.sequence} for source {body.source!r} was "
                    f"permanently skipped (reason: {gap['reason']}); skipped "
                    "sequences cannot be ingested"
                ),
            )

        async with conn.execute(
            "SELECT id FROM events WHERE source = ? AND event_id = ?",
            (body.source, body.event_id),
        ) as cur:
            existing = await cur.fetchone()

        if existing is not None:
            event_pk = existing["id"]
            duplicate = True
        else:
            try:
                cur = await conn.execute(
                    """
                    INSERT INTO events
                        (source, sequence, event_id, payload, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (body.source, body.sequence, body.event_id, payload, now),
                )
            except aiosqlite.IntegrityError:
                # Same (source, sequence) but a different event_id: a client
                # protocol error rather than a safe retry.
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"sequence {body.sequence} for source "
                        f"{body.source!r} already exists with another event_id"
                    ),
                )
            event_pk = cur.lastrowid
            duplicate = False

        queued = 0
        async with conn.execute(
            'SELECT id, "filter" FROM subscriptions WHERE source = ?',
            (body.source,),
        ) as cur:
            subs = await cur.fetchall()
        for sub in subs:
            # Declarative filter: matching events queue for delivery as usual;
            # non-matching ones are persisted as 'skipped' so the worker can
            # step the watermark over them once they become the head — without
            # this the filtered sequence would leave a permanent gap.
            spec = json.loads(sub["filter"]) if sub["filter"] else None
            status = (
                "pending" if filter_matches(spec, body.payload) else "skipped"
            )
            try:
                await conn.execute(
                    """
                    INSERT INTO deliveries
                        (subscription_id, event_id, status, attempts,
                         next_attempt_at, last_error, created_at, updated_at)
                    VALUES (?, ?, ?, 0, NULL, NULL, ?, ?)
                    """,
                    (sub["id"], event_pk, status, now, now),
                )
                if status == "pending":
                    queued += 1
            except aiosqlite.IntegrityError:
                # Row already queued (e.g. duplicate ingest racing a retry).
                pass

    delivery_worker.wake()
    return EventAck(
        source=body.source,
        sequence=body.sequence,
        event_id=body.event_id,
        duplicate=duplicate,
        queued=queued,
        status="accepted",
    )


# -------------------------------------------------------------------- gaps

@app.post(
    "/sources/{source}/gaps/{sequence}/skip",
    response_model=GapSkipOut,
    tags=["gaps"],
)
async def skip_gap(
    body: GapSkipIn,
    response: Response,
    source: str = Path(min_length=1, max_length=256),
    sequence: int = Path(ge=1),
    conn: aiosqlite.Connection = Depends(db),
    delivery_worker: DeliveryWorker = Depends(worker),
) -> GapSkipOut:
    """Permanently skip a missing sequence so queued successors can flow.

    Allowed only while the sequence has no event and a higher sequence is
    already known for the source (i.e. it is a real gap, not the live tail).
    The marker and a ``skipped`` delivery row for every current subscription
    of the source are written in one transaction; the worker then steps each
    watermark over the gap once it becomes the head. Markers are immutable:
    the same reason replays idempotently, a different one conflicts.
    """
    async with tx(conn):
        async with conn.execute(
            "SELECT id, reason, created_at FROM gap_skips "
            "WHERE source = ? AND sequence = ?",
            (source, sequence),
        ) as cur:
            existing = await cur.fetchone()

        if existing is not None:
            if existing["reason"] != body.reason:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"sequence {sequence} for source {source!r} is already "
                        "skipped with a different reason; skip markers are "
                        "immutable"
                    ),
                )
            response.status_code = 200
            return GapSkipOut(
                id=existing["id"],
                source=source,
                sequence=sequence,
                reason=existing["reason"],
                created_at=existing["created_at"],
                duplicate=True,
            )

        async with conn.execute(
            "SELECT 1 FROM events WHERE source = ? AND sequence = ?",
            (source, sequence),
        ) as cur:
            event_present = await cur.fetchone() is not None
        if event_present:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"sequence {sequence} for source {source!r} already has "
                    "an event; only missing sequences can be skipped"
                ),
            )

        async with conn.execute(
            "SELECT 1 FROM events WHERE source = ? AND sequence > ? LIMIT 1",
            (source, sequence),
        ) as cur:
            higher_present = await cur.fetchone() is not None
        if not higher_present:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"source {source!r} has no event above sequence "
                    f"{sequence}; the gap may still fill, refusing to skip"
                ),
            )

        now = utcnow_iso()
        # Placeholder event so the gap occupies its (source, sequence) slot
        # and per-subscription delivery rows can reference it. Its reserved
        # event_id can never collide with a client-supplied one because the
        # gap check above rejects any ingest at this sequence.
        cur = await conn.execute(
            """
            INSERT INTO events (source, sequence, event_id, payload, created_at)
            VALUES (?, ?, ?, NULL, ?)
            """,
            (source, sequence, f"__gap_skip__:{sequence}", now),
        )
        event_pk = cur.lastrowid

        cur = await conn.execute(
            """
            INSERT INTO gap_skips (source, sequence, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (source, sequence, body.reason, now),
        )
        skip_id = cur.lastrowid

        # Fan out a persisted 'skipped' record to every current subscription
        # of the source, in the same transaction as the marker. The worker
        # steps a watermark over such a row only once it sits exactly at
        # next_seq, so a retrying head is never jumped.
        async with conn.execute(
            "SELECT id FROM subscriptions WHERE source = ?", (source,)
        ) as cur:
            sub_ids = [row["id"] for row in await cur.fetchall()]
        for sub_id in sub_ids:
            await conn.execute(
                """
                INSERT INTO deliveries
                    (subscription_id, event_id, status, attempts,
                     next_attempt_at, last_error, created_at, updated_at)
                VALUES (?, ?, 'skipped', 0, NULL, NULL, ?, ?)
                """,
                (sub_id, event_pk, now, now),
            )

    delivery_worker.wake()
    response.status_code = 201
    return GapSkipOut(
        id=skip_id,
        source=source,
        sequence=sequence,
        reason=body.reason,
        created_at=now,
        duplicate=False,
    )


# -------------------------------------------------------------- inspection

_DELIVERY_SELECT = """
    SELECT d.id, d.subscription_id, e.source, e.sequence, e.event_id,
           d.status, d.attempts, d.next_attempt_at, d.last_error,
           d.created_at, d.updated_at
      FROM deliveries d
      JOIN events e ON e.id = d.event_id
"""


def _rows_to_out(rows) -> list[DeliveryOut]:
    return [DeliveryOut(**dict(r)) for r in rows]


@app.get(
    "/backlog",
    response_model=list[DeliveryOut],
    tags=["inspection"],
)
async def get_backlog(
    source: str | None = None,
    subscription_id: int | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    conn: aiosqlite.Connection = Depends(db),
) -> list[DeliveryOut]:
    """Events buffered for delivery (waiting, retrying, or in flight),
    ordered by subscription and sequence so the head of each queue reads
    first."""
    where = ["d.status IN ('pending', 'in_flight')"]
    args: list = []
    if source is not None:
        where.append("e.source = ?")
        args.append(source)
    if subscription_id is not None:
        where.append("d.subscription_id = ?")
        args.append(subscription_id)
    sql = (
        _DELIVERY_SELECT
        + " WHERE "
        + " AND ".join(where)
        + " ORDER BY d.subscription_id, e.sequence LIMIT ?"
    )
    args.append(limit)
    async with conn.execute(sql, args) as cur:
        rows = await cur.fetchall()
    return _rows_to_out(rows)


@app.get(
    "/dead-letters",
    response_model=list[DeliveryOut],
    tags=["dead-letters"],
)
async def list_dead_letters(
    source: str | None = None,
    subscription_id: int | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    conn: aiosqlite.Connection = Depends(db),
) -> list[DeliveryOut]:
    where = ["d.status = 'dead'"]
    args: list = []
    if source is not None:
        where.append("e.source = ?")
        args.append(source)
    if subscription_id is not None:
        where.append("d.subscription_id = ?")
        args.append(subscription_id)
    sql = (
        _DELIVERY_SELECT
        + " WHERE "
        + " AND ".join(where)
        + " ORDER BY d.subscription_id, e.sequence LIMIT ?"
    )
    args.append(limit)
    async with conn.execute(sql, args) as cur:
        rows = await cur.fetchall()
    return _rows_to_out(rows)


@app.post(
    "/dead-letters/{delivery_id}/replay",
    response_model=ReplayResult,
    tags=["dead-letters"],
)
async def replay_one(
    delivery_id: int,
    conn: aiosqlite.Connection = Depends(db),
    delivery_worker: DeliveryWorker = Depends(worker),
) -> ReplayResult:
    ok = await delivery_worker.replay(delivery_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="dead-letter delivery not found",
        )
    return ReplayResult(replayed=1, delivery_ids=[delivery_id])


@app.post(
    "/dead-letters/replay",
    response_model=ReplayResult,
    tags=["dead-letters"],
)
async def replay_bulk(
    body: ReplayBulkBody,
    conn: aiosqlite.Connection = Depends(db),
    delivery_worker: DeliveryWorker = Depends(worker),
) -> ReplayResult:
    replayed: list[int] = []
    for delivery_id in body.delivery_ids:
        if await delivery_worker.replay(delivery_id):
            replayed.append(delivery_id)
    return ReplayResult(replayed=len(replayed), delivery_ids=replayed)

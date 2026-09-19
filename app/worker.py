from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import aiosqlite

from .config import settings
from .database import tx
from .delivery_client import DeliveryClient

STATUSES = ("pending", "in_flight", "delivered", "dead", "skipped")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def backoff_delay(attempts: int) -> float:
    delay = settings.backoff_base * (settings.backoff_factor ** max(attempts - 1, 0))
    return min(delay, settings.backoff_max)


class DeliveryWorker:
    """Strictly ordered, at-least-once webhook delivery.

    Each subscription owns a ``next_seq`` watermark: delivery N is released
    only when sequence ``next_seq`` is present and due. Out-of-order events
    stay buffered until the gap fills; a failing head retries with
    exponential backoff and blocks all later events; once its retry budget
    is spent it moves to dead letters and the watermark steps over it so the
    tail can continue. Replayed dead events are delivered again but never
    move the watermark (they sit behind it).

    Subscriptions with a payload filter persist non-matching events as
    ``skipped``. A skipped event still occupies its sequence number, so once
    it becomes the head (sequence == next_seq) the watermark steps over it
    atomically — never earlier, so a retrying predecessor is never jumped
    and filtered sequences leave no permanent gap.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self.http = DeliveryClient()
        self.notify = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        # One in-flight head per subscription at most. Without this a fast
        # pump could re-claim the same in_flight head while its HTTP attempt
        # is still in progress.
        self._head_locks: dict[int, asyncio.Lock] = {}
        # Keep references to the short backoff-wake tasks so the event loop
        # never garbage-collects them mid-sleep.
        self._wake_tasks: set[asyncio.Task] = set()

    def _lock_for(self, subscription_id: int) -> asyncio.Lock:
        try:
            return self._head_locks[subscription_id]
        except KeyError:
            lock = asyncio.Lock()
            self._head_locks[subscription_id] = lock
            return lock

    def wake(self) -> None:
        self.notify.set()

    async def start(self) -> None:
        await self.http.start()
        self._tasks = [
            asyncio.create_task(self._pump_loop(), name="delivery-pump"),
            asyncio.create_task(self._reconcile_loop(), name="delivery-reconcile"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        self.wake()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self.http.close()

    # ------------------------------------------------------------------ loops

    async def _pump_loop(self) -> None:
        while not self._stop.is_set():
            try:
                made_progress = await self.pump_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient DB error must never kill the background pump.
                made_progress = False
            if made_progress:
                continue  # drain buffered successors immediately
            try:
                await asyncio.wait_for(
                    self.notify.wait(), timeout=settings.idle_interval
                )
            except asyncio.TimeoutError:
                pass
            self.notify.clear()

    async def _reconcile_loop(self) -> None:
        """Crash-safe fallback for the in-memory backoff sleep timers."""
        while not self._stop.is_set():
            await asyncio.sleep(settings.reconcile_interval)
            self.wake()

    # ------------------------------------------------------------- core pump

    async def pump_once(self) -> bool:
        """Release at most one attempt per eligible subscription head.

        Returns True when at least one attempt was dispatched, signalling the
        loop to pump again so a healthy backlog drains without idle delay.
        """
        # Step watermarks over skipped heads first: a filtered event at
        # next_seq would otherwise block its successors forever.
        advanced = await self._advance_skipped()
        heads = await self._claim_heads()
        if not heads:
            return advanced

        dispatched = False
        tasks = []
        for row in heads:
            lock = self._lock_for(row["subscription_id"])
            if lock.locked():
                # The current head is still mid-attempt; don't double-send.
                continue
            tasks.append(asyncio.create_task(self._attempt_guarded(lock, row)))
            dispatched = True
        if tasks:
            await asyncio.gather(*tasks)
        return dispatched or advanced

    async def _advance_skipped(self) -> bool:
        """Move each subscription watermark past skipped deliveries.

        Only a skipped delivery sitting exactly at ``next_seq`` is stepped
        over; sequences below the watermark may still be retrying and must
        never be jumped. Consecutive skipped sequences fold into one atomic
        update per subscription. Returns True if any watermark moved.
        """
        async with tx(self.db):
            async with self.db.execute(
                """
                SELECT s.id AS subscription_id, s.next_seq AS next_seq
                  FROM subscriptions s
                  JOIN deliveries d ON d.subscription_id = s.id
                  JOIN events e ON e.id = d.event_id
                 WHERE d.status = 'skipped'
                   AND e.sequence = s.next_seq
                """
            ) as cur:
                rows = await cur.fetchall()

            advanced = False
            for row in rows:
                sub_id = row["subscription_id"]
                seq = row["next_seq"]
                while await self._is_skipped_at(sub_id, seq):
                    seq += 1
                if seq == row["next_seq"]:
                    continue
                cur = await self.db.execute(
                    """
                    UPDATE subscriptions
                       SET next_seq = ?, cursor_seq = ?
                     WHERE id = ? AND next_seq = ?
                    """,
                    (seq, seq - 1, sub_id, row["next_seq"]),
                )
                advanced = advanced or cur.rowcount > 0
        return advanced

    async def _is_skipped_at(self, subscription_id: int, sequence: int) -> bool:
        async with self.db.execute(
            """
            SELECT 1
              FROM deliveries d
              JOIN events e ON e.id = d.event_id
             WHERE d.subscription_id = ?
               AND d.status = 'skipped'
               AND e.sequence = ?
            """,
            (subscription_id, sequence),
        ) as cur:
            return await cur.fetchone() is not None

    async def _attempt_guarded(
        self, lock: asyncio.Lock, row: aiosqlite.Row
    ) -> None:
        async with lock:
            await self._attempt(row)

    async def _claim_heads(self) -> list[aiosqlite.Row]:
        now = utcnow().isoformat()
        async with tx(self.db):
            async with self.db.execute(
                """
                SELECT d.id AS delivery_id, d.attempts, d.status AS dstatus,
                       s.id AS subscription_id, s.callback_url, s.next_seq,
                       e.source, e.sequence, e.event_id, e.payload
                  FROM subscriptions s
                  JOIN deliveries d ON d.subscription_id = s.id
                  JOIN events e ON e.id = d.event_id
                  JOIN (
                        SELECT d2.subscription_id, MIN(e2.sequence) AS head_seq
                          FROM deliveries d2
                          JOIN events e2 ON e2.id = d2.event_id
                         WHERE d2.status IN ('pending', 'in_flight')
                         GROUP BY d2.subscription_id
                       ) h ON h.subscription_id = d.subscription_id
                          AND h.head_seq = e.sequence
                 WHERE d.status IN ('pending', 'in_flight')
                   AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                """,
                (now,),
            ) as cur:
                rows = await cur.fetchall()

            heads: list[aiosqlite.Row] = []
            for row in rows:
                replay = row["sequence"] < row["next_seq"]
                # Normal path: only the exact watermark sequence is released.
                # Replayed dead events sit behind the watermark and are
                # allowed through without moving it.
                if not replay and row["sequence"] != row["next_seq"]:
                    continue
                heads.append(row)

            if heads:
                await self.db.executemany(
                    "UPDATE deliveries SET status='in_flight', updated_at=? WHERE id=?",
                    [(utcnow().isoformat(), r["delivery_id"]) for r in heads],
                )
        return heads

    async def _attempt(self, row: aiosqlite.Row) -> None:
        attempts = row["attempts"] + 1
        body = {
            "source": row["source"],
            "sequence": row["sequence"],
            "event_id": row["event_id"],
            "payload": _decode_payload(row["payload"]),
        }
        ok, error = await self.http.post(
            row["callback_url"],
            body,
            delivery_id=row["delivery_id"],
            attempt=attempts,
        )
        advances_watermark = row["sequence"] == row["next_seq"]

        if ok:
            await self._record_success(row, attempts, advances_watermark)
            self.wake()
            return

        await self._record_failure(row, attempts, error, advances_watermark)
        self.wake()

    async def _record_success(
        self, row: aiosqlite.Row, attempts: int, advances_watermark: bool
    ) -> None:
        now = utcnow().isoformat()
        async with tx(self.db):
            await self.db.execute(
                """
                UPDATE deliveries
                   SET status='delivered', attempts=?, last_error=NULL,
                       next_attempt_at=NULL, updated_at=?
                 WHERE id=?
                """,
                (attempts, now, row["delivery_id"]),
            )
            if advances_watermark:
                await self.db.execute(
                    """
                    UPDATE subscriptions
                       SET next_seq = ?, cursor_seq = ?
                     WHERE id = ?
                    """,
                    (row["sequence"] + 1, row["sequence"], row["subscription_id"]),
                )

    async def _record_failure(
        self,
        row: aiosqlite.Row,
        attempts: int,
        error: str,
        advances_watermark: bool,
    ) -> None:
        now = utcnow().isoformat()
        if attempts >= settings.max_attempts:
            async with tx(self.db):
                await self.db.execute(
                    """
                    UPDATE deliveries
                       SET status='dead', attempts=?, last_error=?,
                           next_attempt_at=NULL, updated_at=?
                     WHERE id=?
                    """,
                    (attempts, error, now, row["delivery_id"]),
                )
                if advances_watermark:
                    await self.db.execute(
                        """
                        UPDATE subscriptions
                           SET next_seq = ?, cursor_seq = ?
                         WHERE id = ?
                        """,
                        (row["sequence"] + 1, row["sequence"], row["subscription_id"]),
                    )
            return

        delay = backoff_delay(attempts)
        next_at = (utcnow() + timedelta(seconds=delay)).isoformat()
        async with tx(self.db):
            await self.db.execute(
                """
                UPDATE deliveries
                   SET status='pending', attempts=?, last_error=?,
                       next_attempt_at=?, updated_at=?
                 WHERE id=?
                """,
                (attempts, error, next_at, now, row["delivery_id"]),
            )
        wake_task = asyncio.create_task(self._wake_after(delay))
        self._wake_tasks.add(wake_task)

    async def replay(self, delivery_id: int) -> bool:
        """Move a dead delivery back into the retry queue."""
        now = utcnow().isoformat()
        async with tx(self.db):
            cur = await self.db.execute(
                """
                UPDATE deliveries
                   SET status='pending', attempts=0, last_error=NULL,
                       next_attempt_at=NULL, updated_at=?
                 WHERE id=? AND status='dead'
                """,
                (now, delivery_id),
            )
            changed = cur.rowcount
        if changed:
            self.wake()
        return changed > 0

    async def _wake_after(self, delay: float) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(delay)
            self.wake()
        except asyncio.CancelledError:
            pass
        finally:
            if task is not None:
                self._wake_tasks.discard(task)


def _decode_payload(raw: str | None):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw

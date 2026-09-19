"""End-to-end style tests for the ordered webhook service.

Runs the API via Starlette's ASGI test transport and a tiny in-process
receiver ASGI app, so no network ports or Docker are needed. Retry timing is
sped up through env vars read by app.config.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

os.environ.setdefault("DATABASE_PATH", ":memory:")
os.environ.setdefault("MAX_ATTEMPTS", "4")
os.environ.setdefault("BACKOFF_BASE", "0.05")
os.environ.setdefault("BACKOFF_FACTOR", "2")
os.environ.setdefault("BACKOFF_MAX", "1")
os.environ.setdefault("IDLE_INTERVAL", "0.05")
os.environ.setdefault("RECONCILE_INTERVAL", "0.1")
os.environ.setdefault("DELIVERY_TIMEOUT", "5")

import aiosqlite  # noqa: E402
import pytest  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402

from app.config import settings  # noqa: E402
from app.database import init_db  # noqa: E402
from app.main import app  # noqa: E402


# ---------------------------------------------------------------- receiver app

class Receiver:
    """In-process subscriber: records deliveries, can force failures.

    ``fail_until[source]`` = number of initial attempts that fail.
    ``fail_sequences[source]`` restricts forced failures to these sequences
    (default {1}), so later events can succeed while the head struggles.
    """

    def __init__(self) -> None:
        self.received: list[dict] = []
        self.fail_until: dict[str, int] = {}
        self.fail_sequences: dict[str, set[int]] = {}
        self.lock = asyncio.Lock()
        self.events: dict[str, asyncio.Event] = {}
        self.transport = ASGITransport(app=self)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"
        headers_dict = {
            k.decode().lower(): v.decode() for k, v in scope.get("headers", [])
        }
        raw = await receive()
        body = json.loads(raw["body"])
        attempt = int(headers_dict.get("x-webhook-attempt", "1"))
        source = body["source"]
        sequence = body["sequence"]

        fail_budget = self.fail_until.get(source, 0)
        fail_seqs = self.fail_sequences.get(source, {1})
        status = 200
        if attempt <= fail_budget and sequence in fail_seqs:
            status = 500

        async with self.lock:
            self.received.append(
                {**body, "_status": status, "_attempt": attempt}
            )
            ev = self.events.setdefault(
                f"{body['source']}:{body['sequence']}", asyncio.Event()
            )
        if status == 200:
            ev.set()

        payload = b'{"ok":true}' if status == 200 else b'{"error":"forced"}'
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": payload})


@pytest.fixture
def receiver():
    return Receiver()


# httpx normally resolves the URL host; with ASGITransport it doesn't open a
# socket, but it still validates http(s) URLs.
RECEIVER_URL = "http://receiver.local/hook"


@pytest.fixture
async def client(monkeypatch, receiver, tmp_path):
    # Use a file DB per test (aiosqlite memory DBs are per-connection, but the
    # app opens exactly one shared connection, so :memory: also works here).
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(settings, "database_path", db_path)

    # Make delivery traffic for receiver.local land on the in-process ASGI app.
    from app import worker as worker_mod

    original_post = worker_mod.DeliveryClient.post

    async def routed_post(self, url, body, *, delivery_id, attempt):
        if url.startswith(RECEIVER_URL):
            async with AsyncClient(transport=receiver.transport, base_url="http://test") as rc:
                resp = await rc.post(
                    RECEIVER_URL,
                    json=body,
                    headers={
                        "X-Webhook-Delivery-Id": str(delivery_id),
                        "X-Webhook-Attempt": str(attempt),
                    },
                )
            if 200 <= resp.status_code < 300:
                return True, None
            return False, f"HTTP {resp.status_code}: {resp.text}"
        return await original_post(
            self, url, body, delivery_id=delivery_id, attempt=attempt
        )

    monkeypatch.setattr(worker_mod.DeliveryClient, "post", routed_post)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        # lifespan must be driven explicitly with ASGITransport
        async with app.router.lifespan_context(app):
            yield ac

    await asyncio.sleep(0)  # let cancellations settle


# --------------------------------------------------------------------- helpers

async def subscribe(client, source="orders", url=RECEIVER_URL, filter=None) -> dict:
    body = {"source": source, "callback_url": url}
    if filter is not None:
        body["filter"] = filter
    r = await client.post("/subscriptions", json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def send_event(client, source, sequence, event_id, payload=None):
    return await client.post(
        "/events",
        json={
            "source": source,
            "sequence": sequence,
            "event_id": event_id,
            "payload": payload if payload is not None else {"n": sequence},
        },
    )


async def wait_received(receiver, source, sequence, timeout=5.0):
    key = f"{source}:{sequence}"
    ev = receiver.events.setdefault(key, asyncio.Event())
    await asyncio.wait_for(ev.wait(), timeout=timeout)


async def wait_next_seq(client, source, expected, timeout=5.0):
    """Poll until the subscription for ``source`` reports next_seq == expected."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        subs = (await client.get("/subscriptions", params={"source": source})).json()
        if subs and subs[0]["next_seq"] == expected:
            return subs[0]
        await asyncio.sleep(0.02)
    raise AssertionError(f"next_seq for {source!r} did not reach {expected}")


async def delivery_statuses(source="orders") -> dict[int, str]:
    """Read delivery rows straight from the DB: {sequence: status}."""
    async with app.state.db.execute(
        """
        SELECT e.sequence, d.status
          FROM deliveries d
          JOIN events e ON e.id = d.event_id
         WHERE e.source = ?
         ORDER BY e.sequence
        """,
        (source,),
    ) as cur:
        rows = await cur.fetchall()
    return {r["sequence"]: r["status"] for r in rows}


def delivered_sequences(receiver, source="orders") -> list[int]:
    return [
        e["sequence"]
        for e in receiver.received
        if e["source"] == source and e["_status"] == 200
    ]


# ---------------------------------------------------------------------- tests

async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_events_delivered_in_order_out_of_order_ingest(client, receiver):
    await subscribe(client)

    # Ingest 3,1,2: only #1 can move; 2 and 3 must buffer.
    for seq in (3, 1, 2):
        r = await send_event(client, "orders", seq, f"evt-{seq}")
        assert r.status_code == 202

    await wait_received(receiver, "orders", 1)
    await wait_received(receiver, "orders", 2)
    await wait_received(receiver, "orders", 3)

    assert delivered_sequences(receiver) == [1, 2, 3]
    # Event 3 was buffered first but must physically leave last.
    order = [e["sequence"] for e in receiver.received]
    assert order == [1, 2, 3]


async def test_gap_holds_queue_until_filled(client, receiver):
    await subscribe(client)
    await send_event(client, "orders", 1, "evt-1")
    await send_event(client, "orders", 3, "evt-3")

    await wait_received(receiver, "orders", 1)
    # Give the pump time to wrongly send #3 early.
    await asyncio.sleep(0.3)
    assert delivered_sequences(receiver) == [1]

    # Backlog should show #3 waiting.
    r = await client.get("/backlog", params={"source": "orders"})
    assert r.status_code == 200
    backlog = r.json()
    assert [b["sequence"] for b in backlog] == [3]
    assert backlog[0]["status"] == "pending"

    await send_event(client, "orders", 2, "evt-2")
    await wait_received(receiver, "orders", 2)
    await wait_received(receiver, "orders", 3)
    assert delivered_sequences(receiver) == [1, 2, 3]


async def test_duplicate_event_id_is_idempotent(client, receiver):
    await subscribe(client)
    r1 = await send_event(client, "orders", 1, "evt-dup", {"v": 1})
    assert r1.status_code == 202
    assert r1.json()["duplicate"] is False

    # Exact retry: same event_id and sequence.
    r2 = await send_event(client, "orders", 1, "evt-dup", {"v": 1})
    assert r2.status_code == 202
    body = r2.json()
    assert body["duplicate"] is True
    assert body["queued"] == 0

    await wait_received(receiver, "orders", 1)
    await asyncio.sleep(0.2)
    seqs = [e for e in receiver.received if e["event_id"] == "evt-dup"]
    assert len(seqs) == 1  # delivered exactly once


async def test_same_sequence_different_event_id_conflicts(client):
    await subscribe(client)
    assert (await send_event(client, "orders", 1, "a")).status_code == 202
    r = await send_event(client, "orders", 1, "b")
    assert r.status_code == 409


async def test_failure_blocks_head_and_retries_with_backoff_then_succeeds(
    client, receiver
):
    await subscribe(client)
    # Fail the first 2 attempts at sequence 1.
    receiver.fail_until["orders"] = 2

    await send_event(client, "orders", 1, "evt-1")
    await send_event(client, "orders", 2, "evt-2")

    await wait_received(receiver, "orders", 1)
    await wait_received(receiver, "orders", 2)

    attempts_seq1 = [
        e["_attempt"] for e in receiver.received if e["sequence"] == 1
    ]
    assert attempts_seq1 == [1, 2, 3]  # failed, failed, succeeded
    # Sequence 2 never overtook the struggling head.
    assert [e["sequence"] for e in receiver.received][-2:] == [1, 2]

    # The pump that delivers #2 is triggered by #1's success wake; wait
    # briefly for the backlog to drain before asserting it is empty.
    deadline = time.monotonic() + 2
    backlog = None
    while time.monotonic() < deadline:
        r = await client.get("/backlog", params={"source": "orders"})
        backlog = r.json()
        if not backlog:
            break
        await asyncio.sleep(0.02)
    assert backlog == []


async def test_exhausted_retries_go_dead_and_tail_continues(client, receiver):
    await subscribe(client)
    # Always fail sequence 1; MAX_ATTEMPTS=4 in this test module.
    receiver.fail_until["orders"] = 99

    await send_event(client, "orders", 1, "evt-1")
    await send_event(client, "orders", 2, "evt-2")

    # Wait for 4 attempts on the dead head.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if len([e for e in receiver.received if e["sequence"] == 1]) >= 4:
            break
        await asyncio.sleep(0.05)
    attempts = [e for e in receiver.received if e["sequence"] == 1]
    assert len(attempts) == 4
    assert all(e["_status"] == 500 for e in attempts)

    # Once #1 is parked, #2 proceeds and succeeds.
    await wait_received(receiver, "orders", 2, timeout=5)

    dead = (await client.get("/dead-letters", params={"source": "orders"})).json()
    assert len(dead) == 1
    assert dead[0]["sequence"] == 1
    assert dead[0]["status"] == "dead"
    assert dead[0]["attempts"] == 4
    assert dead[0]["last_error"]


async def test_dead_letter_replay_redelivers(client, receiver):
    await subscribe(client)
    receiver.fail_until["orders"] = 99
    await send_event(client, "orders", 1, "evt-1")

    deadline = time.monotonic() + 5
    dead = []
    while time.monotonic() < deadline:
        dead = (
            await client.get("/dead-letters", params={"source": "orders"})
        ).json()
        if dead:
            break
        await asyncio.sleep(0.05)
    assert dead and dead[0]["sequence"] == 1
    delivery_id = dead[0]["id"]

    # Fix the subscriber, then replay.
    receiver.fail_until["orders"] = 0
    r = await client.post(f"/dead-letters/{delivery_id}/replay")
    assert r.status_code == 200
    assert r.json()["replayed"] == 1

    await wait_received(receiver, "orders", 1, timeout=5)
    ok = [e for e in receiver.received if e["sequence"] == 1 and e["_status"] == 200]
    assert len(ok) == 1

    # Replaying a non-dead delivery is a 404.
    r2 = await client.post(f"/dead-letters/{delivery_id}/replay")
    assert r2.status_code == 404


async def test_bulk_replay(client, receiver):
    await subscribe(client, source="s2")
    receiver.fail_until["s2"] = 99
    receiver.fail_sequences["s2"] = {1, 2}
    await send_event(client, "s2", 1, "a1")
    await send_event(client, "s2", 2, "a2")

    # Wait until both are dead.
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        dead = (await client.get("/dead-letters", params={"source": "s2"})).json()
        if len(dead) == 2:
            break
        await asyncio.sleep(0.05)
    ids = [d["id"] for d in dead]

    receiver.fail_until["s2"] = 0
    r = await client.post("/dead-letters/replay", json={"delivery_ids": ids})
    assert r.status_code == 200
    assert sorted(r.json()["delivery_ids"]) == sorted(ids)

    await wait_received(receiver, "s2", 1)
    await wait_received(receiver, "s2", 2)
    assert delivered_sequences(receiver, "s2") == [1, 2]


async def test_subscriptions_filtered_by_source(client, receiver):
    await subscribe(client, source="a")
    await subscribe(client, source="b")

    await send_event(client, "a", 1, "a-1")
    await send_event(client, "b", 1, "b-1")

    await wait_received(receiver, "a", 1)
    await wait_received(receiver, "b", 1)
    assert set(delivered_sequences(receiver, "a")) == {1}
    assert set(delivered_sequences(receiver, "b")) == {1}


async def test_duplicate_subscription_conflicts(client):
    await subscribe(client)
    r = await client.post(
        "/subscriptions",
        json={"source": "orders", "callback_url": RECEIVER_URL},
    )
    assert r.status_code == 409


async def test_backlog_shows_retry_schedule(client, receiver):
    await subscribe(client)
    # Keep the head failing for several attempts so the retry-scheduled
    # backlog row is observable without racing the final success commit.
    receiver.fail_until["orders"] = 3
    await send_event(client, "orders", 1, "evt-1")

    # During retries the backlog entry carries attempts and next_attempt_at.
    deadline = time.monotonic() + 3
    saw_retry = False
    while time.monotonic() < deadline:
        backlog = (await client.get("/backlog")).json()
        if any(b["attempts"] >= 1 and b["next_attempt_at"] for b in backlog):
            saw_retry = True
            break
        await asyncio.sleep(0.02)
    assert saw_retry

    await wait_received(receiver, "orders", 1)  # eventually succeeds


# ---------------------------------------------------------------- filters

async def test_subscription_filter_stored_and_returned(client):
    plain = await subscribe(client, source="plain")
    assert plain["filter"] is None  # no filter -> matches everything

    f = {"all": [{"eq": {"path": "type", "value": "order.created"}}]}
    filtered = await subscribe(client, source="filtered", filter=f)
    assert filtered["filter"] == f

    subs = (await client.get("/subscriptions")).json()
    by_source = {s["source"]: s for s in subs}
    assert by_source["plain"]["filter"] is None
    assert by_source["filtered"]["filter"] == f


@pytest.mark.parametrize(
    "bad_filter",
    [
        {"eqx": {"path": "a", "value": 1}},          # unknown operator
        {"where": {"path": "a"}},                    # unknown operator
        {"all": []},                                 # empty combination
        {"any": []},                                 # empty combination
        {"not": {}},                                 # empty condition object
        {"eq": {"path": "a"}},                       # missing value
        {"in": {"path": "a", "values": []}},         # empty values
        {"exists": {}},                              # missing path
        {"eq": {"path": "a..b", "value": 1}},        # malformed dot path
        {"eq": {"path": "", "value": 1}},            # empty path
        {"eq": {"path": "a", "value": 1}, "in": {"path": "b", "values": [1]}},
    ],
)
async def test_invalid_filters_rejected(client, bad_filter):
    r = await client.post(
        "/subscriptions",
        json={
            "source": "orders",
            "callback_url": RECEIVER_URL,
            "filter": bad_filter,
        },
    )
    assert r.status_code == 422


async def test_filter_nesting_limit_is_five(client):
    leaf = {"eq": {"path": "a", "value": 1}}
    node = leaf
    for _ in range(4):  # leaf is depth 1, four combinators -> depth 5
        node = {"not": node}
    r = await client.post(
        "/subscriptions",
        json={"source": "orders", "callback_url": RECEIVER_URL, "filter": node},
    )
    assert r.status_code == 201, r.text

    too_deep = {"all": [node]}  # depth 6
    r = await client.post(
        "/subscriptions",
        json={
            "source": "orders2",
            "callback_url": RECEIVER_URL,
            "filter": too_deep,
        },
    )
    assert r.status_code == 422


async def test_filter_skips_non_matching_and_watermark_advances(client, receiver):
    await subscribe(client, filter={"eq": {"path": "type", "value": "keep"}})

    # Ingest out of order: 2 (match) buffers behind 1 (skipped), then 3.
    r = await send_event(client, "orders", 2, "evt-2", {"type": "keep"})
    assert r.json()["queued"] == 1
    r = await send_event(client, "orders", 1, "evt-1", {"type": "drop"})
    assert r.json()["queued"] == 0  # filtered out, not queued for delivery
    await send_event(client, "orders", 3, "evt-3", {"type": "keep"})

    await wait_received(receiver, "orders", 2)
    await wait_received(receiver, "orders", 3)
    await asyncio.sleep(0.2)

    # Sequence 1 was never delivered; 2 and 3 left in order.
    assert delivered_sequences(receiver) == [2, 3]
    assert all(e["sequence"] != 1 for e in receiver.received)

    # The worker stepped the watermark over the skipped head.
    sub = await wait_next_seq(client, "orders", 4)
    assert sub["cursor_seq"] == 3

    # Skipped rows are persisted but hidden from backlog and dead letters.
    assert await delivery_statuses() == {
        1: "skipped",
        2: "delivered",
        3: "delivered",
    }
    assert (await client.get("/backlog")).json() == []
    assert (await client.get("/dead-letters")).json() == []


async def test_filter_combinators_in_exists_not_any(client, receiver):
    f = {
        "all": [
            {"in": {"path": "prio", "values": ["high", "urgent"]}},
            {"exists": {"path": "meta.trace"}},
            {"not": {"eq": {"path": "internal", "value": True}}},
            {"any": [
                {"eq": {"path": "region", "value": "eu"}},
                {"eq": {"path": "region", "value": "us"}},
            ]},
        ]
    }
    await subscribe(client, filter=f)

    await send_event(client, "orders", 1, "e1",
                     {"prio": "high", "meta": {"trace": "t"}, "region": "eu"})
    await send_event(client, "orders", 2, "e2",
                     {"prio": "low", "meta": {"trace": "t"}, "region": "eu"})
    await send_event(client, "orders", 3, "e3",
                     {"prio": "urgent", "region": "us"})
    await send_event(client, "orders", 4, "e4",
                     {"prio": "high", "meta": {"trace": "t"},
                      "internal": True, "region": "eu"})
    await send_event(client, "orders", 5, "e5",
                     {"prio": "high", "meta": {"trace": "t"}, "region": "apac"})

    await wait_received(receiver, "orders", 1)
    await wait_next_seq(client, "orders", 6)
    await asyncio.sleep(0.2)

    assert delivered_sequences(receiver) == [1]
    assert await delivery_statuses() == {
        1: "delivered",
        2: "skipped",   # in: prio not in list
        3: "skipped",   # exists: meta.trace missing
        4: "skipped",   # not: internal is true
        5: "skipped",   # any: region neither eu nor us
    }


async def test_skipped_never_overtakes_retrying_head(client, receiver):
    await subscribe(client, filter={"eq": {"path": "keep", "value": True}})
    # The matching head fails its first two attempts.
    receiver.fail_until["orders"] = 2

    await send_event(client, "orders", 1, "e1", {"keep": True})
    await send_event(client, "orders", 2, "e2", {"keep": False})  # skipped
    await send_event(client, "orders", 3, "e3", {"keep": True})

    await wait_received(receiver, "orders", 1)
    await wait_received(receiver, "orders", 3)
    await asyncio.sleep(0.2)

    # Physical send order: only attempts at seq 1 (fail, fail, ok), then 3.
    # The skipped seq 2 must not let seq 3 jump ahead of the retrying head.
    assert [e["sequence"] for e in receiver.received] == [1, 1, 1, 3]
    assert await delivery_statuses() == {
        1: "delivered",
        2: "skipped",
        3: "delivered",
    }
    await wait_next_seq(client, "orders", 4)


async def test_filtered_events_still_reach_dead_letters(client, receiver):
    await subscribe(client, filter={"eq": {"path": "keep", "value": True}})
    receiver.fail_until["orders"] = 99  # matching head always fails

    await send_event(client, "orders", 1, "e1", {"keep": True})   # will go dead
    await send_event(client, "orders", 2, "e2", {"keep": False})  # skipped
    await send_event(client, "orders", 3, "e3", {"keep": True})   # tail

    # Once seq 1 exhausts retries the watermark steps over it and the
    # skipped seq 2, then delivers seq 3.
    await wait_received(receiver, "orders", 3, timeout=8)
    await wait_next_seq(client, "orders", 4)

    dead = (await client.get("/dead-letters", params={"source": "orders"})).json()
    assert [d["sequence"] for d in dead] == [1]
    # The skipped event must not surface in dead letters or the backlog.
    assert all(d["sequence"] != 2 for d in dead)
    assert (await client.get("/backlog")).json() == []
    statuses = await delivery_statuses()
    assert statuses[1] == "dead"
    assert statuses[2] == "skipped"
    assert statuses[3] == "delivered"

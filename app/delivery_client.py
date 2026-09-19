from __future__ import annotations

import asyncio

import httpx

from .config import settings


class DeliveryClient:
    """Thin wrapper around a shared httpx client used for callback POSTs."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=settings.delivery_timeout,
            limits=httpx.Limits(
                max_connections=100, max_keepalive_connections=20
            ),
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def post(
        self, url: str, body: dict, *, delivery_id: int, attempt: int
    ) -> tuple[bool, str | None]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "ordered-webhook/1.0",
            "X-Webhook-Delivery-Id": str(delivery_id),
            "X-Webhook-Attempt": str(attempt),
            "X-Webhook-Source": str(body["source"]),
            "X-Webhook-Sequence": str(body["sequence"]),
            "X-Webhook-Event-Id": str(body["event_id"]),
        }
        try:
            resp = await self._client.post(url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            return False, f"{type(exc).__name__}: {exc}"[:1000]

        if 200 <= resp.status_code < 300:
            return True, None
        snippet = resp.text[:200].replace("\n", " ")
        return False, f"HTTP {resp.status_code}: {snippet}"[:1000]

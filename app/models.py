from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator

from .filtering import validate_filter


class SubscriptionIn(BaseModel):
    source: str = Field(min_length=1, max_length=256)
    callback_url: HttpUrl
    # Declarative payload filter (see app.filtering); omitted = match all.
    filter: dict[str, Any] | None = None

    @field_validator("filter")
    @classmethod
    def _check_filter(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None:
            validate_filter(value)  # ValueError -> 422 for the API caller
        return value


class SubscriptionOut(BaseModel):
    id: int
    source: str
    callback_url: str
    next_seq: int
    cursor_seq: int | None
    filter: dict[str, Any] | None = None
    created_at: str


class EventIn(BaseModel):
    source: str = Field(min_length=1, max_length=256)
    sequence: int = Field(ge=1)
    event_id: str = Field(min_length=1, max_length=256)
    payload: Any | None = None


class EventAck(BaseModel):
    source: str
    sequence: int
    event_id: str
    duplicate: bool
    queued: int
    status: str


class DeliveryOut(BaseModel):
    id: int
    subscription_id: int
    source: str
    sequence: int
    event_id: str
    status: str
    attempts: int
    next_attempt_at: str | None
    last_error: str | None
    created_at: str
    updated_at: str


class ReplayResult(BaseModel):
    replayed: int
    delivery_ids: list[int]


class ReplayBulkBody(BaseModel):
    delivery_ids: list[int] = Field(default_factory=list, max_length=1000)

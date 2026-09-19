CREATE TABLE IF NOT EXISTS subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    callback_url    TEXT NOT NULL,
    "filter"        TEXT,
    created_at      TEXT NOT NULL,
    cursor_seq      INTEGER,
    next_seq        INTEGER NOT NULL DEFAULT 1,
    UNIQUE(source, callback_url)
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    event_id    TEXT NOT NULL,
    payload     TEXT,
    created_at  TEXT NOT NULL,
    UNIQUE(source, event_id),
    UNIQUE(source, sequence)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_id        INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_source ON subscriptions(source);
CREATE INDEX IF NOT EXISTS idx_events_source_seq ON events(source, sequence);
CREATE INDEX IF NOT EXISTS idx_deliveries_sub_status
    ON deliveries(subscription_id, status, next_attempt_at);

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass
class Settings:
    database_path: str = os.getenv("DATABASE_PATH", "/data/webhook.db")
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = _int("PORT", 8080)

    max_attempts: int = _int("MAX_ATTEMPTS", 5)
    backoff_base: float = _float("BACKOFF_BASE", 1.0)
    backoff_factor: float = _float("BACKOFF_FACTOR", 2.0)
    backoff_max: float = _float("BACKOFF_MAX", 300.0)
    delivery_timeout: float = _float("DELIVERY_TIMEOUT", 10.0)

    idle_interval: float = _float("IDLE_INTERVAL", 1.0)
    reconcile_interval: float = _float("RECONCILE_INTERVAL", 2.0)


settings = Settings()

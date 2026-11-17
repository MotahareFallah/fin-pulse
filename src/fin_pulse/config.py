"""Runtime settings, read from environment variables with local-dev defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://finpulse:finpulse@localhost:5434/finpulse"
    )
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6380/0")
    topic_events: str = os.getenv("TOPIC_EVENTS", "fin.events")
    topic_dlq: str = os.getenv("TOPIC_DLQ", "fin.events.dlq")
    worker_group: str = os.getenv("WORKER_GROUP", "fin-pulse-workers")


settings = Settings()

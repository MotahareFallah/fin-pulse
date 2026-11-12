import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic_events: str = os.getenv("KAFKA_TOPIC_EVENTS", "fin.events")
    topic_retry: str = os.getenv("KAFKA_TOPIC_RETRY", "fin.events.retry")
    topic_dlq: str = os.getenv("KAFKA_TOPIC_DLQ", "fin.events.dlq")
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://finpulse:finpulse@localhost:5432/finpulse"
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")


settings = Settings()

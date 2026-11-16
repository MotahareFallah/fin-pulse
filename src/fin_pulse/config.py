"""Runtime settings, read from environment variables with local-dev defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://finpulse:finpulse@localhost:5434/finpulse"
    )


settings = Settings()

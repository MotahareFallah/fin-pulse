"""FastAPI dependencies.

They read what the lifespan stored on `app.state`, so tests can replace them through
`app.dependency_overrides`.
"""

from typing import Annotated

from aiokafka import AIOKafkaProducer
from fastapi import Depends, Request

from fin_pulse.api.dedup import IdempotencyGuard


def get_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.producer


def get_guard(request: Request) -> IdempotencyGuard:
    return request.app.state.guard


Producer = Annotated[AIOKafkaProducer, Depends(get_producer)]
Guard = Annotated[IdempotencyGuard, Depends(get_guard)]

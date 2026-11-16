"""Create the Kafka producer and the Redis client with the app, close them with it."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

from fin_pulse.api.dedup import IdempotencyGuard
from fin_pulse.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
    )
    await producer.start()
    redis = aioredis.from_url(settings.redis_url)
    app.state.producer = producer
    app.state.guard = IdempotencyGuard(redis)
    try:
        yield
    finally:
        await redis.aclose()
        await producer.stop()

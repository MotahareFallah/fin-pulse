"""Ingestion API: validate -> fast dedup (Redis) -> Kafka -> 202.

Design rules (see docs/adr/0001):
- Redis is only a fast path. If Redis is down we still accept the event
  (fail-open); Postgres in the worker is the real idempotency guard.
- If we claim an event_id in Redis but Kafka rejects the write, we MUST release
  the claim. Otherwise the client's retry would be treated as a duplicate and
  the event would be silently lost.
"""
import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from fin_pulse.config import settings
from fin_pulse.models import Event, partition_key

log = logging.getLogger("fin_pulse.api")

IDEMPOTENCY_TTL_SECONDS = 24 * 3600


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
    )
    await producer.start()
    app.state.producer = producer
    app.state.redis = aioredis.from_url(settings.redis_url)
    try:
        yield
    finally:
        await producer.stop()
        await app.state.redis.aclose()


app = FastAPI(title="fin-pulse", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/events", status_code=202)
async def ingest(event: Event, request: Request):
    state = request.app.state
    idem_key = f"idem:{event.event_id}"
    claimed = False

    try:
        claimed = bool(await state.redis.set(idem_key, "1", nx=True, ex=IDEMPOTENCY_TTL_SECONDS))
        if not claimed:
            return JSONResponse(
                status_code=200, content={"event_id": event.event_id, "status": "duplicate"}
            )
    except RedisError:
        log.warning("redis unavailable: skipping fast-path dedup for %s", event.event_id)

    try:
        await state.producer.send_and_wait(
            settings.topic_events,
            value=event.model_dump_json().encode(),
            key=partition_key(event).encode(),
        )
    except KafkaError:
        log.exception("kafka send failed for %s", event.event_id)
        if claimed:
            try:
                await state.redis.delete(idem_key)
            except RedisError:
                log.warning("could not release idempotency key %s", idem_key)
        return JSONResponse(
            status_code=503,
            content={"detail": "event queue unavailable, retry"},
            headers={"Retry-After": "1"},
        )

    return {"event_id": event.event_id, "status": "accepted"}
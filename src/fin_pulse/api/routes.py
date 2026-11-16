"""HTTP routes."""

import logging

from aiokafka.errors import KafkaError
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from fin_pulse.api.deps import Guard, Producer
from fin_pulse.config import settings
from fin_pulse.models import Event, partition_key

log = logging.getLogger(__name__)

router = APIRouter()


def _reply(status_code: int, event_id: str, status: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"event_id": event_id, "status": status}
    )


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/events", status_code=202)
async def ingest(event: Event, producer: Producer, guard: Guard) -> JSONResponse:
    if not await guard.claim(event.event_id):
        return _reply(200, event.event_id, "duplicate")

    try:
        await producer.send_and_wait(
            settings.topic_events,
            value=event.model_dump_json().encode(),
            key=partition_key(event).encode(),
        )
    except KafkaError:
        log.exception("kafka publish failed for %s", event.event_id)
        # Give the claim back, otherwise the client's retry would be taken for a
        # duplicate and the event would be silently lost.
        await guard.release(event.event_id)
        return JSONResponse(
            status_code=503,
            content={"detail": "event queue unavailable, retry"},
            headers={"Retry-After": "1"},
        )

    return _reply(202, event.event_id, "accepted")

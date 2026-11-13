"""Per-message and per-partition handling, independent of Kafka wiring."""

import asyncio
import json
import logging

from pydantic import ValidationError

from fin_pulse.config import settings
from fin_pulse.models import parse_event
from fin_pulse.processing import process_event

log = logging.getLogger(__name__)

RETRY_DELAY_SECONDS = 1.0


async def handle_message(msg, pool, dlq) -> None:
    """Finish one message, or raise if the failure is transient."""
    try:
        event = parse_event(json.loads(msg.value))
    except (ValueError, TypeError, ValidationError) as exc:
        await _send_to_dlq(dlq, msg, exc)
        return
    outcome = await process_event(pool, event)
    log.info("%s -> %s %s", event.event_id, outcome.status, outcome.reason or "")


async def _send_to_dlq(dlq, msg, exc) -> None:
    """A poison message can never succeed on retry, so park it."""
    source = f"{msg.topic}:{msg.partition}:{msg.offset}"
    await dlq.send_and_wait(
        settings.topic_dlq,
        value=msg.value,
        key=msg.key,
        headers=[("reason", str(exc)[:200].encode()), ("source", source.encode())],
    )
    log.warning("poison message %s sent to DLQ", source)


async def run_partition(consumer, tp, messages, pool, dlq) -> int:
    """Handle a partition's messages in order; return the next offset to commit."""
    for msg in messages:
        try:
            await handle_message(msg, pool, dlq)
        except Exception:
            log.exception("transient failure at %s offset=%s", tp, msg.offset)
            # The consumer already advanced past this message: rewind to re-read it.
            consumer.seek(tp, msg.offset)
            await asyncio.sleep(RETRY_DELAY_SECONDS)
            return msg.offset
    return messages[-1].offset + 1

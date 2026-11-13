"""Kafka wiring: poll batches, run partitions concurrently, commit offsets.

At-least-once: an offset is committed only after every earlier message in that
partition is fully handled. A crash after the DB commit but before the offset
commit causes a redelivery, which the idempotent claim in Postgres absorbs.
"""

import asyncio
import logging

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import CommitFailedError
from aiokafka.structs import OffsetAndMetadata

from fin_pulse.config import settings
from fin_pulse.worker.handler import run_partition

log = logging.getLogger(__name__)

GROUP_ID = "fin-pulse-workers"
MAX_RECORDS = 200


async def run(stop: asyncio.Event) -> None:
    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=2, max_size=10)
    dlq = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap, acks="all", enable_idempotence=True
    )
    consumer = AIOKafkaConsumer(
        settings.topic_events,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await dlq.start()
    await consumer.start()
    log.info("worker started, group=%s", GROUP_ID)
    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=MAX_RECORDS)
            if batch:
                await _process_and_commit(consumer, batch, pool, dlq)
    finally:
        await consumer.stop()
        await dlq.stop()
        await pool.close()
        log.info("worker stopped")


async def _process_and_commit(consumer, batch, pool, dlq) -> None:
    tps = list(batch)
    next_offsets = await asyncio.gather(
        *(run_partition(consumer, tp, batch[tp], pool, dlq) for tp in tps)
    )
    offsets = {
        tp: OffsetAndMetadata(nxt, "")
        for tp, nxt in zip(tps, next_offsets, strict=True)
    }
    try:
        await consumer.commit(offsets)
    except CommitFailedError:
        # A rebalance took our partitions; the new owner re-reads from the last
        # committed offset and the DB claim makes the replay harmless.
        log.warning("commit failed (rebalance?); messages will be redelivered")

"""Worker logic tests with fakes: no Kafka or Postgres needed."""

import json
from types import SimpleNamespace

from aiokafka import TopicPartition

from fin_pulse.processing import Outcome
from fin_pulse.worker import handler

TP = TopicPartition("fin.events", 0)


class FakeDLQ:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        self.sent.append((topic, value, headers))


class FakeConsumer:
    def __init__(self):
        self.seeks = []

    def seek(self, tp, offset):
        self.seeks.append((tp, offset))


def msg(offset, body):
    value = body if isinstance(body, bytes) else json.dumps(body).encode()
    return SimpleNamespace(
        topic="fin.events", partition=0, offset=offset, key=b"acc_00001", value=value
    )


def valid(event_id, amount=100):
    return {
        "event_id": event_id,
        "type": "deposit.requested",
        "timestamp": "2026-11-12T10:00:00Z",
        "payload": {"account_id": "acc_00001", "amount": amount, "currency": "IRR"},
    }


async def test_poison_messages_go_to_dlq_and_offset_still_advances():
    dlq = FakeDLQ()
    msgs = [msg(3, b"not json{{"), msg(4, valid("evt_00000009", amount=-5))]
    nxt = await handler.run_partition(FakeConsumer(), TP, msgs, None, dlq)
    assert nxt == 5
    assert len(dlq.sent) == 2
    assert all(topic == "fin.events.dlq" for topic, _, _ in dlq.sent)


async def test_valid_messages_are_processed_in_order_and_offset_advances(monkeypatch):
    seen = []

    async def fake_process(pool, event):
        seen.append(event.event_id)
        return Outcome("applied")

    monkeypatch.setattr(handler, "process_event", fake_process)
    msgs = [msg(5, valid("evt_00000001")), msg(6, valid("evt_00000002"))]
    nxt = await handler.run_partition(FakeConsumer(), TP, msgs, None, FakeDLQ())
    assert seen == ["evt_00000001", "evt_00000002"]
    assert nxt == 7


async def test_transient_failure_stops_partition_and_rewinds(monkeypatch):
    monkeypatch.setattr(handler, "RETRY_DELAY_SECONDS", 0)
    seen = []

    async def flaky(pool, event):
        if event.event_id == "evt_00000002":
            raise ConnectionError("db down")
        seen.append(event.event_id)
        return Outcome("applied")

    monkeypatch.setattr(handler, "process_event", flaky)
    consumer = FakeConsumer()
    msgs = [
        msg(5, valid("evt_00000001")),
        msg(6, valid("evt_00000002")),
        msg(7, valid("evt_00000003")),
    ]
    nxt = await handler.run_partition(consumer, TP, msgs, None, FakeDLQ())
    assert seen == ["evt_00000001"]  # third message NOT processed: order preserved
    assert nxt == 6  # commit only what is truly done
    assert consumer.seeks == [(TP, 6)]  # rewind so the failed message is re-read

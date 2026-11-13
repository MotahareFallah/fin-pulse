import json

from aiokafka.errors import KafkaError
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from fin_pulse.api.main import app


class FakeRedis:
    def __init__(self, down=False):
        self.keys, self.down = set(), down

    async def set(self, key, value, nx=False, ex=None):
        if self.down:
            raise RedisConnectionError("redis down")
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True

    async def delete(self, key):
        self.keys.discard(key)


class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    async def send_and_wait(self, topic, value, key):
        if self.fail:
            raise KafkaError("broker down")
        self.sent.append((topic, key, json.loads(value)))


def make_client(redis=None, producer=None):
    # TestClient used WITHOUT `with` does not run lifespan, so no real Kafka/Redis is needed.
    app.state.redis = redis or FakeRedis()
    app.state.producer = producer or FakeProducer()
    return TestClient(app), app.state.redis, app.state.producer


def deposit(event_id="evt_00000001", **payload):
    p = {"account_id": "acc_00001", "amount": 1000, "currency": "IRR"}
    p.update(payload)
    return {
        "event_id": event_id,
        "type": "deposit.requested",
        "timestamp": "2026-11-12T10:00:00Z",
        "payload": p,
    }


def test_valid_event_is_accepted_and_keyed_by_account():
    client, _, producer = make_client()
    r = client.post("/events", json=deposit())
    assert r.status_code == 202
    assert r.json() == {"event_id": "evt_00000001", "status": "accepted"}
    topic, key, body = producer.sent[0]
    assert key == b"acc_00001" and body["event_id"] == "evt_00000001"


def test_duplicate_event_id_is_sent_to_kafka_once():
    client, _, producer = make_client()
    assert client.post("/events", json=deposit()).status_code == 202
    r = client.post("/events", json=deposit())
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert len(producer.sent) == 1


def test_invalid_event_is_rejected_and_not_sent():
    client, _, producer = make_client()
    assert client.post("/events", json=deposit(amount=-5)).status_code == 422
    assert client.post("/events", json=deposit(amount=10.5)).status_code == 422
    assert producer.sent == []


def test_kafka_failure_returns_503_and_releases_the_claim():
    client, redis, _ = make_client(producer=FakeProducer(fail=True))
    r = client.post("/events", json=deposit())
    assert r.status_code == 503 and r.headers["retry-after"] == "1"
    # claim released, so a retry is not mistaken for a duplicate
    assert redis.keys == set()
    app.state.producer = FakeProducer()
    assert client.post("/events", json=deposit()).status_code == 202


def test_redis_down_still_accepts_events():
    client, _, producer = make_client(redis=FakeRedis(down=True))
    assert client.post("/events", json=deposit()).status_code == 202
    assert len(producer.sent) == 1

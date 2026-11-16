import pytest
from fastapi.testclient import TestClient

from fin_pulse.api.dedup import IdempotencyGuard
from fin_pulse.api.deps import get_guard, get_producer
from fin_pulse.api.main import app


@pytest.fixture
def client(redis, producer):
    # TestClient used without `with` skips the lifespan, so no real Kafka or Redis.
    app.dependency_overrides[get_producer] = lambda: producer
    app.dependency_overrides[get_guard] = lambda: IdempotencyGuard(redis)
    yield TestClient(app)
    app.dependency_overrides.clear()


def deposit(event_id="evt_00000001", **payload):
    p = {"account_id": "acc_00001", "amount": 1000, "currency": "IRR"}
    p.update(payload)
    return {
        "event_id": event_id,
        "type": "deposit.requested",
        "timestamp": "2026-11-12T10:00:00Z",
        "payload": p,
    }


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_valid_event_is_accepted_and_keyed_by_account(client, producer):
    response = client.post("/events", json=deposit())
    assert response.status_code == 202
    assert response.json() == {"event_id": "evt_00000001", "status": "accepted"}
    topic, key, body = producer.sent[0]
    assert (topic, key) == ("fin.events", b"acc_00001")
    assert body["event_id"] == "evt_00000001"


def test_duplicate_event_is_published_once(client, producer):
    assert client.post("/events", json=deposit()).status_code == 202
    response = client.post("/events", json=deposit())
    assert response.status_code == 200
    assert response.json()["status"] == "duplicate"
    assert len(producer.sent) == 1


@pytest.mark.parametrize("amount", [-5, 10.5])
def test_invalid_event_is_rejected_and_not_published(client, producer, amount):
    assert client.post("/events", json=deposit(amount=amount)).status_code == 422
    assert producer.sent == []


def test_kafka_failure_returns_503_and_releases_the_claim(client, redis, producer):
    producer.fail = True
    response = client.post("/events", json=deposit())
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert redis.keys == set()
    # The claim was released, so the client's retry is not mistaken for a duplicate.
    producer.fail = False
    assert client.post("/events", json=deposit()).status_code == 202


def test_redis_outage_does_not_stop_ingestion(client, redis, producer):
    redis.down = True
    assert client.post("/events", json=deposit()).status_code == 202
    assert len(producer.sent) == 1

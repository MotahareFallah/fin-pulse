"""Shared fakes for tests that must not touch real Kafka or Redis."""

import json

import pytest
from aiokafka.errors import KafkaError
from redis.exceptions import ConnectionError as RedisConnectionError


class FakeRedis:
    def __init__(self):
        self.keys: set[str] = set()
        self.down = False

    def _check(self):
        if self.down:
            raise RedisConnectionError("redis is down")

    async def set(self, key, value, nx=False, ex=None):
        self._check()
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True

    async def delete(self, key):
        self._check()
        self.keys.discard(key)


class FakeProducer:
    def __init__(self):
        self.sent = []
        self.fail = False

    async def send_and_wait(self, topic, value, key):
        if self.fail:
            raise KafkaError("broker is down")
        self.sent.append((topic, key, json.loads(value)))


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def producer():
    return FakeProducer()

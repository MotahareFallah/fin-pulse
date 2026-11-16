"""Fast duplicate detection in Redis.

Redis is only an optimization. If it is unavailable the guard lets every event through
(fail-open) and PostgreSQL stays the real idempotency guard (ADR 0001).
"""

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

TTL_SECONDS = 24 * 3600


class IdempotencyGuard:
    def __init__(self, redis: Redis, ttl_seconds: int = TTL_SECONDS) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    @staticmethod
    def _key(event_id: str) -> str:
        return f"idem:{event_id}"

    async def claim(self, event_id: str) -> bool:
        """True if the caller should process the event, False for a known duplicate."""
        try:
            claimed = await self._redis.set(
                self._key(event_id), "1", nx=True, ex=self._ttl
            )
        except RedisError:
            log.warning("redis unavailable, skipping dedup for %s", event_id)
            return True
        return bool(claimed)

    async def release(self, event_id: str) -> None:
        """Give the claim back after a failed publish, so a client retry is accepted."""
        try:
            await self._redis.delete(self._key(event_id))
        except RedisError:
            log.warning("could not release the claim for %s", event_id)

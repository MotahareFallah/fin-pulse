from fin_pulse.api.dedup import IdempotencyGuard


async def test_first_claim_wins_and_a_second_one_is_a_duplicate(redis):
    guard = IdempotencyGuard(redis)
    assert await guard.claim("evt_00000001") is True
    assert await guard.claim("evt_00000001") is False


async def test_released_claim_can_be_taken_again(redis):
    guard = IdempotencyGuard(redis)
    await guard.claim("evt_00000001")
    await guard.release("evt_00000001")
    assert await guard.claim("evt_00000001") is True


async def test_redis_outage_lets_events_through_and_release_does_not_raise(redis):
    redis.down = True
    guard = IdempotencyGuard(redis)
    assert await guard.claim("evt_00000001") is True
    await guard.release("evt_00000001")

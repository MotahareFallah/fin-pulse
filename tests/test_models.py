import pytest
from pydantic import ValidationError

from fin_pulse.models import TransferEvent, parse_event, partition_key

BASE = {"event_id": "evt_00000001", "timestamp": "2026-11-12T10:00:00Z"}


def deposit(**over):
    p = {"account_id": "acc_00001", "amount": 1000, "currency": "IRR"}
    p.update(over)
    return {**BASE, "type": "deposit.requested", "payload": p}


def transfer(**over):
    p = {"from_account": "acc_00001", "to_account": "acc_00002", "amount": 500, "currency": "IRR"}
    p.update(over)
    return {**BASE, "type": "transfer.requested", "payload": p}


def test_valid_deposit():
    assert partition_key(parse_event(deposit())) == "acc_00001"


def test_transfer_partitions_by_source_account():
    ev = parse_event(transfer())
    assert isinstance(ev, TransferEvent)
    assert partition_key(ev) == "acc_00001"


@pytest.mark.parametrize("amount", [0, -5, 150.5, "150", True])
def test_bad_amounts_rejected(amount):
    with pytest.raises(ValidationError):
        parse_event(deposit(amount=amount))


def test_transfer_to_same_account_rejected():
    with pytest.raises(ValidationError):
        parse_event(transfer(to_account="acc_00001"))


def test_unknown_type_and_extra_fields_rejected():
    with pytest.raises(ValidationError):
        parse_event({**BASE, "type": "loan.requested", "payload": {}})
    with pytest.raises(ValidationError):
        parse_event(deposit(note="hi"))

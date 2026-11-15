import pytest
from pydantic import ValidationError

from fin_pulse.models import TransferEvent, parse_event, partition_key

BASE = {"event_id": "evt_00000001", "timestamp": "2026-11-12T10:00:00Z"}


def deposit(**over):
    p = {"account_id": "acc_00001", "amount": 1000, "currency": "IRR"}
    p.update(over)
    return {**BASE, "type": "deposit.requested", "payload": p}


def transfer(**over):
    p = {
        "from_account": "acc_00001",
        "to_account": "acc_00002",
        "amount": 500,
        "currency": "IRR",
    }
    p.update(over)
    return {**BASE, "type": "transfer.requested", "payload": p}


def test_deposit_is_keyed_by_account():
    assert partition_key(parse_event(deposit())) == "acc_00001"


def test_transfer_is_keyed_by_source_account():
    event = parse_event(transfer())
    assert isinstance(event, TransferEvent)
    assert partition_key(event) == "acc_00001"


@pytest.mark.parametrize("amount", [0, -5, 150.5, "150", True])
def test_non_positive_or_non_integer_amounts_are_rejected(amount):
    with pytest.raises(ValidationError):
        parse_event(deposit(amount=amount))


@pytest.mark.parametrize("currency", ["irr", "IRRR"])
def test_invalid_currency_is_rejected(currency):
    with pytest.raises(ValidationError):
        parse_event(deposit(currency=currency))


def test_transfer_to_same_account_is_rejected():
    with pytest.raises(ValidationError):
        parse_event(transfer(to_account="acc_00001"))


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        parse_event({**BASE, "type": "loan.requested", "payload": {}})


def test_unknown_field_is_rejected():
    with pytest.raises(ValidationError):
        parse_event(deposit(note="hi"))

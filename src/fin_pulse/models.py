"""Event contracts. The client sends no partition key: the server derives it,
so a client can never route an event to the wrong partition."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

EventId = Annotated[
    str, Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
]
AccountId = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
]
# strict=True: 150.5 or "150" are rejected. Money is an integer in the smallest unit.
Amount = Annotated[int, Field(strict=True, gt=0, le=10**15)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AccountAmountPayload(_Frozen):
    account_id: AccountId
    amount: Amount
    currency: Currency


class TransferPayload(_Frozen):
    from_account: AccountId
    to_account: AccountId
    amount: Amount
    currency: Currency

    @model_validator(mode="after")
    def _accounts_differ(self):
        if self.from_account == self.to_account:
            raise ValueError("from_account and to_account must differ")
        return self


class DepositEvent(_Frozen):
    event_id: EventId
    type: Literal["deposit.requested"]
    timestamp: datetime
    payload: AccountAmountPayload


class WithdrawalEvent(_Frozen):
    event_id: EventId
    type: Literal["withdrawal.requested"]
    timestamp: datetime
    payload: AccountAmountPayload


class TransferEvent(_Frozen):
    event_id: EventId
    type: Literal["transfer.requested"]
    timestamp: datetime
    payload: TransferPayload


Event = Annotated[
    DepositEvent | WithdrawalEvent | TransferEvent, Field(discriminator="type")
]
event_adapter: TypeAdapter[Event] = TypeAdapter(Event)


def parse_event(data: dict) -> DepositEvent | WithdrawalEvent | TransferEvent:
    return event_adapter.validate_python(data)


def partition_key(event) -> str:
    """Ordering is per account. For a transfer we key on the source account;
    cross-account contention is handled in Postgres with ordered row locks."""
    if isinstance(event, TransferEvent):
        return event.payload.from_account
    return event.payload.account_id

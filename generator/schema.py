"""Strict pydantic models for the public event JSON contract.

The generator writes events as plain JSON lines (``generator.models.Event``);
this module is the *validation* view of the same contract.  The schema audit
validates every event against :class:`EventModel` and JSON Schema generated
from it, so the two representations cannot drift silently.

Strict mode means:

* unknown fields are rejected (``additionalProperties: false``),
* JSON strings are not coerced into numbers and ``"true"`` is not a bool,
* only ``agent_id`` is nullable; every other field must be present and typed.

Cross-field rules that JSON Schema cannot express (agent/channel coupling,
recipient.agent flag) live in the model validators.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from generator.models import Channel, TxnType

#: Rwanda bounding box used for sender geo validation (covers all 60 cells).
RWANDA_LAT = (-2.90, -1.00)
RWANDA_LON = (28.80, 30.60)

#: ULID: Crockford base32, excludes I, L, O and U.
ULID_PATTERN = r"^[0-9A-HJKMNP-TV-Z]{26}$"
#: ISO-8601 UTC with exactly three fractional digits and a Z suffix.
EVENT_TIME_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
MSISDN_PATTERN = r"^h_[0-9a-f]{32}$"
SIM_PATTERN = r"^sim_[0-9a-f]{32}$"
DEVICE_PATTERN = r"^dev_\d{6}_\d$"
CELL_PATTERN = r"^[A-Z]{3}-\d{2}$"
ACCOUNT_PATTERN = r"^(acct_\d{6}|spec_[a-z]+_\d{3})$"
SESSION_PATTERN = r"^(p2p|agt|top|bil|pos|bnk)-[0-9A-Z]{8}$"

_STRICT = ConfigDict(extra="forbid", strict=True)


class GeoModel(BaseModel):
    """Sender position at event time."""

    model_config = _STRICT

    lat: float = Field(ge=RWANDA_LAT[0], le=RWANDA_LAT[1])
    lon: float = Field(ge=RWANDA_LON[0], le=RWANDA_LON[1])
    cell_id: str = Field(pattern=CELL_PATTERN)


class SenderModel(BaseModel):
    """Sender block."""

    model_config = _STRICT

    account_id: str = Field(pattern=ACCOUNT_PATTERN)
    msisdn_hash: str = Field(pattern=MSISDN_PATTERN)
    device_id: str = Field(pattern=DEVICE_PATTERN)
    sim_serial_hash: str = Field(pattern=SIM_PATTERN)
    geo: GeoModel
    account_age_days: int = Field(ge=0)
    kyc_tier: Literal[1, 2, 3]
    balance_before: int = Field(ge=0)


class RecipientModel(BaseModel):
    """Recipient block."""

    model_config = _STRICT

    account_id: str = Field(pattern=ACCOUNT_PATTERN)
    msisdn_hash: str = Field(pattern=MSISDN_PATTERN)
    is_agent: bool
    first_seen_by_sender: bool


class EventModel(BaseModel):
    """One public event record (events.jsonl.zst / events.parquet)."""

    model_config = _STRICT

    event_id: str = Field(pattern=ULID_PATTERN)
    event_time: str = Field(pattern=EVENT_TIME_PATTERN)
    txn_type: TxnType
    amount: int = Field(gt=0)
    currency: Literal["RWF"]
    fee: int = Field(ge=0)
    sender: SenderModel
    recipient: RecipientModel
    channel: Channel
    agent_id: str | None = Field(default=None, pattern=ACCOUNT_PATTERN)
    session_id: str = Field(pattern=SESSION_PATTERN)

    @field_validator("txn_type", "channel", mode="before")
    @classmethod
    def _enum_from_value(cls, value: object) -> object:
        # ``strict=True`` requires enum instances, but the on-disk form is the
        # enum *value*; construct it explicitly rather than coercing strings
        # to arbitrary numbers elsewhere.
        if isinstance(value, str):
            enum = TxnType if value in TxnType._value2member_map_ else Channel
            return enum(value)
        return value

    @model_validator(mode="after")
    def _agent_rules(self) -> EventModel:
        cash = self.txn_type in (TxnType.CASH_IN, TxnType.CASH_OUT)
        if cash:
            if self.channel is not Channel.AGENT:
                raise ValueError("CASH_IN/CASH_OUT must use channel AGENT")
            if self.agent_id is None:
                raise ValueError("CASH_IN/CASH_OUT must carry agent_id")
            if self.agent_id != self.recipient.account_id:
                raise ValueError("agent_id must equal recipient.account_id")
            if not self.recipient.is_agent:
                raise ValueError("CASH_IN/CASH_OUT recipient must be an agent")
        else:
            if self.agent_id is not None:
                raise ValueError("agent_id must be null for non-agent transactions")
            # A bank transfer may credit an agent's float (AGENT_FRAUD's
            # documented "float jump via BANK_TRANSFER"); no other non-agent
            # type may target an agent wallet.
            if self.recipient.is_agent and self.txn_type is not TxnType.BANK_TRANSFER:
                raise ValueError(
                    "recipient.is_agent may only be true for CASH_IN/CASH_OUT "
                    "or BANK_TRANSFER float credits"
                )
        return self


__all__ = [
    "ACCOUNT_PATTERN",
    "CELL_PATTERN",
    "EVENT_TIME_PATTERN",
    "EventModel",
    "GeoModel",
    "MSISDN_PATTERN",
    "RecipientModel",
    "SESSION_PATTERN",
    "SIM_PATTERN",
    "SenderModel",
    "ULID_PATTERN",
]

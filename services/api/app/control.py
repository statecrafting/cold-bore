"""Control-plane command models: the Python half of the cross-language
contract in coldbore-proto (crates/coldbore-proto/src/control.rs). The wire
shape is serde's internally-tagged form: {"cmd": "...", ...fields}.

Changing anything here means changing the Rust side, the architecture doc §7,
and the owning spec in the same PR.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, Field

MAX_REORDER_WINDOW = 4096
MAX_RATE_MULTIPLIER = 100.0


class LinkCommand(BaseModel):
    cmd: Literal["link"]
    pad: Annotated[int, Field(ge=0, le=65535)]
    state: Literal["up", "down"]


class DupCommand(BaseModel):
    cmd: Literal["dup"]
    rate: Annotated[float, Field(ge=0.0, le=1.0, allow_inf_nan=False)]


class ReorderCommand(BaseModel):
    cmd: Literal["reorder"]
    window: Annotated[int, Field(ge=0, le=MAX_REORDER_WINDOW)]


class RateCommand(BaseModel):
    cmd: Literal["rate"]
    multiplier: Annotated[float, Field(gt=0.0, le=MAX_RATE_MULTIPLIER, allow_inf_nan=False)]


class KillCommand(BaseModel):
    cmd: Literal["kill"]
    service: Literal["edge", "ingest"]


class ResetCommand(BaseModel):
    cmd: Literal["reset"]


ControlCommand = Annotated[
    LinkCommand | DupCommand | ReorderCommand | RateCommand | KillCommand | ResetCommand,
    Field(discriminator="cmd"),
]

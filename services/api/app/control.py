"""Control-plane command models: the Python half of the cross-language
contract in coldbore-proto (crates/coldbore-proto/src/control.rs). The wire
shape is serde's internally-tagged form: {"cmd": "...", ...fields}.

Changing anything here means changing the Rust side, the architecture doc §7,
and the owning spec in the same PR.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

MAX_REORDER_WINDOW = 4096
MAX_RATE_MULTIPLIER = 100.0
MAX_PADS = 64
MAX_WELLS_PER_PAD = 64
MAX_TOTAL_WELLS = 2048


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


class TopologyCommand(BaseModel):
    """Resize the simulated field at runtime. A setting, not a fault:
    `reset` does not touch it."""

    cmd: Literal["topology"]
    pads: Annotated[int, Field(ge=1, le=MAX_PADS)]
    wells_per_pad: Annotated[int, Field(ge=1, le=MAX_WELLS_PER_PAD)]

    @model_validator(mode="after")
    def total_wells_bounded(self) -> "TopologyCommand":
        if self.pads * self.wells_per_pad > MAX_TOTAL_WELLS:
            raise ValueError(
                f"{self.pads} pads x {self.wells_per_pad} wells exceeds "
                f"{MAX_TOTAL_WELLS} wells total"
            )
        return self


class KillCommand(BaseModel):
    cmd: Literal["kill"]
    service: Literal["edge", "ingest"]


class ResetCommand(BaseModel):
    cmd: Literal["reset"]


ControlCommand = Annotated[
    LinkCommand
    | DupCommand
    | ReorderCommand
    | RateCommand
    | TopologyCommand
    | KillCommand
    | ResetCommand,
    Field(discriminator="cmd"),
]

"""The control-command contract must match coldbore-proto's serde shape
exactly (internally tagged on "cmd"). These tests pin the wire format."""

import pytest
from pydantic import TypeAdapter, ValidationError

from app.control import ControlCommand

adapter: TypeAdapter = TypeAdapter(ControlCommand)


def test_link_round_trip():
    cmd = adapter.validate_python({"cmd": "link", "pad": 2, "state": "down"})
    assert cmd.model_dump() == {"cmd": "link", "pad": 2, "state": "down"}


def test_rate_bounds_match_rust():
    adapter.validate_python({"cmd": "rate", "multiplier": 100.0})
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "rate", "multiplier": 0.0})
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "rate", "multiplier": 101.0})


def test_dup_bounds():
    adapter.validate_python({"cmd": "dup", "rate": 0.05})
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "dup", "rate": 1.5})


def test_reorder_bounds():
    adapter.validate_python({"cmd": "reorder", "window": 0})
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "reorder", "window": 5000})


def test_kill_targets():
    adapter.validate_python({"cmd": "kill", "service": "ingest"})
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "kill", "service": "api"})


def test_topology_round_trip_and_bounds():
    cmd = adapter.validate_python({"cmd": "topology", "pads": 6, "wells_per_pad": 12})
    assert cmd.model_dump() == {"cmd": "topology", "pads": 6, "wells_per_pad": 12}
    for pads, wells in [(0, 8), (8, 0), (65, 1), (1, 65), (64, 64)]:
        with pytest.raises(ValidationError):
            adapter.validate_python({"cmd": "topology", "pads": pads, "wells_per_pad": wells})


def test_unknown_command_rejected():
    with pytest.raises(ValidationError):
        adapter.validate_python({"cmd": "rm_rf", "path": "/"})

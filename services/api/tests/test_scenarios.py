"""Scenario loading: the YAML grammar is validated against the control
contract at load time, and the shipped scenarios must all load. The engine
preflight refuses to arm a scenario over a substrate that is not pulsing."""

import time

import pytest
from pydantic import ValidationError

from app.scenarios import SCENARIOS_DIR, Engine, Scenario, grade, load_all


def test_shipped_scenarios_all_load():
    scenarios = load_all()
    assert len(scenarios) == 5
    assert set(scenarios) == {
        "01-first-frost",
        "02-double-vision",
        "03-out-of-order",
        "04-night-shift-crash",
        "05-perfect-storm",
    }
    for s in scenarios.values():
        assert s.duration_s > 0
        assert s.timeline, s.id
        assert abs(sum(s.scoring.values()) - 100) < 1e-9, s.id
        assert all(0 <= step.at <= s.duration_s for step in s.timeline), s.id


def test_corrupt_scenario_refuses_to_load(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "id: bad\ntitle: Bad\nduration_s: 10\n"
        "timeline:\n  - at: 1\n    cmd: { cmd: rm_rf, path: / }\n"
    )
    with pytest.raises(ValidationError):
        Scenario.load(bad)


def test_grades():
    assert grade(100) == "S"
    assert grade(86) == "A"
    assert grade(70) == "B"
    assert grade(50) == "C"
    assert grade(49.9) == "F"


def test_scenarios_dir_is_the_repo_one():
    assert (SCENARIOS_DIR / "01-first-frost.yaml").is_file()


_LIVE_POOL = object()


def _engine(snapshots, pool=_LIVE_POOL):
    return Engine(
        pool_getter=lambda: pool,
        publish_control=None,
        broadcast=lambda _msg: None,
        snapshots_getter=lambda: snapshots,
    )


def test_preflight_names_every_problem():
    problems = _engine({}, pool=None).substrate_problems()
    assert "database not connected" in problems
    assert "edge has never reported" in problems
    assert "ingest has never reported" in problems


def test_preflight_flags_a_silent_service():
    now_ms = time.time() * 1000
    snapshots = {
        "edge": {"t_ms": now_ms},
        "ingest": {"t_ms": now_ms - 60_000},
    }
    problems = _engine(snapshots).substrate_problems()
    assert len(problems) == 1
    assert problems[0].startswith("ingest silent for")


def test_preflight_passes_on_a_live_substrate():
    now_ms = time.time() * 1000
    snapshots = {"edge": {"t_ms": now_ms}, "ingest": {"t_ms": now_ms - 1000}}
    assert _engine(snapshots).substrate_problems() == []


@pytest.mark.asyncio
async def test_start_refuses_dead_substrate():
    engine = _engine({}, pool=None)
    with pytest.raises(RuntimeError, match="substrate not live"):
        await engine.start("01-first-frost")

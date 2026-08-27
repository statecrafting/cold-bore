"""Scenario loading: the YAML grammar is validated against the control
contract at load time, and the shipped scenarios must all load."""

import pytest
from pydantic import ValidationError

from app.scenarios import SCENARIOS_DIR, Scenario, grade, load_all


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

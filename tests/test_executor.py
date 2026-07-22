from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import amo.core.executor as executor


def _bundle(*, allow_destructive: bool = False):
    return SimpleNamespace(
        approval=SimpleNamespace(
            approved_mode="safe_sync",
            allow_destructive=allow_destructive,
        ),
        filtered_plan={"steps": [{"id": "step_0001", "op": "ensure_schema", "schema": "public"}]},
        plan_sha256="a" * 64,
    )


def test_execute_uses_approval_not_config_for_destructive_authority(monkeypatch, tmp_path):
    captured = {}

    def fake_execute_plan(*, cfg, plan_obj, plan_sha256, state_path):
        captured.update(
            cfg=cfg,
            plan_obj=plan_obj,
            plan_sha256=plan_sha256,
            state_path=state_path,
        )

    monkeypatch.setattr(executor, "_execute_plan", fake_execute_plan)
    original_cfg = {"engine": {"allow_destructive": True, "copy": {"truncate_first": True}}}

    executor.execute(
        cfg=original_cfg, bundle=_bundle(allow_destructive=False), state_path="state.json"
    )

    assert captured["cfg"]["engine"]["allow_destructive"] is False
    assert captured["cfg"]["engine"]["copy"]["truncate_first"] is False
    assert original_cfg["engine"]["allow_destructive"] is True
    assert captured["plan_sha256"] == "a" * 64

    executor.execute(
        cfg=original_cfg,
        bundle=_bundle(allow_destructive=True),
        state_path="state.json",
    )
    assert captured["cfg"]["engine"]["allow_destructive"] is True
    assert captured["cfg"]["engine"]["copy"]["truncate_first"] is False


def test_failed_step_is_retried_and_attempt_history_is_preserved(monkeypatch, tmp_path):
    calls = {"count": 0}

    class FakeOrchestrator:
        def __init__(self, _cfg):
            pass

        def ensure_schema(self, _schema):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("temporary failure")

        def close(self):
            pass

    monkeypatch.setattr(executor, "MigrationOrchestrator", FakeOrchestrator)
    state_path = tmp_path / "state.json"
    plan = {"steps": [{"id": "step_0001", "op": "ensure_schema", "schema": "public"}]}

    with pytest.raises(RuntimeError, match="temporary failure"):
        executor._execute_plan({}, plan, "b" * 64, str(state_path))

    failed_state = json.loads(state_path.read_text())
    assert failed_state["completed"]["step_0001"]["status"] == "failed"
    assert len(failed_state["completed"]["step_0001"]["attempts"]) == 1

    executor._execute_plan({}, plan, "b" * 64, str(state_path))
    succeeded_state = json.loads(state_path.read_text())
    assert succeeded_state["completed"]["step_0001"]["status"] == "succeeded"
    assert [
        attempt["status"] for attempt in succeeded_state["completed"]["step_0001"]["attempts"]
    ] == [
        "failed",
        "succeeded",
    ]

    executor._execute_plan({}, plan, "b" * 64, str(state_path))
    assert calls["count"] == 2


def test_state_from_another_plan_is_rejected_before_database_initialization(monkeypatch, tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "2",
                "plan_sha256": "c" * 64,
                "completed": {},
            }
        ),
        encoding="utf-8",
    )

    def unexpected_orchestrator(_cfg):
        raise AssertionError("database initialization must not happen")

    monkeypatch.setattr(executor, "MigrationOrchestrator", unexpected_orchestrator)

    with pytest.raises(RuntimeError, match="different approved plan"):
        executor._execute_plan(
            {},
            {"steps": [{"id": "step_0001", "op": "ensure_schema", "schema": "public"}]},
            "d" * 64,
            str(state_path),
        )

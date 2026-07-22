from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("streamlit")

import streamlit_app


def test_browser_config_keeps_password_out_of_written_document(monkeypatch):
    monkeypatch.delenv("AMO_TEST_DB_PASSWORD", raising=False)

    config = streamlit_app._database_config_with_env_password(
        db_type="postgresql",
        host="localhost",
        port=5432,
        database="demo",
        user="demo",
        password="local-secret-value",
        password_env="AMO_TEST_DB_PASSWORD",
    )

    assert config["password"] == "${AMO_TEST_DB_PASSWORD}"
    assert os.environ["AMO_TEST_DB_PASSWORD"] == "local-secret-value"


def _summary_payload() -> dict:
    return {
        "overview": {
            "mode": "safe_sync",
            "planner": "heuristic_v2",
            "source_tables": 1,
            "target_tables": 0,
            "tables_to_copy": 1,
            "tables_to_sync_metadata": 0,
            "manual_review_count": 0,
            "skipped_tables": 0,
        },
        "drift_summary": {
            "source_tables": 1,
            "target_tables": 0,
            "missing_in_target": 1,
            "missing_in_source": 0,
            "metadata_match": 0,
            "metadata_diff": 0,
        },
        "preflight_warnings": [],
        "manual_review_required": [],
        "destructive_actions": [],
        "table_recommendations": [
            {
                "schema": "public",
                "table": "users",
                "diff_status": "missing_in_target",
                "action": "copy",
                "transfer_strategy": "full_copy",
                "chunk_column": None,
                "chunk_count": 1,
                "concurrency_hint": 1,
                "verification_depth": "rowcount",
                "risk_score": 0,
                "risk_level": "low",
                "warnings": [],
                "manual_review_required": False,
                "rationale": "",
                "key_readiness": "primary_key",
                "conflict_key": ["id"],
                "upsert_eligible": True,
            }
        ],
        "planner_recommendation": "Plan can proceed after user approval.",
    }


def test_streamlit_approve_writes_table_strategies(tmp_path):
    summary_path = tmp_path / "summary.json"
    plan_path = tmp_path / "plan.json"
    source_manifest_path = tmp_path / "source_manifest.json"
    approval_path = tmp_path / "approval.json"
    summary_path.write_text(json.dumps(_summary_payload()))
    plan_path.write_text("{}")
    source_manifest_path.write_text("{}")

    streamlit_app._approve(
        str(plan_path),
        str(summary_path),
        str(source_manifest_path),
        "safe_sync",
        "reviewer",
        False,
        ["public.users"],
        [],
        [],
        {"public.users": {"strategy": "upsert"}},
        "",
        str(approval_path),
    )

    approval = json.loads(approval_path.read_text())
    assert approval["table_strategies"]["public.users"]["strategy"] == "upsert"
    assert approval["table_strategies"]["public.users"]["conflict_key"] == ["id"]


def test_demo_target_guard_only_allows_local_docker_target():
    assert streamlit_app._is_demo_target(
        {"host": "localhost", "port": 5434, "database": "targetdb", "user": "target"}
    )
    assert not streamlit_app._is_demo_target(
        {"host": "prod-db", "port": 5432, "database": "targetdb", "user": "target"}
    )


def test_streamlit_run_passes_destructive_approval_bundle(tmp_path, monkeypatch):
    captured = {}

    monkeypatch.setattr(streamlit_app, "load_env", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        streamlit_app,
        "load_config",
        lambda _path: {"engine": {"allow_destructive": False, "copy": {"truncate_first": True}}},
    )
    bundle = SimpleNamespace(
        approval=SimpleNamespace(
            allow_destructive=True,
            included_tables=["public.users"],
            excluded_tables=[],
        ),
        filtered_plan={
            "steps": [{"id": "step_0001", "op": "copy_table", "schema": "public", "table": "users"}]
        },
    )
    monkeypatch.setattr(streamlit_app, "build_approved_execution_bundle", lambda **_kwargs: bundle)

    def fake_execute(*, cfg, bundle, state_path):
        captured["cfg"] = cfg
        captured["bundle"] = bundle
        Path(state_path).write_text("{}")

    monkeypatch.setattr(streamlit_app, "execute", fake_execute)

    state_path = streamlit_app._run(
        "config.yaml",
        "plan.json",
        "approval.json",
        str(tmp_path / "state.json"),
        fresh=True,
    )

    assert state_path == str(tmp_path / "state.json")
    assert captured["bundle"].approval.allow_destructive is True


def test_streamlit_run_passes_non_destructive_approval_bundle(tmp_path, monkeypatch):
    captured = {}

    monkeypatch.setattr(streamlit_app, "load_env", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        streamlit_app,
        "load_config",
        lambda _path: {"engine": {"allow_destructive": True, "copy": {"truncate_first": True}}},
    )
    bundle = SimpleNamespace(
        approval=SimpleNamespace(
            allow_destructive=False,
            included_tables=["public.users"],
            excluded_tables=[],
        ),
        filtered_plan={
            "steps": [{"id": "step_0001", "op": "copy_table", "schema": "public", "table": "users"}]
        },
    )
    monkeypatch.setattr(streamlit_app, "build_approved_execution_bundle", lambda **_kwargs: bundle)

    def fake_execute(*, cfg, bundle, state_path):
        captured["cfg"] = cfg
        captured["bundle"] = bundle
        Path(state_path).write_text("{}")

    monkeypatch.setattr(streamlit_app, "execute", fake_execute)

    streamlit_app._run(
        "config.yaml",
        "plan.json",
        "approval.json",
        str(tmp_path / "state.json"),
        fresh=True,
    )

    assert captured["bundle"].approval.allow_destructive is False

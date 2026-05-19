from __future__ import annotations

from pathlib import Path

from amo import mcp_tools
from amo.core.analysis import write_json


def _plan() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-05-18T00:00:00+00:00",
        "planner": "heuristic_v2",
        "strategy": "largest_first",
        "source": {"host": "src", "database": "demo"},
        "steps": [
            {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
            {
                "id": "step_0002",
                "op": "copy_table",
                "schema": "public",
                "table": "users",
                "validate": {"rowcount": True, "sample_hash": True, "sample_rows": 50},
            },
            {
                "id": "step_0003",
                "op": "verify_table",
                "schema": "public",
                "table": "users",
                "validate": {"rowcount": True, "sample_hash": True, "sample_rows": 50},
            },
        ],
    }


def _summary() -> dict:
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
                "verification_depth": "rowcount_and_sample_hash",
                "risk_score": 10,
                "risk_level": "low",
                "warnings": [],
                "manual_review_required": False,
                "rationale": "safe small table",
            }
        ],
        "planner_recommendation": "Plan can proceed after user approval.",
    }


def _diff() -> dict:
    return {
        "source_role": "source",
        "target_role": "target",
        "summary": {
            "source_tables": 1,
            "target_tables": 0,
            "missing_in_target": 1,
            "missing_in_source": 0,
            "metadata_match": 0,
            "metadata_diff": 0,
        },
        "tables": [
            {
                "schema": "public",
                "table": "users",
                "status": "missing_in_target",
                "structure_compatible": True,
                "source_present": True,
                "target_present": False,
            }
        ],
        "warnings": [],
    }


def test_mcp_tools_validate_read_and_list_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run_dir = Path("runs/demo")
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "plan.json"
    summary_path = run_dir / "pre_migration_summary.json"
    write_json(plan_path, _plan())
    write_json(summary_path, _summary())

    validation = mcp_tools.validate_plan(str(plan_path))
    artifact = mcp_tools.read_artifact(str(summary_path))
    listed = mcp_tools.list_artifacts(str(run_dir))

    assert validation["ok"] is True
    assert validation["step_count"] == 3
    assert artifact["kind"] == "pre_migration_summary"
    assert any(item["name"] == "plan" and item["exists"] for item in listed["artifacts"])


def test_mcp_tools_reject_sensitive_and_outside_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path(".env").write_text("SECRET=value", encoding="utf-8")

    try:
        mcp_tools.read_artifact(".env")
    except ValueError as exc:
        assert "Refusing" in str(exc)
    else:
        raise AssertionError("Expected sensitive file read to be rejected")

    outside = tmp_path.parent / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    try:
        mcp_tools.read_artifact(str(outside))
    except ValueError as exc:
        assert "outside the workspace" in str(exc)
    else:
        raise AssertionError("Expected outside path read to be rejected")


def test_mcp_tools_generate_review_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run_dir = Path("runs/demo")
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "plan.json"
    summary_path = run_dir / "pre_migration_summary.json"
    diff_path = run_dir / "manifest_diff.json"
    state_path = run_dir / "state.json"
    write_json(plan_path, _plan())
    write_json(summary_path, _summary())
    write_json(diff_path, _diff())
    write_json(state_path, {"completed": {"step_0001": {"ok": True}}})

    critique = mcp_tools.critique_plan(str(plan_path), str(summary_path), str(diff_path))
    questions = mcp_tools.generate_clarification_questions(str(summary_path), str(diff_path))
    rationale = mcp_tools.generate_plan_rationale(
        str(plan_path), str(summary_path), critique["critique_path"]
    )
    post = mcp_tools.build_post_summary(
        str(plan_path), str(state_path), str(summary_path), out_path=str(run_dir / "post.json")
    )

    assert Path(critique["critique_path"]).exists()
    assert questions["question_count"] == 0
    assert Path(rationale["rationale_path"]).exists()
    assert Path(post["summary_path"]).exists()
    assert Path(post["failure_analysis_path"]).exists()

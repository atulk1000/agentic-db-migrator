from __future__ import annotations

import json
from pathlib import Path

import pytest

from amo.core.analysis import build_approval_document, write_json
from amo.core.policy import ApprovalPolicyError, build_approved_execution_bundle


def _manifest() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-07-16T00:00:00+00:00",
        "source": {"host": "source-db", "database": "sourcedb"},
        "include_schemas": ["public"],
        "tables": [
            {
                "schema": "public",
                "table": "users",
                "estimated_rows": 2,
                "estimated_bytes": 200,
                "primary_key": ["id"],
                "has_geometry": False,
                "columns": [
                    {
                        "name": "id",
                        "type_sql": "integer",
                        "udt_name": "int4",
                        "not_null": True,
                        "attidentity": "",
                        "default_sql": None,
                        "nextval_sequences": [],
                    }
                ],
                "partition": {
                    "is_partition_parent": False,
                    "partition_key": None,
                    "children": [],
                },
                "foreign_keys": [],
                "indexes": [],
                "grants": [],
            }
        ],
        "matviews": [],
        "matview_indexes": [],
        "udfs": [
            {
                "schema": "public",
                "name": "answer",
                "create_statement": "CREATE OR REPLACE FUNCTION public.answer() RETURNS integer LANGUAGE SQL AS $$ SELECT 42 $$;",
            }
        ],
        "schema_grants": {},
        "errors": [],
    }


def _plan(*, udf_statement: str | None = None) -> dict:
    steps = [
        {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
        {
            "id": "step_0002",
            "op": "create_udfs",
            "schema": "public",
            "udfs": [
                {
                    "schema": "public",
                    "name": "answer",
                    "create_statement": udf_statement or "SELECT untrusted_planner_text",
                }
            ],
        },
        {"id": "step_0003", "op": "ensure_table", "schema": "public", "table": "users"},
        {
            "id": "step_0004",
            "op": "copy_table",
            "schema": "public",
            "table": "users",
            "validate": {"rowcount": True},
        },
        {
            "id": "step_0005",
            "op": "verify_table",
            "schema": "public",
            "table": "users",
            "validate": {"rowcount": True},
        },
    ]
    return {
        "version": "v2",
        "generated_at": "2026-07-16T00:00:00+00:00",
        "planner": "openai",
        "strategy": "risk_aware",
        "source": {"host": "source-db", "database": "sourcedb"},
        "steps": steps,
    }


def _summary() -> dict:
    return {
        "overview": {
            "mode": "safe_sync",
            "planner": "openai",
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
                "rationale": "Target table is missing.",
                "key_readiness": "primary_key",
                "conflict_key": ["id"],
                "upsert_eligible": True,
            }
        ],
        "planner_recommendation": "Proceed after approval.",
    }


def _approved_artifacts(tmp_path: Path, *, plan: dict | None = None) -> dict[str, Path]:
    paths = {
        "plan": tmp_path / "plan.json",
        "summary": tmp_path / "summary.json",
        "manifest": tmp_path / "source_manifest.json",
        "approval": tmp_path / "approval.json",
    }
    write_json(paths["plan"], plan or _plan())
    write_json(paths["summary"], _summary())
    write_json(paths["manifest"], _manifest())
    approval = build_approval_document(
        plan_path=paths["plan"],
        summary_path=paths["summary"],
        source_manifest_path=paths["manifest"],
        approved_mode="safe_sync",
        approved_by="security-test",
        include_tables=["public.users"],
        table_strategies={"public.users": {"strategy": "append_only"}},
    )
    write_json(paths["approval"], approval)
    return paths


def test_approved_bundle_hydrates_untrusted_udf_from_manifest(tmp_path):
    paths = _approved_artifacts(tmp_path)

    bundle = build_approved_execution_bundle(approval_path=paths["approval"])

    udf_step = next(step for step in bundle.filtered_plan["steps"] if step["op"] == "create_udfs")
    assert udf_step["udfs"][0]["create_statement"] == _manifest()["udfs"][0]["create_statement"]
    assert "untrusted_planner_text" not in udf_step["udfs"][0]["create_statement"]


def test_approval_strategy_replaces_planner_load_authority(tmp_path):
    plan = _plan()
    copy_step = next(step for step in plan["steps"] if step["op"] == "copy_table")
    copy_step["op"] = "upsert_table"
    copy_step["conflict_key"] = ["id"]
    copy_step["transfer"] = {
        "load_strategy": "truncate_reload",
        "conflict_key": ["id"],
    }
    paths = _approved_artifacts(tmp_path, plan=plan)

    bundle = build_approved_execution_bundle(approval_path=paths["approval"])

    approved_copy = next(
        step for step in bundle.filtered_plan["steps"] if step["id"] == copy_step["id"]
    )
    assert approved_copy["op"] == "copy_table"
    assert approved_copy["conflict_key"] == []
    assert approved_copy["transfer"]["load_strategy"] == "append_only"
    assert "conflict_key" not in approved_copy["transfer"]


def test_approved_bundle_rejects_unknown_chunk_column(tmp_path):
    plan = _plan()
    copy_step = next(step for step in plan["steps"] if step["op"] == "copy_table")
    copy_step["transfer"] = {"chunk_column": "planner_injected_column"}
    paths = _approved_artifacts(tmp_path, plan=plan)

    with pytest.raises(ApprovalPolicyError, match="unknown chunk column"):
        build_approved_execution_bundle(approval_path=paths["approval"])


@pytest.mark.parametrize("artifact_name", ["plan", "summary", "manifest"])
def test_approved_bundle_rejects_artifact_tampering(tmp_path, artifact_name):
    paths = _approved_artifacts(tmp_path)
    with paths[artifact_name].open("a", encoding="utf-8") as handle:
        handle.write(" ")

    with pytest.raises(ApprovalPolicyError, match="SHA-256 does not match"):
        build_approved_execution_bundle(approval_path=paths["approval"])


def test_approved_bundle_rejects_legacy_approval(tmp_path):
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "approved_mode": "safe_sync",
                "approved_at": "2026-07-16T00:00:00+00:00",
                "plan_path": "plan.json",
                "summary_path": "summary.json",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ApprovalPolicyError, match="predates schema v2"):
        build_approved_execution_bundle(approval_path=approval_path)


def test_approved_bundle_rejects_unknown_udf(tmp_path):
    plan = _plan()
    plan["steps"][1]["udfs"][0]["name"] = "not_in_manifest"
    paths = _approved_artifacts(tmp_path, plan=plan)

    with pytest.raises(ApprovalPolicyError, match="unknown source UDF"):
        build_approved_execution_bundle(approval_path=paths["approval"])


def test_approved_bundle_rejects_destructive_strategy_without_approval(tmp_path):
    paths = _approved_artifacts(tmp_path)
    approval = json.loads(paths["approval"].read_text())
    approval["table_strategies"] = {
        "public.users": {"strategy": "truncate_reload", "conflict_key": []}
    }
    paths["approval"].write_text(json.dumps(approval), encoding="utf-8")

    with pytest.raises(ApprovalPolicyError, match="truncate_reload requires"):
        build_approved_execution_bundle(approval_path=paths["approval"])


def test_approved_bundle_rejects_overlapping_table_scope(tmp_path):
    paths = _approved_artifacts(tmp_path)
    approval = json.loads(paths["approval"].read_text())
    approval["excluded_tables"] = ["public.users"]
    paths["approval"].write_text(json.dumps(approval), encoding="utf-8")

    with pytest.raises(ApprovalPolicyError, match="include and exclude"):
        build_approved_execution_bundle(approval_path=paths["approval"])


def test_retry_plan_cannot_broaden_or_change_approved_steps(tmp_path):
    paths = _approved_artifacts(tmp_path)
    bundle = build_approved_execution_bundle(approval_path=paths["approval"])

    changed = json.loads(json.dumps(bundle.filtered_plan))
    changed["steps"][0]["schema"] = "other"
    with pytest.raises(ApprovalPolicyError, match="changes approved step payload"):
        bundle.with_filtered_plan(changed)

    broadened = json.loads(json.dumps(bundle.filtered_plan))
    broadened["steps"].append({"id": "step_extra", "op": "ensure_schema", "schema": "public"})
    with pytest.raises(ApprovalPolicyError, match="broadens the approved execution scope"):
        bundle.with_filtered_plan(broadened)


def test_approved_bundle_rejects_duplicate_step_ids(tmp_path):
    plan = _plan()
    plan["steps"][1]["id"] = plan["steps"][0]["id"]
    paths = _approved_artifacts(tmp_path, plan=plan)

    with pytest.raises(ApprovalPolicyError, match="step ids must be unique"):
        build_approved_execution_bundle(approval_path=paths["approval"])

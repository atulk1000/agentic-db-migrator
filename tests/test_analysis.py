from __future__ import annotations

from amo.core.analysis import (
    build_approval_document,
    build_pre_migration_summary,
    diff_manifests,
    filter_plan_for_approval,
    write_json,
)


def _table(
    schema: str,
    table: str,
    *,
    estimated_rows: int,
    columns: list[dict],
    primary_key: list[str] | None = None,
    indexes: list[dict] | None = None,
    foreign_keys: list[dict] | None = None,
    partition: dict | None = None,
) -> dict:
    return {
        "schema": schema,
        "table": table,
        "estimated_rows": estimated_rows,
        "estimated_bytes": estimated_rows * 100,
        "primary_key": primary_key or [],
        "has_geometry": False,
        "columns": columns,
        "partition": partition or {"is_partition_parent": False, "partition_key": None, "children": []},
        "foreign_keys": foreign_keys or [],
        "indexes": indexes or [],
    }


def _manifest(tables: list[dict]) -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "source": {"host": "db", "database": "demo"},
        "include_schemas": sorted({table["schema"] for table in tables}),
        "tables": tables,
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }


def _plan() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "planner": "heuristic_v2",
        "strategy": "largest_first",
        "source": {"host": "db", "database": "demo"},
        "steps": [
            {"id": "step_0001", "op": "ensure_schema", "schema": "analytics"},
            {"id": "step_0002", "op": "ensure_schema", "schema": "public"},
            {"id": "step_0003", "op": "ensure_table", "schema": "analytics", "table": "events"},
            {"id": "step_0004", "op": "copy_table", "schema": "analytics", "table": "events", "validate": {"rowcount": True}},
            {"id": "step_0005", "op": "sync_sequences", "schema": "analytics", "table": "events"},
            {"id": "step_0006", "op": "verify_table", "schema": "analytics", "table": "events", "validate": {"rowcount": True}},
            {"id": "step_0007", "op": "ensure_table", "schema": "public", "table": "users"},
            {"id": "step_0008", "op": "copy_table", "schema": "public", "table": "users", "validate": {"rowcount": True}},
            {"id": "step_0009", "op": "sync_sequences", "schema": "public", "table": "users"},
            {"id": "step_0010", "op": "verify_table", "schema": "public", "table": "users", "validate": {"rowcount": True}},
            {"id": "step_0011", "op": "ensure_table", "schema": "public", "table": "orders"},
            {"id": "step_0012", "op": "copy_table", "schema": "public", "table": "orders", "validate": {"rowcount": True}},
            {"id": "step_0013", "op": "create_indexes", "schema": "public", "table": "orders", "indexes": []},
            {"id": "step_0014", "op": "verify_table", "schema": "public", "table": "orders", "validate": {"rowcount": True}},
            {
                "id": "step_0015",
                "op": "add_fks",
                "schema": "public",
                "fks": [{"schema": "public", "table": "orders", "name": "orders_user_id_fkey", "definition": "FOREIGN KEY (user_id) REFERENCES public.users(id)"}],
            },
        ],
    }


def test_diff_and_pre_summary_surface_transfer_strategy_and_manual_review():
    int_col = {"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}
    ts_col = {"name": "event_ts", "type_sql": "timestamp without time zone", "udt_name": "timestamp", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}
    amount_int = {"name": "amount", "type_sql": "integer", "udt_name": "int4", "not_null": False, "attidentity": "", "default_sql": None, "nextval_sequences": []}
    amount_text = {"name": "amount", "type_sql": "text", "udt_name": "text", "not_null": False, "attidentity": "", "default_sql": None, "nextval_sequences": []}

    source_manifest = _manifest(
        [
            _table("public", "users", estimated_rows=1000, columns=[int_col], primary_key=["id"]),
            _table("public", "orders", estimated_rows=200000, columns=[int_col, amount_int], primary_key=["id"]),
            _table(
                "analytics",
                "events",
                estimated_rows=800000,
                columns=[int_col, ts_col],
                primary_key=["id"],
                partition={
                    "is_partition_parent": True,
                    "partition_key": "RANGE (event_ts)",
                    "children": [{"schema": "analytics", "table": "events_2026", "bound": "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"}],
                },
            ),
        ]
    )
    target_manifest = _manifest(
        [
            _table("public", "users", estimated_rows=900, columns=[int_col], primary_key=["id"]),
            _table("public", "orders", estimated_rows=180000, columns=[int_col, amount_text], primary_key=["id"]),
            _table("public", "legacy", estimated_rows=100, columns=[int_col], primary_key=["id"]),
        ]
    )

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    assert manifest_diff["summary"] == {
        "source_tables": 3,
        "target_tables": 3,
        "missing_in_target": 1,
        "missing_in_source": 1,
        "metadata_match": 1,
        "metadata_diff": 1,
    }

    summary = build_pre_migration_summary(source_manifest, target_manifest, manifest_diff, _plan(), migration_mode="safe_sync")
    recs = {(item["schema"], item["table"]): item for item in summary["table_recommendations"]}

    assert recs[("public", "orders")]["action"] == "manual_review"
    assert "public.orders" in summary["manual_review_required"]

    events = recs[("analytics", "events")]
    assert events["transfer_strategy"] == "partition_wise_copy"
    assert events["chunk_column"] == "event_ts"
    assert events["chunk_count"] > 1
    assert events["concurrency_hint"] == 4


def test_filter_plan_for_approval_keeps_only_approved_tables(tmp_path):
    source_manifest = _manifest(
        [
            _table("public", "users", estimated_rows=1000, columns=[{"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}], primary_key=["id"]),
            _table("public", "orders", estimated_rows=200000, columns=[{"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}], primary_key=["id"]),
            _table(
                "analytics",
                "events",
                estimated_rows=800000,
                columns=[
                    {"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []},
                    {"name": "event_ts", "type_sql": "timestamp without time zone", "udt_name": "timestamp", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []},
                ],
                primary_key=["id"],
                partition={
                    "is_partition_parent": True,
                    "partition_key": "RANGE (event_ts)",
                    "children": [{"schema": "analytics", "table": "events_2026", "bound": "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"}],
                },
            ),
        ]
    )
    target_manifest = _manifest(
        [
            _table("public", "users", estimated_rows=1000, columns=[{"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}], primary_key=["id"]),
            _table("public", "orders", estimated_rows=200000, columns=[{"name": "id", "type_sql": "integer", "udt_name": "int4", "not_null": True, "attidentity": "", "default_sql": None, "nextval_sequences": []}, {"name": "amount", "type_sql": "text", "udt_name": "text", "not_null": False, "attidentity": "", "default_sql": None, "nextval_sequences": []}], primary_key=["id"]),
        ]
    )
    manifest_diff = diff_manifests(source_manifest, target_manifest)
    pre_summary = build_pre_migration_summary(source_manifest, target_manifest, manifest_diff, _plan(), migration_mode="safe_sync")

    plan_path = tmp_path / "plan.json"
    summary_path = tmp_path / "summary.json"
    write_json(plan_path, _plan())
    write_json(summary_path, pre_summary)

    approval = build_approval_document(
        plan_path=plan_path,
        summary_path=summary_path,
        approved_mode="safe_sync",
        include_tables=["public.users", "analytics.events"],
        exclude_tables=["public.orders"],
    )

    filtered = filter_plan_for_approval(_plan(), pre_summary, approval)
    step_pairs = {(step.get("op"), step.get("schema"), step.get("table")) for step in filtered["steps"]}

    assert ("ensure_schema", "public", None) in step_pairs
    assert ("ensure_schema", "analytics", None) in step_pairs
    assert ("copy_table", "public", "users") in step_pairs
    assert ("copy_table", "analytics", "events") in step_pairs
    assert ("copy_table", "public", "orders") not in step_pairs
    assert all("orders" not in str(step) for step in filtered["steps"])


def test_build_approval_document_uses_schema_name_for_default_includes(tmp_path):
    summary = {
        "overview": {
            "mode": "safe_sync",
            "planner": "heuristic_v2",
            "source_tables": 2,
            "target_tables": 2,
            "tables_to_copy": 2,
            "tables_to_sync_metadata": 0,
            "manual_review_count": 0,
            "skipped_tables": 0,
        },
        "drift_summary": {
            "source_tables": 2,
            "target_tables": 2,
            "missing_in_target": 0,
            "missing_in_source": 0,
            "metadata_match": 2,
            "metadata_diff": 0,
        },
        "preflight_warnings": [],
        "manual_review_required": [],
        "destructive_actions": [],
        "table_recommendations": [
            {
                "schema": "analytics",
                "table": "events",
                "diff_status": "metadata_match",
                "action": "copy",
                "transfer_strategy": "full_copy",
                "chunk_column": "event_ts",
                "chunk_count": 1,
                "concurrency_hint": 1,
                "verification_depth": "rowcount",
                "risk_score": 0,
                "risk_level": "low",
                "warnings": [],
                "manual_review_required": False,
                "rationale": "",
            },
            {
                "schema": "demo",
                "table": "users",
                "diff_status": "metadata_match",
                "action": "copy",
                "transfer_strategy": "full_copy",
                "chunk_column": "created_at",
                "chunk_count": 1,
                "concurrency_hint": 1,
                "verification_depth": "rowcount",
                "risk_score": 0,
                "risk_level": "low",
                "warnings": [],
                "manual_review_required": False,
                "rationale": "",
            },
        ],
        "planner_recommendation": "Plan can proceed after user approval.",
    }
    summary_path = tmp_path / "summary.json"
    plan_path = tmp_path / "plan.json"
    write_json(summary_path, summary)
    write_json(plan_path, _plan())

    approval = build_approval_document(
        plan_path=plan_path,
        summary_path=summary_path,
        approved_mode="safe_sync",
    )

    assert approval["included_tables"] == ["analytics.events", "demo.users"]

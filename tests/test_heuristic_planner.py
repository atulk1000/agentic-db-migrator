from __future__ import annotations

import json

from amo.core.planners.heuristic_planner import generate_plan


def test_partitioned_tables_are_grouped_without_duplicate_child_work(tmp_path):
    manifest = {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "source": {"host": "src", "database": "demo"},
        "include_schemas": ["public"],
        "tables": [
            {
                "schema": "public",
                "table": "events",
                "estimated_rows": 1000,
                "estimated_bytes": 2048,
                "primary_key": ["id", "created_at"],
                "has_geometry": False,
                "columns": [],
                "partition": {
                    "is_partition_parent": True,
                    "partition_key": "RANGE (created_at)",
                    "children": [
                        {"schema": "public", "table": "events_2026", "bound": "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"}
                    ],
                },
                "foreign_keys": [{"name": "events_account_fk", "definition": "FOREIGN KEY (account_id) REFERENCES public.accounts(id)"}],
                "indexes": [{"index_name": "events_created_at_idx", "index_definition": "CREATE INDEX events_created_at_idx ON public.events (created_at)"}],
            },
            {
                "schema": "public",
                "table": "events_2026",
                "estimated_rows": 1000,
                "estimated_bytes": 2048,
                "primary_key": ["id", "created_at"],
                "has_geometry": False,
                "columns": [],
                "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
                "foreign_keys": [],
                "indexes": [],
            },
        ],
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    plan = generate_plan(str(manifest_path))
    ops = [(step["op"], step.get("schema"), step.get("table")) for step in plan["steps"]]

    assert ("ensure_table", "public", "events") in ops
    assert ("ensure_table", "public", "events_2026") in ops
    assert ops.count(("copy_table", "public", "events_2026")) == 1
    assert ("copy_table", "public", "events") not in ops
    assert ("sync_sequences", "public", "events") in ops
    assert ("verify_table", "public", "events") in ops
    assert ("create_indexes", "public", "events") in ops

    fk_steps = [step for step in plan["steps"] if step["op"] == "add_fks"]
    assert len(fk_steps) == 1
    assert len(fk_steps[0]["fks"]) == 1


def test_heuristic_plan_emits_grants_matview_staging_and_maintenance_steps(tmp_path):
    manifest = {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "source": {"host": "src", "database": "demo"},
        "include_schemas": ["public"],
        "schema_grants": {
            "public": [
                {"grantee": "app_user", "privilege_type": "USAGE", "object_type": "schema", "schema": "public"}
            ]
        },
        "tables": [
            {
                "schema": "public",
                "table": "users",
                "estimated_rows": 250000,
                "estimated_bytes": 125000000,
                "primary_key": ["id"],
                "has_geometry": False,
                "geometry_columns": [],
                "columns": [{"name": "created_at", "type_sql": "timestamp without time zone", "udt_name": "timestamp"}],
                "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
                "foreign_keys": [],
                "indexes": [],
                "grants": [
                    {
                        "grantee": "app_user",
                        "privilege_type": "SELECT",
                        "object_type": "relation",
                        "schema": "public",
                        "object_name": "users",
                    }
                ],
            }
        ],
        "matviews": [
            {
                "schema": "public",
                "name": "big_mv",
                "definition": "SELECT * FROM public.users",
                "estimated_rows": 1000000,
                "estimated_bytes": 150000000,
                "has_geometry": False,
                "geometry_columns": [],
                "grants": [
                    {
                        "grantee": "report_user",
                        "privilege_type": "SELECT",
                        "object_type": "relation",
                        "schema": "public",
                        "object_name": "big_mv",
                    }
                ],
            }
        ],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    plan = generate_plan(str(manifest_path))
    ops = [(step["op"], step.get("schema"), step.get("table")) for step in plan["steps"]]

    assert ("apply_grants", "public", None) in ops
    assert ("vacuum_analyze_table", "public", "users") in ops
    assert ("stage_matviews", "public", None) in ops
    assert ("create_matviews", "public", None) in ops

    copy_step = next(step for step in plan["steps"] if step["op"] == "copy_table")
    assert copy_step["transfer"]["engine"] in {"copy", "spark_jdbc"}
    assert copy_step["transfer"]["chunk_column"] == "created_at"

    stage_step = next(step for step in plan["steps"] if step["op"] == "stage_matviews")
    assert stage_step["matviews"][0]["strategy"] == "staged_rebuild"

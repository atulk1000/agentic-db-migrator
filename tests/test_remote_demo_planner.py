from __future__ import annotations

import json

from amo.core.planners import remote_demo


def _manifest() -> dict:
    int_col = {
        "name": "id",
        "type_sql": "integer",
        "udt_name": "int4",
        "not_null": True,
        "attidentity": "",
        "default_sql": None,
        "nextval_sequences": [],
    }
    return {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "source": {"host": "src", "database": "demo"},
        "include_schemas": ["public"],
        "tables": [
            {
                "schema": "public",
                "table": "users",
                "estimated_rows": 10,
                "estimated_bytes": 100,
                "primary_key": ["id"],
                "has_geometry": False,
                "columns": [int_col],
                "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
                "foreign_keys": [],
                "indexes": [],
            }
        ],
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }


def test_remote_demo_uses_shared_normalization(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))

    monkeypatch.setenv("DEMO_PLANNER_URL", "https://demo.example.com/plan")
    monkeypatch.setattr(
        remote_demo,
        "_post_json",
        lambda *args, **kwargs: {
            "plan": {
                "version": "v2",
                "generated_at": "2023-10-27T10:00:00.000+00:00",
                "planner": "demo",
                "strategy": "demo",
                "source": {"host": "src", "database": "demo", "port": 5432},
                "steps": [
                    {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
                    {"id": "step_0002", "op": "ensure_table", "schema": "public", "name": "users"},
                    {"id": "step_0003", "op": "copy_table", "schema": "public", "name": "users"},
                    {"id": "step_0004", "op": "verify_table", "schema": "public", "name": "users", "mode": "sample_hash"},
                ],
            }
        },
    )

    plan = remote_demo.generate_plan(str(manifest_path))
    assert plan["planner"] == "demo"
    assert plan["planner_metadata"]["mode"] == "remote_demo"
    assert all(step.get("table") == "users" for step in plan["steps"] if step["op"] != "ensure_schema")
    assert all("name" not in step for step in plan["steps"])

from __future__ import annotations

import json

from amo.core.planners import openai


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


def test_openai_normalizes_common_field_mistakes(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MODEL", "openai-test")

    malformed_response = {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "planner": "openai",
        "strategy": "Full migration",
        "source": {"host": "src", "database": "demo", "port": 5432},
        "steps": [
            {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
            {"id": "step_0002", "op": "ensure_table", "schema": "public", "name": "users"},
            {"id": "step_0003", "op": "copy_table", "schema": "public", "name": "users"},
            {
                "id": "step_0004",
                "op": "verify_table",
                "schema": "public",
                "name": "users",
                "mode": "sample_hash",
            },
        ],
    }

    monkeypatch.setattr(
        openai, "_request_openai_plan", lambda **_kwargs: json.dumps(malformed_response)
    )

    plan = openai.generate_plan(str(manifest_path))
    assert plan["planner"] == "openai"
    assert plan["planner_metadata"]["mode"] == "live_api"
    assert plan["planner_metadata"]["provider"] == "openai"

    table_steps = [
        step
        for step in plan["steps"]
        if step["op"] in {"ensure_table", "copy_table", "verify_table"}
    ]
    assert all(step["table"] == "users" for step in table_steps)
    assert all("name" not in step for step in plan["steps"])
    assert plan["steps"][-1]["validate"]["sample_hash"] is True


def test_openai_falls_back_when_output_cannot_be_repaired(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        openai,
        "_request_openai_plan",
        lambda **_kwargs: json.dumps(
            {"steps": [{"id": "s1", "op": "copy_table", "schema": "public"}]}
        ),
    )

    plan = openai.generate_plan(str(manifest_path))
    assert plan["planner"] == "openai_stub"
    assert plan["planner_metadata"]["mode"] == "heuristic_fallback"

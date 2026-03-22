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

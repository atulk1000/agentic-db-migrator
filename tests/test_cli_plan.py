from __future__ import annotations

import json

from typer.testing import CliRunner

from amo.cli import app


RUNNER = CliRunner()


def _write_manifest(path) -> None:
    path.write_text(
        json.dumps(
            {
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
                        "columns": [
                            {
                                "name": "id",
                                "type_sql": "integer",
                                "udt_name": "int4",
                                "not_null": True,
                                "attidentity": "",
                                "default_sql": "nextval('public.users_id_seq'::regclass)",
                                "nextval_sequences": ["public.users_id_seq"],
                            }
                        ],
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
        )
    )


def test_cli_plan_supports_all_advertised_planners(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)

    for planner_name in ("heuristic", "demo", "gemini", "openai", "ollama"):
        out_path = tmp_path / f"{planner_name}.json"
        result = RUNNER.invoke(
            app,
            ["plan", "--manifest", str(manifest_path), "--planner", planner_name, "--out", str(out_path)],
        )
        assert result.exit_code == 0, result.output

        plan = json.loads(out_path.read_text())
        assert plan["planner"]
        assert plan["steps"]

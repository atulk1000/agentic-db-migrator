from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

import amo.cli as cli


RUNNER = CliRunner()


def _table(schema: str, table: str, estimated_rows: int, columns: list[dict], *, primary_key: list[str] | None = None) -> dict:
    return {
        "schema": schema,
        "table": table,
        "estimated_rows": estimated_rows,
        "estimated_bytes": estimated_rows * 100,
        "primary_key": primary_key or [],
        "has_geometry": False,
        "columns": columns,
        "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
        "foreign_keys": [],
        "indexes": [],
    }


def _manifest(*tables: dict) -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "source": {"host": "db", "database": "demo"},
        "include_schemas": sorted({table["schema"] for table in tables}),
        "tables": list(tables),
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }


def _plan() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-03-22T00:00:00+00:00",
        "planner": "openai_v1",
        "strategy": "risk_aware",
        "source": {"host": "db", "database": "demo"},
        "steps": [
            {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
            {"id": "step_0002", "op": "ensure_table", "schema": "public", "table": "users"},
            {"id": "step_0003", "op": "copy_table", "schema": "public", "table": "users", "validate": {"rowcount": True}},
            {"id": "step_0004", "op": "verify_table", "schema": "public", "table": "users", "validate": {"rowcount": True}},
            {"id": "step_0005", "op": "ensure_table", "schema": "public", "table": "orders"},
            {"id": "step_0006", "op": "copy_table", "schema": "public", "table": "orders", "validate": {"rowcount": True}},
            {"id": "step_0007", "op": "verify_table", "schema": "public", "table": "orders", "validate": {"rowcount": True}},
        ],
    }


def test_cli_analyze_review_approve_run_and_summarize_post(tmp_path, monkeypatch):
    int_col = {
        "name": "id",
        "type_sql": "integer",
        "udt_name": "int4",
        "not_null": True,
        "attidentity": "",
        "default_sql": None,
        "nextval_sequences": [],
    }
    amount_int = {
        "name": "amount",
        "type_sql": "integer",
        "udt_name": "int4",
        "not_null": False,
        "attidentity": "",
        "default_sql": None,
        "nextval_sequences": [],
    }
    amount_text = {
        "name": "amount",
        "type_sql": "text",
        "udt_name": "text",
        "not_null": False,
        "attidentity": "",
        "default_sql": None,
        "nextval_sequences": [],
    }

    source_manifest = _manifest(
        _table("public", "users", 1000, [int_col], primary_key=["id"]),
        _table("public", "orders", 250000, [int_col, amount_int], primary_key=["id"]),
    )
    target_manifest = _manifest(
        _table("public", "users", 1000, [int_col], primary_key=["id"]),
        _table("public", "orders", 250000, [int_col, amount_text], primary_key=["id"]),
    )

    monkeypatch.setattr(cli, "load_env", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "load_config", lambda *_args, **_kwargs: {})

    def fake_build_database_manifest(_cfg, db_key):
        return source_manifest if db_key == "source" else target_manifest

    monkeypatch.setattr(cli, "build_database_manifest", fake_build_database_manifest)
    monkeypatch.setattr(cli, "_generate_plan", lambda **_kwargs: _plan())

    out_dir = tmp_path / "analysis"
    result = RUNNER.invoke(cli.app, ["analyze", "--config", "config.yaml", "--planner", "openai", "--out-dir", str(out_dir)])
    assert result.exit_code == 0, result.output

    summary_path = out_dir / "pre_migration_summary.json"
    plan_path = out_dir / "plan.json"
    assert summary_path.exists()
    assert (out_dir / "manifest_diff.json").exists()
    assert (out_dir / "pre_migration_summary.md").exists()

    review_result = RUNNER.invoke(cli.app, ["review", "--summary", str(summary_path)])
    assert review_result.exit_code == 0, review_result.output
    assert "Pre-Migration Summary" in review_result.output

    approval_path = out_dir / "approval.json"
    approve_result = RUNNER.invoke(
        cli.app,
        [
            "approve",
            "--plan",
            str(plan_path),
            "--summary",
            str(summary_path),
            "--out",
            str(approval_path),
            "--mode",
            "safe_sync",
            "--include-table",
            "public.users",
            "--exclude-table",
            "public.orders",
        ],
    )
    assert approve_result.exit_code == 0, approve_result.output

    captured = {}

    def fake_execute(*, cfg, plan_path, state_path, plan_obj=None):
        captured["cfg"] = cfg
        captured["plan_path"] = plan_path
        captured["state_path"] = state_path
        captured["plan_obj"] = plan_obj
        state_payload = {
            "completed": {
                "step_0001": {"ok": True},
                "step_0002": {"ok": True},
                "step_0003": {"ok": True},
                "step_0004": {"ok": True},
            }
        }
        Path(state_path).write_text(json.dumps(state_payload))

    import amo.core.executor as executor

    monkeypatch.setattr(executor, "execute", fake_execute)

    state_path = out_dir / "state.json"
    run_result = RUNNER.invoke(
        cli.app,
        [
            "run",
            "--config",
            "config.yaml",
            "--plan",
            str(plan_path),
            "--approval",
            str(approval_path),
            "--state",
            str(state_path),
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    assert captured["plan_obj"] is not None
    filtered_tables = {step.get("table") for step in captured["plan_obj"]["steps"] if step.get("table")}
    assert filtered_tables == {"users"}

    report_path = out_dir / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "ok": True,
                "tables_checked": 1,
                "results": [{"schema": "public", "table": "users", "ok": True}],
            }
        )
    )

    post_summary_path = out_dir / "post_migration_summary.json"
    summarize_result = RUNNER.invoke(
        cli.app,
        [
            "summarize-post",
            "--plan",
            str(plan_path),
            "--state",
            str(state_path),
            "--report",
            str(report_path),
            "--pre-summary",
            str(summary_path),
            "--out",
            str(post_summary_path),
        ],
    )
    assert summarize_result.exit_code == 0, summarize_result.output
    assert post_summary_path.exists()
    assert post_summary_path.with_suffix(".md").exists()

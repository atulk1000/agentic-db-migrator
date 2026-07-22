from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

import amo.cli as cli
import amo.core.agent as agent_module
from amo.core.agent import AgentPhase, AgentPhaseResult, MigrationAgent

RUNNER = CliRunner()


def _table(
    schema: str,
    table: str,
    estimated_rows: int = 10,
    *,
    primary_key: list[str] | None = None,
) -> dict:
    return {
        "schema": schema,
        "table": table,
        "estimated_rows": estimated_rows,
        "estimated_bytes": estimated_rows * 100,
        "primary_key": primary_key or [],
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
        "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
        "foreign_keys": [],
        "indexes": [],
    }


def _manifest() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-06-01T00:00:00+00:00",
        "source": {"host": "db", "database": "demo"},
        "include_schemas": ["public"],
        "tables": [_table("public", "users", primary_key=["id"])],
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "errors": [],
    }


def _plan() -> dict:
    return {
        "version": "v2",
        "generated_at": "2026-06-01T00:00:00+00:00",
        "planner": "heuristic_v2",
        "strategy": "risk_aware",
        "source": {"host": "db", "database": "demo"},
        "steps": [
            {"id": "step_0001", "op": "ensure_schema", "schema": "public"},
            {"id": "step_0002", "op": "ensure_table", "schema": "public", "table": "users"},
            {
                "id": "step_0003",
                "op": "copy_table",
                "schema": "public",
                "table": "users",
                "validate": {"rowcount": True},
            },
            {
                "id": "step_0004",
                "op": "verify_table",
                "schema": "public",
                "table": "users",
                "validate": {"rowcount": True},
            },
        ],
    }


def _patch_agent_dependencies(monkeypatch):
    monkeypatch.setattr(agent_module, "load_env", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(agent_module, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        agent_module,
        "build_database_manifest",
        lambda _cfg, db_key: _manifest(),
    )
    monkeypatch.setattr(agent_module, "generate_agent_plan", lambda **_kwargs: _plan())


def test_migration_agent_records_trace_and_blocks_without_approval(tmp_path, monkeypatch):
    _patch_agent_dependencies(monkeypatch)
    agent = MigrationAgent(
        config_path="config.yaml",
        planner="heuristic",
        migration_mode="safe_sync",
        analysis_dir=str(tmp_path / "agent"),
        run_id="agent-test",
    )

    assert agent.observe().ok is True
    assert agent.plan().ok is True
    assert agent.critique().ok is True
    blocked = agent.dry_run()

    assert blocked.ok is False
    assert blocked.blockers == ["approval artifact required"]
    assert (tmp_path / "agent" / "source_manifest.json").exists()
    assert (tmp_path / "agent" / "plan.json").exists()
    assert (tmp_path / "agent" / "planner_critique.json").exists()

    trace = json.loads((tmp_path / "agent" / "agent_trace.json").read_text())
    assert trace["status"] == "blocked"
    assert trace["blocked_reason"] == "approval artifact required"
    assert [phase["phase"] for phase in trace["phases"]] == [
        "observe",
        "plan",
        "critique",
        "dry_run",
    ]


def test_migration_agent_executes_valid_approval_and_summarizes(tmp_path, monkeypatch):
    _patch_agent_dependencies(monkeypatch)
    captured = {}

    def fake_execute(*, cfg, bundle, state_path):
        captured["cfg"] = cfg
        captured["bundle"] = bundle
        captured["plan_obj"] = bundle.filtered_plan
        Path(state_path).write_text(
            json.dumps(
                {
                    "completed": {
                        "step_0001": {"ok": True},
                        "step_0002": {"ok": True},
                        "step_0003": {"ok": True},
                        "step_0004": {
                            "ok": True,
                            "verify": {
                                "ok": True,
                                "schema": "public",
                                "table": "users",
                                "source_rows": 1,
                                "target_rows": 1,
                            },
                        },
                    }
                }
            )
        )

    agent = MigrationAgent(
        config_path="config.yaml",
        planner="heuristic",
        migration_mode="safe_sync",
        analysis_dir=str(tmp_path / "agent"),
        run_id="agent-test",
        executor=fake_execute,
    )
    agent.observe()
    agent.plan()
    agent.critique()
    approval = agent.prepare_approval(
        include_tables=["public.users"],
        table_strategies={"public.users": {"strategy": "append_only"}},
    )

    assert approval.ok is True
    assert agent.dry_run().ok is True
    assert agent.execute_approved().ok is True
    summary = agent.summarize()

    assert summary.ok is True
    assert captured["bundle"].approval.allow_destructive is False
    assert captured["plan_obj"]["approval"]["approved_tables"] == ["public.users"]
    assert (tmp_path / "agent" / "post_migration_summary.json").exists()

    trace = json.loads((tmp_path / "agent" / "agent_trace.json").read_text())
    assert trace["status"] == "complete"
    assert trace["artifacts"]["approval"].endswith("approval.json")
    assert trace["artifacts"]["state"].endswith("state.json")


def test_cli_agent_run_exposes_runtime_boundary(tmp_path, monkeypatch):
    class FakeAgent:
        def __init__(self, **_kwargs):
            self.trace_path = tmp_path / "agent_trace.json"

        def observe(self):
            return AgentPhaseResult(
                phase=AgentPhase.OBSERVE,
                ok=True,
                message="observed",
                next_phase=AgentPhase.PLAN,
            )

        def plan(self):
            return AgentPhaseResult(
                phase=AgentPhase.PLAN,
                ok=True,
                message="planned",
                next_phase=AgentPhase.CRITIQUE,
            )

        def critique(self):
            return AgentPhaseResult(
                phase=AgentPhase.CRITIQUE,
                ok=True,
                message="critiqued",
                next_phase=AgentPhase.APPROVAL,
            )

        def dry_run(self, *, approval_path=None):
            return AgentPhaseResult(
                phase=AgentPhase.DRY_RUN,
                ok=False,
                message="Approval artifact is required before dry-run.",
                blockers=["approval artifact required"],
                next_phase=AgentPhase.APPROVAL,
            )

    monkeypatch.setattr(cli, "MigrationAgent", FakeAgent)
    result = RUNNER.invoke(
        cli.app,
        ["agent-run", "--config", "config.yaml", "--out-dir", str(tmp_path / "agent")],
    )

    assert result.exit_code == 0, result.output
    assert "OK observe: observed" in result.output
    assert "BLOCKED dry_run: Approval artifact is required before dry-run." in result.output
    assert "Agent trace:" in result.output

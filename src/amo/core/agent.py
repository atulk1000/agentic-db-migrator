from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from amo.core.agentic import (
    build_clarification_questions,
    build_failure_analysis,
    build_planner_critique,
    render_failure_analysis,
    render_plan_rationale,
)
from amo.core.analysis import (
    build_approval_document,
    build_database_manifest,
    build_dry_run_preview,
    build_execution_graph,
    build_post_migration_summary,
    build_pre_migration_summary,
    diff_manifests,
    load_approval,
    load_pre_summary,
    read_json,
    render_dry_run_preview,
    render_execution_graph,
    render_post_migration_summary,
    render_pre_migration_summary,
    write_json,
)
from amo.core.config import load_config, load_env
from amo.core.planners.models import validate_plan_document
from amo.core.workflow_models import MigrationMode

PLANNER_MODULES = {
    "heuristic": "amo.core.planners.heuristic_planner",
    "demo": "amo.core.planners.remote_demo",
    "gemini": "amo.core.planners.gemini",
    "openai": "amo.core.planners.openai",
}


class AgentPhase(str, Enum):
    OBSERVE = "observe"
    PLAN = "plan"
    CRITIQUE = "critique"
    APPROVAL = "approval"
    DRY_RUN = "dry_run"
    EXECUTE = "execute"
    VERIFY = "verify"
    SUMMARIZE = "summarize"
    RECOVER = "recover"
    BLOCKED = "blocked"
    COMPLETE = "complete"


class AgentPhaseResult(BaseModel):
    phase: AgentPhase
    ok: bool
    message: str
    artifacts: dict[str, str] = Field(default_factory=dict)
    blockers: list[str] = Field(default_factory=list)
    next_phase: AgentPhase | None = None


class AgentRunState(BaseModel):
    run_id: str
    config_path: str
    planner: str
    migration_mode: MigrationMode
    analysis_dir: str
    current_phase: AgentPhase = AgentPhase.OBSERVE
    artifacts: dict[str, str] = Field(default_factory=dict)
    approval_required: bool = True
    blocked_reason: str | None = None


def _default_agent_dir() -> str:
    return str(Path("runs") / f"agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


def _load_planner_module(planner: str):
    mod_path = PLANNER_MODULES.get(planner)
    if not mod_path:
        raise ValueError(f"Unknown planner: {planner}. Use heuristic|demo|gemini|openai")
    return importlib.import_module(mod_path)


def generate_agent_plan(
    manifest_path: str,
    planner: str,
    context_path: str | None = None,
    objective: str | None = None,
) -> dict[str, Any]:
    module = _load_planner_module(planner)
    signature = inspect.signature(module.generate_plan)
    kwargs = {"manifest_path": manifest_path}
    if "context_path" in signature.parameters:
        kwargs["context_path"] = context_path
    if "objective" in signature.parameters:
        kwargs["objective"] = objective
    return validate_plan_document(module.generate_plan(**kwargs))


class MigrationAgent:
    """Coordinates the bounded migration agent loop using existing core modules."""

    def __init__(
        self,
        *,
        config_path: str = "config.yaml",
        planner: str = "heuristic",
        migration_mode: MigrationMode = "safe_sync",
        analysis_dir: str | None = None,
        run_id: str | None = None,
        executor: Callable[..., None] | None = None,
    ) -> None:
        run_id = run_id or f"agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        analysis_dir = analysis_dir or _default_agent_dir()
        self.root = Path(analysis_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.executor = executor
        self.state = AgentRunState(
            run_id=run_id,
            config_path=config_path,
            planner=planner,
            migration_mode=migration_mode,
            analysis_dir=str(self.root),
        )
        self.phase_results: list[AgentPhaseResult] = []
        self._write_trace(status="initialized")

    @property
    def trace_path(self) -> Path:
        return self.root / "agent_trace.json"

    def _path(self, name: str) -> Path:
        return self.root / name

    def _remember(self, key: str, path: str | Path) -> str:
        value = str(path)
        self.state.artifacts[key] = value
        return value

    def _artifact(self, key: str) -> str | None:
        return self.state.artifacts.get(key)

    def _record(self, result: AgentPhaseResult) -> AgentPhaseResult:
        self.phase_results.append(result)
        self.state.current_phase = result.next_phase or result.phase
        self.state.blocked_reason = "; ".join(result.blockers) if result.blockers else None
        status = "blocked" if result.blockers and not result.ok else "running"
        if result.phase == AgentPhase.COMPLETE or result.next_phase == AgentPhase.COMPLETE:
            status = "complete"
        self._write_trace(status=status)
        return result

    def _write_trace(self, status: str) -> None:
        trace = {
            "run_id": self.state.run_id,
            "status": status,
            "current_phase": self.state.current_phase,
            "config_path": self.state.config_path,
            "planner": self.state.planner,
            "migration_mode": self.state.migration_mode,
            "approval_required": self.state.approval_required,
            "blocked_reason": self.state.blocked_reason,
            "artifacts": dict(self.state.artifacts),
            "phases": [item.model_dump(mode="python") for item in self.phase_results],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json(self.trace_path, trace)

    def observe(self) -> AgentPhaseResult:
        load_env(".env")
        cfg = load_config(self.state.config_path)
        source_manifest = build_database_manifest(cfg, db_key="source")
        target_manifest = build_database_manifest(cfg, db_key="target")
        manifest_diff = diff_manifests(source_manifest, target_manifest)

        source_manifest_path = self._path("source_manifest.json")
        target_manifest_path = self._path("target_manifest.json")
        diff_path = self._path("manifest_diff.json")
        write_json(source_manifest_path, source_manifest)
        write_json(target_manifest_path, target_manifest)
        write_json(diff_path, manifest_diff)

        artifacts = {
            "source_manifest": self._remember("source_manifest", source_manifest_path),
            "target_manifest": self._remember("target_manifest", target_manifest_path),
            "manifest_diff": self._remember("manifest_diff", diff_path),
        }
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.OBSERVE,
                ok=True,
                message="Observed source and target database state.",
                artifacts=artifacts,
                next_phase=AgentPhase.PLAN,
            )
        )

    def plan(self) -> AgentPhaseResult:
        source_manifest_path = self._artifact("source_manifest")
        target_manifest_path = self._artifact("target_manifest")
        diff_path = self._artifact("manifest_diff")
        missing = [
            name
            for name, value in {
                "source_manifest": source_manifest_path,
                "target_manifest": target_manifest_path,
                "manifest_diff": diff_path,
            }.items()
            if not value
        ]
        if missing:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.PLAN,
                    ok=False,
                    message="Cannot plan before observation artifacts exist.",
                    blockers=[f"missing {name}" for name in missing],
                )
            )

        source_manifest = read_json(source_manifest_path)
        target_manifest = read_json(target_manifest_path)
        manifest_diff = read_json(diff_path)
        plan_obj = generate_agent_plan(
            manifest_path=source_manifest_path,
            planner=self.state.planner,
            context_path=diff_path,
            objective=f"migration_mode={self.state.migration_mode}",
        )
        pre_summary = build_pre_migration_summary(
            source_manifest=source_manifest,
            target_manifest=target_manifest,
            manifest_diff=manifest_diff,
            plan=plan_obj,
            migration_mode=self.state.migration_mode,
        )

        plan_path = self._path("plan.json")
        pre_summary_path = self._path("pre_migration_summary.json")
        pre_summary_md_path = self._path("pre_migration_summary.md")
        write_json(plan_path, plan_obj)
        write_json(pre_summary_path, pre_summary)
        pre_summary_md_path.write_text(render_pre_migration_summary(pre_summary), encoding="utf-8")

        artifacts = {
            "plan": self._remember("plan", plan_path),
            "pre_migration_summary": self._remember("pre_migration_summary", pre_summary_path),
            "pre_migration_summary_md": self._remember(
                "pre_migration_summary_md", pre_summary_md_path
            ),
        }
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.PLAN,
                ok=True,
                message=f"Generated migration plan with {len(plan_obj.get('steps', []))} steps.",
                artifacts=artifacts,
                next_phase=AgentPhase.CRITIQUE,
            )
        )

    def critique(self) -> AgentPhaseResult:
        plan_path = self._artifact("plan")
        summary_path = self._artifact("pre_migration_summary")
        diff_path = self._artifact("manifest_diff")
        if not plan_path or not summary_path or not diff_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.CRITIQUE,
                    ok=False,
                    message="Cannot critique before plan, summary, and diff artifacts exist.",
                    blockers=["missing plan, summary, or diff artifact"],
                )
            )

        plan_obj = read_json(plan_path)
        pre_summary = read_json(summary_path)
        manifest_diff = read_json(diff_path)
        critique = build_planner_critique(plan_obj, pre_summary, manifest_diff)
        questions = build_clarification_questions(pre_summary, manifest_diff)

        critique_path = self._path("planner_critique.json")
        questions_path = self._path("clarification_questions.json")
        rationale_path = self._path("planner_rationale.md")
        write_json(critique_path, critique)
        write_json(questions_path, questions)
        rationale_path.write_text(
            render_plan_rationale(plan_obj, pre_summary, critique),
            encoding="utf-8",
        )

        artifacts = {
            "planner_critique": self._remember("planner_critique", critique_path),
            "clarification_questions": self._remember("clarification_questions", questions_path),
            "planner_rationale": self._remember("planner_rationale", rationale_path),
        }
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.CRITIQUE,
                ok=True,
                message="Generated planner critique, clarification questions, and rationale.",
                artifacts=artifacts,
                next_phase=AgentPhase.APPROVAL,
            )
        )

    def prepare_approval(
        self,
        *,
        approved_by: str = "agent-user",
        allow_destructive: bool = False,
        include_tables: list[str] | None = None,
        exclude_tables: list[str] | None = None,
        approved_manual_review_items: list[str] | None = None,
        table_strategies: dict[str, Any] | None = None,
        notes: str | None = None,
        approval_path: str | None = None,
    ) -> AgentPhaseResult:
        plan_path = self._artifact("plan")
        summary_path = self._artifact("pre_migration_summary")
        if not plan_path or not summary_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.APPROVAL,
                    ok=False,
                    message="Cannot prepare approval before plan and summary artifacts exist.",
                    blockers=["missing plan or pre_migration_summary artifact"],
                )
            )

        approval_path = approval_path or str(self._path("approval.json"))
        approval = build_approval_document(
            plan_path=plan_path,
            summary_path=summary_path,
            approved_mode=self.state.migration_mode,
            approved_by=approved_by,
            allow_destructive=allow_destructive,
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            approved_manual_review_items=approved_manual_review_items,
            table_strategies=table_strategies,
            notes=notes,
        )
        write_json(approval_path, approval)
        artifacts = {"approval": self._remember("approval", approval_path)}
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.APPROVAL,
                ok=True,
                message="Prepared approval artifact for human review.",
                artifacts=artifacts,
                next_phase=AgentPhase.DRY_RUN,
            )
        )

    def dry_run(self, *, approval_path: str | None = None) -> AgentPhaseResult:
        approval_path = approval_path or self._artifact("approval")
        if not approval_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.DRY_RUN,
                    ok=False,
                    message="Approval artifact is required before dry-run.",
                    blockers=["approval artifact required"],
                    next_phase=AgentPhase.APPROVAL,
                )
            )
        approval_obj = load_approval(approval_path)
        plan_path = approval_obj.plan_path or self._artifact("plan")
        if not plan_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.DRY_RUN,
                    ok=False,
                    message="Plan artifact is required before dry-run.",
                    blockers=["plan artifact required"],
                )
            )

        self._remember("approval", approval_path)
        self._remember("plan", plan_path)
        summary_obj = load_pre_summary(approval_obj.summary_path)
        plan_obj = read_json(plan_path)
        preview = build_dry_run_preview(
            plan=plan_obj,
            summary=summary_obj.model_dump(mode="python"),
            approval=approval_obj.model_dump(mode="python"),
        )

        preview_path = self._path("dry_run_preview.json")
        preview_md_path = self._path("dry_run_preview.md")
        graph_path = self._path("execution_graph.json")
        graph_md_path = self._path("execution_graph.md")
        write_json(preview_path, preview)
        preview_md_path.write_text(render_dry_run_preview(preview), encoding="utf-8")
        graph = (
            build_execution_graph(preview["filtered_plan"])
            if preview.get("filtered_plan")
            else {"node_count": 0, "edge_count": 0, "nodes": [], "edges": []}
        )
        write_json(graph_path, graph)
        graph_md_path.write_text(render_execution_graph(graph), encoding="utf-8")

        artifacts = {
            "approval": self._remember("approval", approval_path),
            "dry_run_preview": self._remember("dry_run_preview", preview_path),
            "dry_run_preview_md": self._remember("dry_run_preview_md", preview_md_path),
            "execution_graph": self._remember("execution_graph", graph_path),
            "execution_graph_md": self._remember("execution_graph_md", graph_md_path),
        }
        blockers = [item.get("reason", "blocked") for item in preview.get("blocked", [])]
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.DRY_RUN,
                ok=bool(preview.get("ok")),
                message=(
                    "Approved migration is ready to execute."
                    if preview.get("ok")
                    else "Approved migration failed dry-run validation."
                ),
                artifacts=artifacts,
                blockers=blockers,
                next_phase=AgentPhase.EXECUTE if preview.get("ok") else AgentPhase.APPROVAL,
            )
        )

    def execute_approved(
        self,
        *,
        approval_path: str | None = None,
        state_path: str | None = None,
        fresh: bool = False,
    ) -> AgentPhaseResult:
        approval_path = approval_path or self._artifact("approval")
        if not approval_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.EXECUTE,
                    ok=False,
                    message="Approval artifact is required before execution.",
                    blockers=["approval artifact required"],
                    next_phase=AgentPhase.APPROVAL,
                )
            )
        approval_obj = load_approval(approval_path)
        plan_path = approval_obj.plan_path or self._artifact("plan")
        if not plan_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.EXECUTE,
                    ok=False,
                    message="Plan artifact is required before execution.",
                    blockers=["plan artifact required"],
                )
            )

        load_env(".env")
        cfg = load_config(self.state.config_path)
        self._remember("approval", approval_path)
        self._remember("plan", plan_path)
        if approval_obj.approved_mode == "plan_only":
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.EXECUTE,
                    ok=False,
                    message="Approved mode plan_only cannot be executed.",
                    blockers=["plan_only approval cannot execute"],
                    next_phase=AgentPhase.APPROVAL,
                )
            )

        engine_cfg = cfg.setdefault("engine", {})
        engine_cfg["allow_destructive"] = bool(approval_obj.allow_destructive)
        if not approval_obj.allow_destructive:
            engine_cfg.setdefault("copy", {})["truncate_first"] = False

        summary_obj = load_pre_summary(approval_obj.summary_path)
        plan_obj = read_json(plan_path)
        preview = build_dry_run_preview(
            plan=plan_obj,
            summary=summary_obj.model_dump(mode="python"),
            approval=approval_obj.model_dump(mode="python"),
        )
        if not preview.get("ok"):
            blockers = [item.get("reason", "blocked") for item in preview.get("blocked", [])]
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.EXECUTE,
                    ok=False,
                    message="Execution blocked because dry-run validation failed.",
                    blockers=blockers or ["dry-run validation failed"],
                    next_phase=AgentPhase.APPROVAL,
                )
            )

        state_path = state_path or str(self._path("state.json"))
        if fresh and Path(state_path).exists():
            Path(state_path).unlink()

        executor = self.executor
        if executor is None:
            from amo.core.executor import execute as executor

        executor(
            cfg=cfg,
            plan_path=plan_path,
            state_path=state_path,
            plan_obj=preview["filtered_plan"],
        )
        artifacts = {"state": self._remember("state", state_path)}
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.EXECUTE,
                ok=True,
                message="Executed approved migration plan.",
                artifacts=artifacts,
                next_phase=AgentPhase.VERIFY,
            )
        )

    def verify(self, *, report_path: str | None = None) -> AgentPhaseResult:
        plan_path = self._artifact("plan")
        if not plan_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.VERIFY,
                    ok=False,
                    message="Plan artifact is required before verification.",
                    blockers=["plan artifact required"],
                )
            )

        from amo.core.verifier import verify_plan, write_report

        load_env(".env")
        cfg = load_config(self.state.config_path)
        report_path = report_path or str(self._path("verification_report.json"))
        report = verify_plan(cfg=cfg, plan_path=plan_path)
        write_report(report, report_path)
        artifacts = {"verification_report": self._remember("verification_report", report_path)}
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.VERIFY,
                ok=bool(report.get("ok")),
                message="Verification completed.",
                artifacts=artifacts,
                blockers=[] if report.get("ok") else ["verification failed"],
                next_phase=AgentPhase.SUMMARIZE,
            )
        )

    def summarize(
        self,
        *,
        state_path: str | None = None,
        report_path: str | None = None,
        out_path: str | None = None,
    ) -> AgentPhaseResult:
        plan_path = self._artifact("plan")
        state_path = state_path or self._artifact("state")
        report_path = report_path or self._artifact("verification_report")
        pre_summary_path = self._artifact("pre_migration_summary")
        if not plan_path or not state_path:
            return self._record(
                AgentPhaseResult(
                    phase=AgentPhase.SUMMARIZE,
                    ok=False,
                    message="Plan and state artifacts are required before summarizing.",
                    blockers=[
                        "plan artifact required" if not plan_path else "state artifact required"
                    ],
                )
            )

        plan_obj = read_json(plan_path)
        state_obj = read_json(state_path)
        report_obj = read_json(report_path) if report_path and Path(report_path).exists() else None
        pre_summary_obj = (
            read_json(pre_summary_path)
            if pre_summary_path and Path(pre_summary_path).exists()
            else None
        )
        summary = build_post_migration_summary(
            plan=plan_obj,
            state=state_obj,
            report=report_obj,
            pre_summary=pre_summary_obj,
        )
        out_path = out_path or str(self._path("post_migration_summary.json"))
        write_json(out_path, summary)
        summary_md_path = str(Path(out_path).with_suffix(".md"))
        Path(summary_md_path).write_text(render_post_migration_summary(summary), encoding="utf-8")

        failure_analysis = build_failure_analysis(
            plan=plan_obj,
            state=state_obj,
            report=report_obj,
            pre_summary=pre_summary_obj,
        )
        failure_json_path = str(Path(out_path).with_name("failure_analysis.json"))
        failure_md_path = str(Path(out_path).with_name("failure_analysis.md"))
        write_json(failure_json_path, failure_analysis)
        Path(failure_md_path).write_text(
            render_failure_analysis(failure_analysis), encoding="utf-8"
        )

        artifacts = {
            "post_migration_summary": self._remember("post_migration_summary", out_path),
            "post_migration_summary_md": self._remember(
                "post_migration_summary_md", summary_md_path
            ),
            "failure_analysis": self._remember("failure_analysis", failure_json_path),
            "failure_analysis_md": self._remember("failure_analysis_md", failure_md_path),
        }
        return self._record(
            AgentPhaseResult(
                phase=AgentPhase.SUMMARIZE,
                ok=True,
                message="Summarized migration outcome.",
                artifacts=artifacts,
                next_phase=AgentPhase.COMPLETE,
            )
        )

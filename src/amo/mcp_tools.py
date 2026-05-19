from __future__ import annotations

import importlib
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any

from amo.core.agentic import (
    build_clarification_questions,
    build_failure_analysis,
    build_planner_critique,
    render_failure_analysis,
    render_plan_rationale,
)
from amo.core.analysis import (
    build_database_manifest,
    build_post_migration_summary,
    build_pre_migration_summary,
    diff_manifests,
    read_json,
    render_post_migration_summary,
    render_pre_migration_summary,
    write_json,
)
from amo.core.config import load_config, load_env
from amo.core.planners.heuristic_planner import write_plan
from amo.core.planners.models import validate_plan_document
from amo.core.workflow_models import MigrationMode

PLANNER_MODULES = {
    "heuristic": "amo.core.planners.heuristic_planner",
    "demo": "amo.core.planners.remote_demo",
    "gemini": "amo.core.planners.gemini",
    "openai": "amo.core.planners.openai",
}

ARTIFACT_NAMES = {
    "source_manifest": "source_manifest.json",
    "target_manifest": "target_manifest.json",
    "manifest_diff": "manifest_diff.json",
    "plan": "plan.json",
    "pre_migration_summary": "pre_migration_summary.json",
    "pre_migration_summary_markdown": "pre_migration_summary.md",
    "planner_critique": "planner_critique.json",
    "clarification_questions": "clarification_questions.json",
    "planner_rationale": "planner_rationale.md",
    "approval": "approval.json",
    "post_migration_summary": "post_migration_summary.json",
    "post_migration_summary_markdown": "post_migration_summary.md",
    "failure_analysis": "failure_analysis.json",
    "failure_analysis_markdown": "failure_analysis.md",
    "verification_report": "verification_report.json",
}

ALLOWED_ARTIFACT_SUFFIXES = {".json", ".md"}
DENIED_FILENAMES = {".env", "config.yaml", "config.example.yaml"}


def _workspace_root() -> Path:
    return Path.cwd().resolve()


def _safe_resolve(path: str | Path) -> Path:
    root = _workspace_root()
    resolved = Path(path).expanduser().resolve()
    if not (resolved == root or root in resolved.parents):
        raise ValueError(f"Path is outside the workspace: {path}")
    return resolved


def _safe_artifact_path(path: str | Path) -> Path:
    resolved = _safe_resolve(path)
    if resolved.name in DENIED_FILENAMES:
        raise ValueError(f"Refusing to read sensitive or configuration file: {path}")
    if resolved.suffix.lower() not in ALLOWED_ARTIFACT_SUFFIXES:
        raise ValueError(f"Unsupported artifact extension: {resolved.suffix}")
    return resolved


def _safe_output_path(path: str | Path) -> Path:
    resolved = _safe_resolve(path)
    if resolved.name in DENIED_FILENAMES:
        raise ValueError(f"Refusing to write sensitive or configuration file: {path}")
    if resolved.suffix.lower() not in ALLOWED_ARTIFACT_SUFFIXES:
        raise ValueError(f"Unsupported artifact extension: {resolved.suffix}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def artifact_kind(obj: dict[str, Any]) -> str:
    summary = obj.get("summary")
    if "overview" in obj and "drift_summary" in obj:
        return "pre_migration_summary"
    if "execution_overview" in obj and "verification_summary" in obj:
        return "post_migration_summary"
    if "ok" in obj and "results" in obj and "tables_checked" in obj:
        return "verification_report"
    if obj.get("agent") == "planner_critic":
        return "planner_critique"
    if obj.get("agent") == "clarification_agent":
        return "clarification_questions"
    if obj.get("agent") == "post_run_failure_analyst":
        return "failure_analysis"
    if "steps" in obj and "planner" in obj:
        return "migration_plan"
    if "approved_mode" in obj and "summary_path" in obj:
        return "approval"
    if "completed" in obj or "failed" in obj or "skipped" in obj:
        return "run_state"
    if isinstance(summary, dict) and {
        "source_tables",
        "target_tables",
        "missing_in_target",
        "missing_in_source",
        "metadata_match",
        "metadata_diff",
    }.issubset(summary):
        return "manifest_diff"
    if "tables" in obj and "source" in obj:
        return "manifest"
    return "unknown"


def _generate_plan(
    manifest_path: str,
    planner: str,
    context_path: str | None = None,
    objective: str | None = None,
) -> dict[str, Any]:
    mod_path = PLANNER_MODULES.get(planner)
    if not mod_path:
        raise ValueError(f"Unknown planner: {planner}. Use one of {sorted(PLANNER_MODULES)}")
    module = importlib.import_module(mod_path)
    signature = inspect.signature(module.generate_plan)
    kwargs = {"manifest_path": manifest_path}
    if "context_path" in signature.parameters:
        kwargs["context_path"] = context_path
    if "objective" in signature.parameters:
        kwargs["objective"] = objective
    return validate_plan_document(module.generate_plan(**kwargs))


def default_mcp_run_dir() -> str:
    return str(Path("runs") / f"mcp_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


def analyze_databases(
    config_path: str = "config.yaml",
    planner: str = "heuristic",
    migration_mode: MigrationMode = "safe_sync",
    out_dir: str | None = None,
) -> dict[str, Any]:
    """Run read-only source/target analysis and create review artifacts."""
    load_env(".env")
    cfg = load_config(config_path)
    out_root = _safe_resolve(out_dir or default_mcp_run_dir())
    out_root.mkdir(parents=True, exist_ok=True)

    source_manifest = build_database_manifest(cfg, db_key="source")
    target_manifest = build_database_manifest(cfg, db_key="target")

    paths = {name: out_root / filename for name, filename in ARTIFACT_NAMES.items()}
    write_json(paths["source_manifest"], source_manifest)
    write_json(paths["target_manifest"], target_manifest)

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    write_json(paths["manifest_diff"], manifest_diff)

    plan = _generate_plan(
        manifest_path=str(paths["source_manifest"]),
        planner=planner,
        context_path=str(paths["manifest_diff"]),
        objective=f"migration_mode={migration_mode}",
    )
    write_plan(plan, paths["plan"])

    pre_summary = build_pre_migration_summary(
        source_manifest=source_manifest,
        target_manifest=target_manifest,
        manifest_diff=manifest_diff,
        plan=plan,
        migration_mode=migration_mode,
    )
    write_json(paths["pre_migration_summary"], pre_summary)
    paths["pre_migration_summary_markdown"].write_text(
        render_pre_migration_summary(pre_summary),
        encoding="utf-8",
    )

    critique = build_planner_critique(plan, pre_summary, manifest_diff)
    questions = build_clarification_questions(pre_summary, manifest_diff)
    write_json(paths["planner_critique"], critique)
    write_json(paths["clarification_questions"], questions)
    paths["planner_rationale"].write_text(
        render_plan_rationale(plan, pre_summary, critique),
        encoding="utf-8",
    )

    return {
        "out_dir": str(out_root),
        "source_manifest": str(paths["source_manifest"]),
        "target_manifest": str(paths["target_manifest"]),
        "manifest_diff": str(paths["manifest_diff"]),
        "plan": str(paths["plan"]),
        "pre_migration_summary": str(paths["pre_migration_summary"]),
        "pre_migration_summary_markdown": str(paths["pre_migration_summary_markdown"]),
        "planner_critique": str(paths["planner_critique"]),
        "clarification_questions": str(paths["clarification_questions"]),
        "planner_rationale": str(paths["planner_rationale"]),
    }


def list_artifacts(run_dir: str) -> dict[str, Any]:
    """List known migration artifacts in a run directory."""
    root = _safe_resolve(run_dir)
    artifacts = []
    for name, filename in ARTIFACT_NAMES.items():
        path = root / filename
        exists = path.exists()
        kind = "missing"
        if exists and path.suffix.lower() == ".json":
            try:
                kind = artifact_kind(read_json(path))
            except Exception:
                kind = "unreadable_json"
        elif exists and path.suffix.lower() == ".md":
            kind = "markdown"
        artifacts.append(
            {
                "name": name,
                "path": str(path),
                "exists": exists,
                "kind": kind,
            }
        )
    return {"run_dir": str(root), "artifacts": artifacts}


def read_artifact(path: str) -> dict[str, Any]:
    """Read a safe JSON or Markdown migration artifact."""
    artifact_path = _safe_artifact_path(path)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Artifact not found: {path}")
    if artifact_path.suffix.lower() == ".json":
        content = read_json(artifact_path)
        return {
            "path": str(artifact_path),
            "kind": artifact_kind(content),
            "content": content,
        }
    return {
        "path": str(artifact_path),
        "kind": "markdown",
        "content": artifact_path.read_text(encoding="utf-8"),
    }


def validate_plan(plan_path: str) -> dict[str, Any]:
    """Validate a plan artifact against the deterministic plan schema."""
    try:
        plan = validate_plan_document(read_json(_safe_artifact_path(plan_path)))
        return {"ok": True, "errors": [], "step_count": len(plan.get("steps", []))}
    except Exception as exc:
        return {"ok": False, "errors": [str(exc)], "step_count": 0}


def critique_plan(
    plan_path: str,
    summary_path: str,
    diff_path: str,
    out_path: str | None = None,
) -> dict[str, Any]:
    """Run planner critic against existing analysis artifacts."""
    plan = read_json(_safe_artifact_path(plan_path))
    summary = read_json(_safe_artifact_path(summary_path))
    diff = read_json(_safe_artifact_path(diff_path))
    critique = build_planner_critique(plan, summary, diff)
    output = (
        _safe_output_path(out_path)
        if out_path
        else _safe_output_path(Path(summary_path).with_name("planner_critique.json"))
    )
    write_json(output, critique)
    return {
        "critique_path": str(output),
        "score": critique.get("score"),
        "status": critique.get("status"),
        "finding_count": critique.get("finding_count", 0),
    }


def generate_clarification_questions(
    summary_path: str,
    diff_path: str,
    out_path: str | None = None,
) -> dict[str, Any]:
    """Generate operator clarification questions from summary and drift."""
    summary = read_json(_safe_artifact_path(summary_path))
    diff = read_json(_safe_artifact_path(diff_path))
    questions = build_clarification_questions(summary, diff)
    output = (
        _safe_output_path(out_path)
        if out_path
        else _safe_output_path(Path(summary_path).with_name("clarification_questions.json"))
    )
    write_json(output, questions)
    return {
        "questions_path": str(output),
        "question_count": questions.get("question_count", 0),
    }


def generate_plan_rationale(
    plan_path: str,
    summary_path: str,
    critique_path: str,
    out_path: str | None = None,
) -> dict[str, Any]:
    """Generate a Markdown rationale for a migration plan."""
    plan = read_json(_safe_artifact_path(plan_path))
    summary = read_json(_safe_artifact_path(summary_path))
    critique = read_json(_safe_artifact_path(critique_path))
    output = (
        _safe_output_path(out_path)
        if out_path
        else _safe_output_path(Path(summary_path).with_name("planner_rationale.md"))
    )
    output.write_text(render_plan_rationale(plan, summary, critique), encoding="utf-8")
    return {"rationale_path": str(output), "format": "markdown"}


def build_post_summary(
    plan_path: str,
    state_path: str,
    pre_summary_path: str,
    report_path: str | None = None,
    out_path: str | None = None,
) -> dict[str, Any]:
    """Build post-migration summary and failure-analysis artifacts."""
    plan = read_json(_safe_artifact_path(plan_path))
    state = read_json(_safe_artifact_path(state_path))
    pre_summary = read_json(_safe_artifact_path(pre_summary_path))
    report = read_json(_safe_artifact_path(report_path)) if report_path else None
    output = (
        _safe_output_path(out_path)
        if out_path
        else _safe_output_path(Path(pre_summary_path).with_name("post_migration_summary.json"))
    )

    summary = build_post_migration_summary(
        plan=plan,
        state=state,
        report=report,
        pre_summary=pre_summary,
    )
    write_json(output, summary)
    summary_md = output.with_suffix(".md")
    summary_md.write_text(render_post_migration_summary(summary), encoding="utf-8")

    failure_analysis = build_failure_analysis(
        plan=plan,
        state=state,
        report=report,
        pre_summary=pre_summary,
    )
    failure_json = output.with_name("failure_analysis.json")
    failure_md = output.with_name("failure_analysis.md")
    write_json(failure_json, failure_analysis)
    failure_md.write_text(render_failure_analysis(failure_analysis), encoding="utf-8")

    return {
        "summary_path": str(output),
        "summary_markdown_path": str(summary_md),
        "failure_analysis_path": str(failure_json),
        "failure_analysis_markdown_path": str(failure_md),
    }


def latest_run_dir() -> str | None:
    runs = Path("runs")
    if not runs.exists():
        return None
    candidates = [path for path in runs.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return str(max(candidates, key=lambda path: path.stat().st_mtime))


def latest_artifact_text(filename: str) -> str:
    run_dir = latest_run_dir()
    if not run_dir:
        raise FileNotFoundError("No runs directory artifacts found.")
    artifact = Path(run_dir) / filename
    safe = _safe_artifact_path(artifact)
    if not safe.exists():
        raise FileNotFoundError(f"Latest artifact not found: {filename}")
    return safe.read_text(encoding="utf-8")


def allowed_operations_policy() -> dict[str, Any]:
    return {
        "execution_model": "planner_recommends_executor_enforces_human_approves",
        "mcp_mvp": "read_review_only",
        "allowed_tool_categories": [
            "analyze",
            "read_artifacts",
            "validate_plan",
            "critique_plan",
            "generate_questions",
            "generate_rationale",
            "summarize_post",
        ],
        "forbidden": [
            "arbitrary_sql_execution",
            "credential_exposure",
            "unapproved_migration_execution",
            "destructive_mutation_without_approval",
        ],
    }

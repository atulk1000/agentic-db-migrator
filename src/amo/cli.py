from __future__ import annotations

import importlib
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import typer

from amo.core.analysis import (
    build_approval_document,
    build_database_manifest,
    build_post_migration_summary,
    build_pre_migration_summary,
    diff_manifests,
    filter_plan_for_approval,
    load_approval,
    load_pre_summary,
    read_json,
    render_post_migration_summary,
    render_pre_migration_summary,
    write_json,
)
from amo.core.config import load_config, load_env
from amo.core.manifest_builder import write_manifest
from amo.core.planners.heuristic_planner import write_plan
from amo.core.planners.models import validate_plan_document
from amo.core.workflow_models import MigrationMode


app = typer.Typer()

PLANNER_MODULES = {
    "heuristic": "amo.core.planners.heuristic_planner",
    "demo": "amo.core.planners.remote_demo",
    "gemini": "amo.core.planners.gemini",
    "openai": "amo.core.planners.openai",
    "ollama": "amo.core.planners.ollama",
}


def _load_planner_module(planner: str):
    mod_path = PLANNER_MODULES.get(planner)
    if not mod_path:
        raise typer.BadParameter(f"Unknown planner: {planner}. Use heuristic|demo|gemini|openai|ollama")
    return importlib.import_module(mod_path)


def _generate_plan(manifest_path: str, planner: str) -> dict:
    module = _load_planner_module(planner)
    return validate_plan_document(module.generate_plan(manifest_path=manifest_path))


def _default_analysis_dir() -> str:
    return str(Path("runs") / f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


@app.command()
def discover(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    out: str = typer.Option("manifest.json", help="Output manifest path"),
    database: str = typer.Option("source", help="Database role to inspect: source or target"),
):
    load_env(".env")
    cfg = load_config(config)

    if database not in ("source", "target"):
        raise typer.BadParameter("database must be source or target")

    manifest = build_database_manifest(cfg, db_key=database)
    write_manifest(manifest, out)
    typer.echo(
        f"Wrote {database} manifest to {out} "
        f"({len(manifest.get('tables', []))} tables, {len(manifest.get('errors', []))} errors)"
    )


@app.command()
def plan(
    manifest: str = typer.Option("manifest.json", help="Input manifest path"),
    planner: str = typer.Option("heuristic", help="Planner to use: heuristic | demo | gemini | openai | ollama"),
    out: str = typer.Option("plan.json", help="Output plan path"),
):
    plan_obj = _generate_plan(manifest_path=manifest, planner=planner)
    write_plan(plan_obj, out)
    typer.echo(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")


@app.command()
def analyze(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    planner: str = typer.Option("heuristic", help="Planner to use: heuristic | demo | gemini | openai | ollama"),
    mode: MigrationMode = typer.Option("safe_sync", help="Recommended migration mode"),
    out_dir: str = typer.Option(None, help="Output directory for analysis artifacts"),
):
    load_env(".env")
    cfg = load_config(config)

    out_root = Path(out_dir or _default_analysis_dir())
    out_root.mkdir(parents=True, exist_ok=True)

    source_manifest = build_database_manifest(cfg, db_key="source")
    target_manifest = build_database_manifest(cfg, db_key="target")

    source_manifest_path = out_root / "source_manifest.json"
    target_manifest_path = out_root / "target_manifest.json"
    diff_path = out_root / "manifest_diff.json"
    plan_path = out_root / "plan.json"
    pre_summary_path = out_root / "pre_migration_summary.json"
    pre_summary_text_path = out_root / "pre_migration_summary.md"

    write_json(source_manifest_path, source_manifest)
    write_json(target_manifest_path, target_manifest)

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    write_json(diff_path, manifest_diff)

    plan_obj = _generate_plan(manifest_path=str(source_manifest_path), planner=planner)
    write_plan(plan_obj, plan_path)

    pre_summary = build_pre_migration_summary(
        source_manifest=source_manifest,
        target_manifest=target_manifest,
        manifest_diff=manifest_diff,
        plan=plan_obj,
        migration_mode=mode,
    )
    write_json(pre_summary_path, pre_summary)
    pre_summary_text_path.write_text(render_pre_migration_summary(pre_summary), encoding="utf-8")

    typer.echo(f"Wrote analysis artifacts to {out_root}")
    typer.echo(f"- source manifest: {source_manifest_path}")
    typer.echo(f"- target manifest: {target_manifest_path}")
    typer.echo(f"- diff: {diff_path}")
    typer.echo(f"- plan: {plan_path}")
    typer.echo(f"- pre-migration summary: {pre_summary_path}")


@app.command()
def review(
    summary: str = typer.Option(..., help="Path to pre_migration_summary.json"),
):
    summary_obj = load_pre_summary(summary)
    typer.echo(render_pre_migration_summary(summary_obj.model_dump(mode="python")))


@app.command()
def approve(
    plan: str = typer.Option(..., help="Path to plan.json"),
    summary: str = typer.Option(..., help="Path to pre_migration_summary.json"),
    out: str = typer.Option("approval.json", help="Output approval path"),
    mode: MigrationMode = typer.Option("safe_sync", help="Approved migration mode"),
    approved_by: str = typer.Option("manual", help="Actor approving the migration"),
    allow_destructive: bool = typer.Option(
        False,
        "--allow-destructive",
        help="Allow running manual-review items that require destructive approval",
    ),
    include_table: Optional[List[str]] = typer.Option(None, "--include-table", help="Fully qualified table to include"),
    exclude_table: Optional[List[str]] = typer.Option(None, "--exclude-table", help="Fully qualified table to exclude"),
    approve_manual_review: Optional[List[str]] = typer.Option(
        None,
        "--approve-manual-review",
        help="Fully qualified table to allow despite manual-review routing",
    ),
    notes: Optional[str] = typer.Option(None, help="Optional approval notes"),
):
    approval = build_approval_document(
        plan_path=plan,
        summary_path=summary,
        approved_mode=mode,
        approved_by=approved_by,
        allow_destructive=allow_destructive,
        include_tables=include_table,
        exclude_tables=exclude_table,
        approved_manual_review_items=approve_manual_review,
        notes=notes,
    )
    write_json(out, approval)
    typer.echo(f"Wrote approval to {out}")


@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: Optional[str] = typer.Option(
        None,
        "--state",
        help="Checkpoint state file. If omitted, a timestamped file is created under runs/.",
    ),
    fresh: bool = typer.Option(
        False,
        "--fresh",
        help="Start a fresh run (ignore existing state file if provided).",
    ),
    truncate_first: Optional[bool] = typer.Option(
        None,
        "--truncate/--no-truncate",
        help="Truncate target tables before COPY (overrides engine.copy.truncate_first)",
    ),
    allow_destructive: Optional[bool] = typer.Option(
        None,
        "--allow-destructive/--no-allow-destructive",
        help="Allow destructive ops like TRUNCATE (overrides engine.allow_destructive)",
    ),
    approval: Optional[str] = typer.Option(
        None,
        "--approval",
        help="Approval artifact generated by amo approve",
    ),
):
    from amo.core.executor import execute

    load_env(".env")
    cfg = load_config(config)

    if truncate_first is not None:
        cfg.setdefault("engine", {}).setdefault("copy", {})["truncate_first"] = bool(truncate_first)

    if allow_destructive is not None:
        cfg.setdefault("engine", {})["allow_destructive"] = bool(allow_destructive)

    if state is None:
        Path("runs").mkdir(exist_ok=True)
        state = f"runs/state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    if fresh and Path(state).exists():
        Path(state).unlink()

    filtered_plan = None
    if approval:
        approval_obj = load_approval(approval)
        if approval_obj.approved_mode == "plan_only":
            raise typer.BadParameter("Approved mode plan_only cannot be executed.")

        if allow_destructive and not approval_obj.allow_destructive:
            raise typer.BadParameter("Approval artifact does not allow destructive execution.")

        summary_obj = load_pre_summary(approval_obj.summary_path)
        original_plan = read_json(plan)
        filtered_plan = filter_plan_for_approval(
            plan=original_plan,
            summary=summary_obj.model_dump(mode="python"),
            approval=approval_obj.model_dump(mode="python"),
        )

    execute(cfg=cfg, plan_path=plan, state_path=state, plan_obj=filtered_plan)
    typer.echo(f"Run complete. State saved to {state}")


@app.command()
def verify(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    out: str = typer.Option("report.json", help="Output report path"),
):
    from amo.core.verifier import verify_plan, write_report

    load_env(".env")
    cfg = load_config(config)

    report = verify_plan(cfg=cfg, plan_path=plan)
    write_report(report, out)
    typer.echo(f"Wrote report to {out}")


@app.command("summarize-post")
def summarize_post(
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: str = typer.Option("state.json", help="Path to execution state JSON"),
    out: str = typer.Option("post_migration_summary.json", help="Output summary path"),
    report: Optional[str] = typer.Option(None, help="Optional verification report JSON"),
    pre_summary: Optional[str] = typer.Option(None, "--pre-summary", help="Optional pre-migration summary JSON"),
):
    plan_obj = read_json(plan)
    state_obj = read_json(state)
    report_obj = read_json(report) if report else None
    pre_summary_obj = read_json(pre_summary) if pre_summary else None

    summary = build_post_migration_summary(
        plan=plan_obj,
        state=state_obj,
        report=report_obj,
        pre_summary=pre_summary_obj,
    )
    write_json(out, summary)

    text_path = Path(out).with_suffix(".md")
    text_path.write_text(render_post_migration_summary(summary), encoding="utf-8")
    typer.echo(f"Wrote post-migration summary to {out}")


if __name__ == "__main__":
    app()

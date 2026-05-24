from __future__ import annotations

import importlib
import inspect
from datetime import datetime
from pathlib import Path

import typer

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
    build_retry_plan,
    diff_manifests,
    filter_plan_for_approval,
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
}


def _load_planner_module(planner: str):
    mod_path = PLANNER_MODULES.get(planner)
    if not mod_path:
        raise typer.BadParameter(f"Unknown planner: {planner}. Use heuristic|demo|gemini|openai")
    return importlib.import_module(mod_path)


def _generate_plan(
    manifest_path: str,
    planner: str,
    context_path: str | None = None,
    objective: str | None = None,
) -> dict:
    module = _load_planner_module(planner)
    signature = inspect.signature(module.generate_plan)
    kwargs = {"manifest_path": manifest_path}
    if "context_path" in signature.parameters:
        kwargs["context_path"] = context_path
    if "objective" in signature.parameters:
        kwargs["objective"] = objective
    return validate_plan_document(module.generate_plan(**kwargs))


def _default_analysis_dir() -> str:
    return str(Path("runs") / f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


def _parse_table_strategy(values: list[str] | None) -> dict[str, dict]:
    parsed: dict[str, dict] = {}
    for value in values or []:
        if "=" not in value:
            raise typer.BadParameter(
                "--table-strategy must use TABLE=strategy or TABLE=upsert:key1,key2"
            )
        table_key, strategy_spec = value.split("=", 1)
        if "." not in table_key:
            raise typer.BadParameter("--table-strategy table must be schema-qualified")
        strategy, _, key_spec = strategy_spec.partition(":")
        item = {"strategy": strategy}
        if key_spec:
            item["conflict_key"] = [part.strip() for part in key_spec.split(",") if part.strip()]
        parsed[table_key] = item
    return parsed


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
    planner: str = typer.Option(
        "heuristic", help="Planner to use: heuristic | demo | gemini | openai"
    ),
    out: str = typer.Option("plan.json", help="Output plan path"),
):
    plan_obj = _generate_plan(manifest_path=manifest, planner=planner)
    write_plan(plan_obj, out)
    typer.echo(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")


@app.command()
def analyze(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    planner: str = typer.Option(
        "heuristic", help="Planner to use: heuristic | demo | gemini | openai"
    ),
    mode: MigrationMode = typer.Option(  # noqa: B008
        "safe_sync", help="Recommended migration mode"
    ),
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
    critique_path = out_root / "planner_critique.json"
    rationale_path = out_root / "planner_rationale.md"
    questions_path = out_root / "clarification_questions.json"

    write_json(source_manifest_path, source_manifest)
    write_json(target_manifest_path, target_manifest)

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    write_json(diff_path, manifest_diff)

    plan_obj = _generate_plan(
        manifest_path=str(source_manifest_path),
        planner=planner,
        context_path=str(diff_path),
        objective=f"migration_mode={mode}",
    )
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

    critique = build_planner_critique(plan_obj, pre_summary, manifest_diff)
    questions = build_clarification_questions(pre_summary, manifest_diff)
    write_json(critique_path, critique)
    write_json(questions_path, questions)
    rationale_path.write_text(
        render_plan_rationale(plan_obj, pre_summary, critique),
        encoding="utf-8",
    )

    typer.echo(f"Wrote analysis artifacts to {out_root}")
    typer.echo(f"- source manifest: {source_manifest_path}")
    typer.echo(f"- target manifest: {target_manifest_path}")
    typer.echo(f"- diff: {diff_path}")
    typer.echo(f"- plan: {plan_path}")
    typer.echo(f"- pre-migration summary: {pre_summary_path}")
    typer.echo(f"- planner critique: {critique_path}")
    typer.echo(f"- clarification questions: {questions_path}")
    typer.echo(f"- planner rationale: {rationale_path}")


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
    mode: MigrationMode = typer.Option("safe_sync", help="Approved migration mode"),  # noqa: B008
    approved_by: str = typer.Option("manual", help="Actor approving the migration"),
    allow_destructive: bool = typer.Option(
        False,
        "--allow-destructive",
        help="Allow running manual-review items that require destructive approval",
    ),
    include_table: list[str] | None = typer.Option(  # noqa: B008
        None, "--include-table", help="Fully qualified table to include"
    ),
    exclude_table: list[str] | None = typer.Option(  # noqa: B008
        None, "--exclude-table", help="Fully qualified table to exclude"
    ),
    approve_manual_review: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--approve-manual-review",
        help="Fully qualified table to allow despite manual-review routing",
    ),
    table_strategy: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--table-strategy",
        help="Per-table load strategy: schema.table=append_only|upsert[:key]|truncate_reload|skip",
    ),
    notes: str | None = typer.Option(None, help="Optional approval notes"),
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
        table_strategies=_parse_table_strategy(table_strategy),
        notes=notes,
    )
    write_json(out, approval)
    typer.echo(f"Wrote approval to {out}")


@app.command("dry-run")
def dry_run(
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    approval: str = typer.Option(..., help="Approval artifact generated by amo approve"),
    out: str | None = typer.Option(None, help="Output preview JSON path"),
):
    approval_obj = load_approval(approval)
    summary_obj = load_pre_summary(approval_obj.summary_path)
    plan_obj = read_json(plan)

    preview = build_dry_run_preview(
        plan=plan_obj,
        summary=summary_obj.model_dump(mode="python"),
        approval=approval_obj.model_dump(mode="python"),
    )

    out_path = Path(out) if out else Path(approval).with_name("dry_run_preview.json")
    write_json(out_path, preview)
    text_path = out_path.with_suffix(".md")
    text_path.write_text(render_dry_run_preview(preview), encoding="utf-8")
    graph = (
        build_execution_graph(preview["filtered_plan"])
        if preview.get("filtered_plan")
        else {"node_count": 0, "edge_count": 0, "nodes": [], "edges": []}
    )
    graph_json_path = out_path.with_name("execution_graph.json")
    graph_md_path = out_path.with_name("execution_graph.md")
    write_json(graph_json_path, graph)
    graph_md_path.write_text(render_execution_graph(graph), encoding="utf-8")
    typer.echo(render_dry_run_preview(preview))
    typer.echo(f"Wrote dry-run preview to {out_path}")
    typer.echo(f"Wrote execution graph to {graph_json_path}")
    if not preview.get("ok"):
        raise typer.Exit(code=1)


@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: str | None = typer.Option(
        None,
        "--state",
        help="Checkpoint state file. If omitted, a timestamped file is created under runs/.",
    ),
    fresh: bool = typer.Option(
        False,
        "--fresh",
        help="Start a fresh run (ignore existing state file if provided).",
    ),
    truncate_first: bool | None = typer.Option(
        None,
        "--truncate/--no-truncate",
        help="Truncate target tables before COPY (overrides engine.copy.truncate_first)",
    ),
    allow_destructive: bool | None = typer.Option(
        None,
        "--allow-destructive/--no-allow-destructive",
        help="Allow destructive ops like TRUNCATE (overrides engine.allow_destructive)",
    ),
    approval: str | None = typer.Option(
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
        engine_cfg = cfg.setdefault("engine", {})
        engine_cfg["allow_destructive"] = bool(approval_obj.allow_destructive)
        if not approval_obj.allow_destructive:
            engine_cfg.setdefault("copy", {})["truncate_first"] = False

        summary_obj = load_pre_summary(approval_obj.summary_path)
        original_plan = read_json(plan)
        preview = build_dry_run_preview(
            plan=original_plan,
            summary=summary_obj.model_dump(mode="python"),
            approval=approval_obj.model_dump(mode="python"),
        )
        if not preview.get("ok"):
            blocked = preview.get("blocked") or []
            first_reason = blocked[0].get("reason", "approval is not executable") if blocked else ""
            raise typer.BadParameter(f"Approval failed dry-run validation: {first_reason}")
        filtered_plan = filter_plan_for_approval(
            plan=original_plan,
            summary=summary_obj.model_dump(mode="python"),
            approval=approval_obj.model_dump(mode="python"),
        )

    execute(cfg=cfg, plan_path=plan, state_path=state, plan_obj=filtered_plan)
    typer.echo(f"Run complete. State saved to {state}")


@app.command()
def retry(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    approval: str = typer.Option(..., help="Approval artifact generated by amo approve"),
    state: str = typer.Option(..., help="Source state file from the failed run"),
    mode: str = typer.Option(
        "failed_only", help="Retry mode: failed_only | from_failed_step | table"
    ),
    table: str | None = typer.Option(None, help="Schema-qualified table for table retry mode"),
    out_dir: str | None = typer.Option(None, help="Directory for retry artifacts"),
    retry_state: str | None = typer.Option(None, help="New state file for the retry execution"),
    allow_destructive: bool | None = typer.Option(
        None,
        "--allow-destructive/--no-allow-destructive",
        help="Allow destructive ops like TRUNCATE (overrides engine.allow_destructive)",
    ),
):
    from amo.core.executor import execute

    load_env(".env")
    cfg = load_config(config)
    if allow_destructive is not None:
        cfg.setdefault("engine", {})["allow_destructive"] = bool(allow_destructive)

    approval_obj = load_approval(approval)
    if approval_obj.approved_mode == "plan_only":
        raise typer.BadParameter("Approved mode plan_only cannot be retried.")
    if allow_destructive and not approval_obj.allow_destructive:
        raise typer.BadParameter("Approval artifact does not allow destructive execution.")
    engine_cfg = cfg.setdefault("engine", {})
    engine_cfg["allow_destructive"] = bool(approval_obj.allow_destructive)
    if not approval_obj.allow_destructive:
        engine_cfg.setdefault("copy", {})["truncate_first"] = False

    summary_obj = load_pre_summary(approval_obj.summary_path)
    original_plan = read_json(plan)
    preview = build_dry_run_preview(
        plan=original_plan,
        summary=summary_obj.model_dump(mode="python"),
        approval=approval_obj.model_dump(mode="python"),
    )
    if not preview.get("ok"):
        blocked = preview.get("blocked") or []
        first_reason = blocked[0].get("reason", "approval is not executable") if blocked else ""
        raise typer.BadParameter(f"Approval failed dry-run validation: {first_reason}")

    source_state = read_json(state)
    retry_plan, retry_summary = build_retry_plan(
        plan=preview["filtered_plan"],
        state=source_state,
        mode=mode,
        table=table,
    )
    if not retry_summary.get("ok"):
        raise typer.BadParameter(retry_summary.get("blocked_reason", "No retry steps selected."))

    retry_root = Path(out_dir) if out_dir else Path(state).parent
    retry_root.mkdir(parents=True, exist_ok=True)
    retry_plan_path = retry_root / "retry_plan.json"
    retry_summary_path = retry_root / "retry_summary.json"
    write_json(retry_plan_path, retry_plan)
    write_json(retry_summary_path, retry_summary)

    if retry_state is None:
        retry_state = str(
            retry_root / f"retry_state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

    execute(cfg=cfg, plan_path=str(retry_plan_path), state_path=retry_state, plan_obj=retry_plan)
    typer.echo(f"Retry complete. State saved to {retry_state}")
    typer.echo(f"Wrote retry plan to {retry_plan_path}")
    typer.echo(f"Wrote retry summary to {retry_summary_path}")


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
    report: str | None = typer.Option(None, help="Optional verification report JSON"),
    pre_summary: str | None = typer.Option(
        None, "--pre-summary", help="Optional pre-migration summary JSON"
    ),
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
    failure_analysis = build_failure_analysis(
        plan=plan_obj,
        state=state_obj,
        report=report_obj,
        pre_summary=pre_summary_obj,
    )
    failure_json_path = Path(out).with_name("failure_analysis.json")
    failure_md_path = Path(out).with_name("failure_analysis.md")
    write_json(failure_json_path, failure_analysis)
    failure_md_path.write_text(render_failure_analysis(failure_analysis), encoding="utf-8")
    typer.echo(f"Wrote post-migration summary to {out}")
    typer.echo(f"Wrote failure analysis to {failure_md_path}")


if __name__ == "__main__":
    app()

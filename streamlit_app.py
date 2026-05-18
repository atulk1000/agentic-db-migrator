from __future__ import annotations

import contextlib
import importlib
import inspect
import io
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st

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
from amo.core.agentic import (
    build_clarification_questions,
    build_failure_analysis,
    build_planner_critique,
    render_failure_analysis,
    render_plan_rationale,
)
from amo.core.config import load_config, load_env
from amo.core.executor import execute
from amo.core.planners.heuristic_planner import write_plan
from amo.core.planners.models import validate_plan_document
from amo.core.workflow_models import MigrationMode

PLANNER_MODULES = {
    "heuristic": "amo.core.planners.heuristic_planner",
    "demo": "amo.core.planners.remote_demo",
    "gemini": "amo.core.planners.gemini",
    "openai": "amo.core.planners.openai",
}

MIGRATION_MODES: list[MigrationMode] = [
    "safe_sync",
    "missing_only",
    "metadata_diff_only",
    "data_diff_only",
    "full_refresh",
    "plan_only",
]


def _load_planner_module(planner: str):
    mod_path = PLANNER_MODULES[planner]
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
    return str(Path("runs") / f"streamlit_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


def _capture(callable_obj, *args, **kwargs):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        result = callable_obj(*args, **kwargs)
    return result, stdout.getvalue(), stderr.getvalue()


def _session_defaults() -> None:
    defaults = {
        "analysis_dir": _default_analysis_dir(),
        "last_plan_path": "",
        "last_summary_path": "",
        "last_approval_path": "",
        "last_state_path": "",
        "last_report_path": "",
        "last_post_summary_path": "",
        "last_critique_path": "",
        "last_questions_path": "",
        "last_rationale_path": "",
        "last_failure_analysis_path": "",
        "flash_message": "",
        "flash_kind": "success",
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def _read_if_exists(path: str) -> dict[str, Any] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    return read_json(p)


def _artifact_kind(obj: dict[str, Any]) -> str:
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
    if "missing_tables_in_target" in obj or "table_metadata_mismatches" in obj:
        return "manifest_diff"
    return "unknown"


def _read_expected_artifact(
    path: str,
    expected_kind: str,
    label: str,
) -> dict[str, Any] | None:
    obj = _read_if_exists(path)
    if obj is None:
        return None

    actual_kind = _artifact_kind(obj)
    if actual_kind != expected_kind:
        st.error(
            f"{label} expects `{expected_kind}`, but `{path}` looks like `{actual_kind}`. "
            "Use the matching artifact from the Analyze output directory."
        )
        return None
    return obj


def _validate_artifact_path(
    path: str,
    expected_kind: str,
    label: str,
    required: bool = True,
) -> bool:
    if not path:
        if required:
            st.error(f"{label} is required.")
        return not required

    p = Path(path)
    if not p.exists():
        if required:
            st.error(f"{label} does not exist: `{path}`")
        return not required

    return _read_expected_artifact(path, expected_kind, label) is not None


def _artifact_exists(path: str) -> bool:
    return bool(path) and Path(path).exists()


def _analysis_artifact(name: str) -> str:
    return str(Path(st.session_state["analysis_dir"]) / name)


def _step_counts(plan_obj: dict[str, Any] | None) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    if not plan_obj:
        return []
    for step in plan_obj.get("steps", []):
        op = step.get("op", "unknown")
        counts[op] = counts.get(op, 0) + 1
    return [{"op": op, "count": count} for op, count in sorted(counts.items())]


def _summary_metric(summary_obj: dict[str, Any] | None, key: str, fallback: int = 0) -> int:
    if not summary_obj:
        return fallback
    value = summary_obj.get(key)
    if isinstance(value, list):
        return len(value)
    if isinstance(value, int):
        return value
    return fallback


def _table_recommendations(summary_obj: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not summary_obj:
        return []
    recommendations = []
    for item in summary_obj.get("table_recommendations", []):
        recommendations.append(
            {
                "table": f"{item.get('schema')}.{item.get('table')}",
                "action": item.get("action"),
                "strategy": item.get("transfer_strategy"),
                "chunk_column": item.get("chunk_column"),
                "chunk_count": item.get("chunk_count"),
                "parallelism": item.get("parallelism"),
                "verification": item.get("verification_depth"),
                "risk": item.get("risk_level"),
                "manual_review": item.get("manual_review_required"),
            }
        )
    return recommendations


def _workflow_status_rows() -> list[dict[str, Any]]:
    return [
        {
            "step": "Analyze",
            "status": (
                "Done" if _artifact_exists(st.session_state["last_summary_path"]) else "Pending"
            ),
            "path": st.session_state["last_summary_path"] or "-",
        },
        {
            "step": "Approve",
            "status": (
                "Done" if _artifact_exists(st.session_state["last_approval_path"]) else "Pending"
            ),
            "path": st.session_state["last_approval_path"] or "-",
        },
        {
            "step": "Run",
            "status": (
                "Done" if _artifact_exists(st.session_state["last_state_path"]) else "Pending"
            ),
            "path": st.session_state["last_state_path"] or "-",
        },
        {
            "step": "Post Summary",
            "status": (
                "Done"
                if _artifact_exists(st.session_state["last_post_summary_path"])
                else "Pending"
            ),
            "path": st.session_state["last_post_summary_path"] or "-",
        },
    ]


def _render_overview(
    summary_obj: dict[str, Any] | None,
    diff_obj: dict[str, Any] | None,
    plan_obj: dict[str, Any] | None,
) -> None:
    overview = (summary_obj or {}).get("overview", {})
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Source Tables", overview.get("source_tables", 0))
    c2.metric("Target Tables", overview.get("target_tables", 0))
    c3.metric("Plan Steps", len((plan_obj or {}).get("steps", [])))
    c4.metric("Copy Candidates", overview.get("tables_to_copy", 0))
    c5.metric("Warnings", len((summary_obj or {}).get("preflight_warnings", [])))

    if diff_obj:
        drift_rows = [
            {
                "category": "missing_tables_in_target",
                "count": len(diff_obj.get("missing_tables_in_target", [])),
            },
            {
                "category": "extra_tables_in_target",
                "count": len(diff_obj.get("extra_tables_in_target", [])),
            },
            {
                "category": "table_metadata_mismatches",
                "count": len(diff_obj.get("table_metadata_mismatches", [])),
            },
            {
                "category": "missing_matviews_in_target",
                "count": len(diff_obj.get("missing_matviews_in_target", [])),
            },
            {"category": "udf_differences", "count": len(diff_obj.get("udf_differences", []))},
        ]
        with st.expander("Drift Snapshot", expanded=False):
            st.dataframe(drift_rows, use_container_width=True, hide_index=True)

    if plan_obj:
        with st.expander("Plan Step Mix", expanded=False):
            st.dataframe(_step_counts(plan_obj), use_container_width=True, hide_index=True)


def _render_pre_summary_block(summary_obj: dict[str, Any]) -> None:
    overview = summary_obj.get("overview", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Copy Candidates", overview.get("tables_to_copy", 0))
    c2.metric("Metadata Sync", overview.get("tables_to_sync_metadata", 0))
    c3.metric("Manual Review", overview.get("manual_review_count", 0))
    c4.metric("Warnings", len(summary_obj.get("preflight_warnings", [])))
    st.text(render_pre_migration_summary(summary_obj))
    recommendations = _table_recommendations(summary_obj)
    if recommendations:
        st.markdown("### Table Recommendations")
        st.dataframe(recommendations, use_container_width=True, hide_index=True)


def _render_state_block(state_obj: dict[str, Any]) -> None:
    completed = len(state_obj.get("completed", []))
    failed = len(state_obj.get("failed", []))
    skipped = len(state_obj.get("skipped", []))
    c1, c2, c3 = st.columns(3)
    c1.metric("Completed Steps", completed)
    c2.metric("Failed Steps", failed)
    c3.metric("Skipped Steps", skipped)
    if failed:
        st.error("Some steps failed. Review the execution state before continuing.")
    with st.expander("Raw state JSON", expanded=False):
        st.json(state_obj)


def _render_post_summary_block(summary_obj: dict[str, Any]) -> None:
    execution_overview = summary_obj.get("execution_overview", {})
    verification_summary = summary_obj.get("verification_summary", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Executed", execution_overview.get("steps_executed", 0))
    c2.metric("Failures", execution_overview.get("step_failures", 0))
    c3.metric("Tables Verified", verification_summary.get("tables_checked", 0))
    c4.metric("Verification OK", "Yes" if verification_summary.get("ok", False) else "No")
    st.text(render_post_migration_summary(summary_obj))
    with st.expander("Raw post summary JSON", expanded=False):
        st.json(summary_obj)


def _analyze(config_path: str, planner: str, mode: MigrationMode, out_dir: str) -> dict[str, str]:
    load_env(".env")
    cfg = load_config(config_path)

    out_root = Path(out_dir)
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
    questions_path = out_root / "clarification_questions.json"
    rationale_path = out_root / "planner_rationale.md"

    write_json(source_manifest_path, source_manifest)
    write_json(target_manifest_path, target_manifest)

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    write_json(diff_path, manifest_diff)

    plan_obj = _generate_plan(
        str(source_manifest_path),
        planner,
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

    return {
        "source_manifest": str(source_manifest_path),
        "target_manifest": str(target_manifest_path),
        "diff": str(diff_path),
        "plan": str(plan_path),
        "summary": str(pre_summary_path),
        "critique": str(critique_path),
        "questions": str(questions_path),
        "rationale": str(rationale_path),
    }


def _approve(
    plan_path: str,
    summary_path: str,
    mode: MigrationMode,
    approved_by: str,
    allow_destructive: bool,
    include_tables: list[str],
    exclude_tables: list[str],
    approved_manual_review_items: list[str],
    notes: str,
    out_path: str,
) -> str:
    approval = build_approval_document(
        plan_path=plan_path,
        summary_path=summary_path,
        approved_mode=mode,
        approved_by=approved_by,
        allow_destructive=allow_destructive,
        include_tables=include_tables or None,
        exclude_tables=exclude_tables or None,
        approved_manual_review_items=approved_manual_review_items or None,
        notes=notes or None,
    )
    write_json(out_path, approval)
    return out_path


def _run(config_path: str, plan_path: str, approval_path: str, state_path: str, fresh: bool) -> str:
    load_env(".env")
    cfg = load_config(config_path)

    state_file = Path(state_path)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    if fresh and state_file.exists():
        state_file.unlink()

    approval_obj = load_approval(approval_path)
    summary_obj = load_pre_summary(approval_obj.summary_path)
    original_plan = read_json(plan_path)
    filtered_plan = filter_plan_for_approval(
        plan=original_plan,
        summary=summary_obj.model_dump(mode="python"),
        approval=approval_obj.model_dump(mode="python"),
    )
    execute(cfg=cfg, plan_path=plan_path, state_path=str(state_file), plan_obj=filtered_plan)
    return str(state_file)


def _summarize_post(
    plan_path: str, state_path: str, pre_summary_path: str, out_path: str, report_path: str = ""
) -> str:
    plan_obj = read_json(plan_path)
    state_obj = read_json(state_path)
    pre_summary_obj = read_json(pre_summary_path)
    report_obj = read_json(report_path) if report_path and Path(report_path).exists() else None

    summary = build_post_migration_summary(
        plan=plan_obj,
        state=state_obj,
        report=report_obj,
        pre_summary=pre_summary_obj,
    )
    write_json(out_path, summary)
    Path(out_path).with_suffix(".md").write_text(
        render_post_migration_summary(summary), encoding="utf-8"
    )
    failure_analysis = build_failure_analysis(
        plan=plan_obj,
        state=state_obj,
        report=report_obj,
        pre_summary=pre_summary_obj,
    )
    failure_json_path = Path(out_path).with_name("failure_analysis.json")
    failure_md_path = Path(out_path).with_name("failure_analysis.md")
    write_json(failure_json_path, failure_analysis)
    failure_md_path.write_text(render_failure_analysis(failure_analysis), encoding="utf-8")
    return out_path


def main() -> None:
    st.set_page_config(page_title="Agentic DB Migrator", layout="wide")
    _session_defaults()

    st.title("Agentic DB Migrator")
    st.caption("A browser workflow for analyze, review, approve, run, and summarize-post.")

    if st.session_state.get("flash_message"):
        flash_kind = st.session_state.get("flash_kind", "success")
        flash_message = st.session_state["flash_message"]
        if flash_kind == "warning":
            st.warning(flash_message)
        elif flash_kind == "error":
            st.error(flash_message)
        else:
            st.success(flash_message)
        st.session_state["flash_message"] = ""
        st.session_state["flash_kind"] = "success"

    with st.sidebar:
        st.header("Run Settings")
        config_path = st.text_input("Config path", value="config.yaml", key="sidebar_config_path")
        planner = st.selectbox(
            "Planner", options=list(PLANNER_MODULES), index=0, key="sidebar_planner"
        )
        mode = st.selectbox("Migration mode", options=MIGRATION_MODES, index=0, key="sidebar_mode")
        analysis_dir = st.text_input(
            "Analysis output directory",
            value=st.session_state["analysis_dir"],
            key="sidebar_analysis_dir",
        )
        approved_by = st.text_input(
            "Approved by", value="streamlit-user", key="sidebar_approved_by"
        )
        fresh_run = st.checkbox("Fresh run", value=True, key="sidebar_fresh_run")
        st.markdown("### Workflow Status")
        st.dataframe(_workflow_status_rows(), use_container_width=True, hide_index=True)

    latest_summary = _read_expected_artifact(
        st.session_state["last_summary_path"],
        "pre_migration_summary",
        "Latest summary",
    )
    latest_plan = _read_expected_artifact(
        st.session_state["last_plan_path"],
        "migration_plan",
        "Latest plan",
    )
    latest_diff = _read_expected_artifact(
        _analysis_artifact("manifest_diff.json"),
        "manifest_diff",
        "Latest manifest diff",
    )

    st.markdown("## Workflow Dashboard")
    _render_overview(latest_summary, latest_diff, latest_plan)

    tabs = st.tabs(["Analyze", "Review", "Approve", "Run", "Post Summary", "Artifacts"])

    with tabs[0]:
        st.subheader("Analyze")
        st.write(
            "Build source and target manifests, compute drift, generate a plan, and render a pre-migration summary."
        )
        if st.button("Run Analyze", type="primary", key="analyze_run_button"):
            try:
                artifacts, stdout, stderr = _capture(
                    _analyze, config_path, planner, mode, analysis_dir
                )
                st.session_state["analysis_dir"] = analysis_dir
                st.session_state["last_plan_path"] = artifacts["plan"]
                st.session_state["last_summary_path"] = artifacts["summary"]
                st.session_state["last_critique_path"] = artifacts["critique"]
                st.session_state["last_questions_path"] = artifacts["questions"]
                st.session_state["last_rationale_path"] = artifacts["rationale"]
                st.session_state["flash_message"] = "Analysis completed."
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        summary_obj = _read_expected_artifact(
            st.session_state["last_summary_path"],
            "pre_migration_summary",
            "Latest pre-migration summary",
        )
        if summary_obj:
            st.markdown("### Latest Pre-Migration Summary")
            _render_pre_summary_block(summary_obj)
        critique_obj = _read_expected_artifact(
            st.session_state["last_critique_path"],
            "planner_critique",
            "Planner critique",
        )
        if critique_obj:
            with st.expander("Planner Critic Agent", expanded=bool(critique_obj.get("findings"))):
                st.metric("Critic Score", critique_obj.get("score", 0))
                st.json(critique_obj)
        questions_obj = _read_expected_artifact(
            st.session_state["last_questions_path"],
            "clarification_questions",
            "Clarification questions",
        )
        if questions_obj:
            with st.expander(
                "Interactive Clarification Questions", expanded=bool(questions_obj.get("questions"))
            ):
                st.json(questions_obj)
        if (
            st.session_state["last_rationale_path"]
            and Path(st.session_state["last_rationale_path"]).exists()
        ):
            with st.expander("Plan Rationale", expanded=False):
                st.markdown(
                    Path(st.session_state["last_rationale_path"]).read_text(encoding="utf-8")
                )

    with tabs[1]:
        st.subheader("Review")
        review_summary_path = st.text_input(
            "Summary path", value=st.session_state["last_summary_path"], key="review_summary_path"
        )
        summary_obj = _read_expected_artifact(
            review_summary_path,
            "pre_migration_summary",
            "Review Summary path",
        )
        if summary_obj:
            _render_pre_summary_block(summary_obj)
            with st.expander("Raw summary JSON", expanded=False):
                st.json(summary_obj)
            critique_obj = _read_expected_artifact(
                st.session_state["last_critique_path"],
                "planner_critique",
                "Planner critique",
            )
            if critique_obj:
                with st.expander("Planner Critic Agent", expanded=False):
                    st.json(critique_obj)
            questions_obj = _read_expected_artifact(
                st.session_state["last_questions_path"],
                "clarification_questions",
                "Clarification questions",
            )
            if questions_obj:
                with st.expander("Clarification Questions", expanded=False):
                    st.json(questions_obj)
        else:
            st.info("Run Analyze first or enter an existing pre_migration_summary.json path.")

    with tabs[2]:
        st.subheader("Approve")
        plan_path = st.text_input(
            "Plan path", value=st.session_state["last_plan_path"], key="approve_plan_path"
        )
        summary_path = st.text_input(
            "Summary path", value=st.session_state["last_summary_path"], key="approve_summary_path"
        )
        approval_path = st.text_input(
            "Approval output path",
            value=str(Path(st.session_state["analysis_dir"]) / "approval.json"),
            key="approve_output_path",
        )

        _read_expected_artifact(
            plan_path,
            "migration_plan",
            "Approve Plan path",
        )
        summary_obj = _read_expected_artifact(
            summary_path,
            "pre_migration_summary",
            "Approve Summary path",
        )
        table_options: list[str] = []
        manual_review_options: list[str] = []
        default_include: list[str] = []
        if summary_obj:
            table_options = [
                f"{item['schema']}.{item['table']}"
                for item in summary_obj.get("table_recommendations", [])
            ]
            default_include = [
                f"{item['schema']}.{item['table']}"
                for item in summary_obj.get("table_recommendations", [])
                if item.get("action") in ("copy", "sync_metadata")
            ]
            manual_review_options = list(summary_obj.get("manual_review_required", []))

        include_tables = st.multiselect(
            "Included tables",
            options=table_options,
            default=default_include,
            key="approve_include_tables",
        )
        exclude_tables = st.multiselect(
            "Excluded tables", options=table_options, default=[], key="approve_exclude_tables"
        )
        approve_manual_review_items = st.multiselect(
            "Approved manual-review items",
            options=manual_review_options,
            default=[],
            key="approve_manual_review_items",
        )
        allow_destructive = st.checkbox(
            "Allow destructive actions", value=False, key="approve_allow_destructive"
        )
        notes = st.text_area("Approval notes", value="", key="approve_notes")

        if st.button("Create Approval", key="approve_create_button"):
            inputs_ok = all(
                [
                    _validate_artifact_path(plan_path, "migration_plan", "Approve Plan path"),
                    _validate_artifact_path(
                        summary_path,
                        "pre_migration_summary",
                        "Approve Summary path",
                    ),
                ]
            )
            if not inputs_ok:
                st.stop()
            try:
                created_path, stdout, stderr = _capture(
                    _approve,
                    plan_path,
                    summary_path,
                    mode,
                    approved_by,
                    allow_destructive,
                    include_tables,
                    exclude_tables,
                    approve_manual_review_items,
                    notes,
                    approval_path,
                )
                st.session_state["last_approval_path"] = created_path
                st.session_state["flash_message"] = f"Approval written to {created_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

    with tabs[3]:
        st.subheader("Run")
        run_plan_path = st.text_input(
            "Plan path ", value=st.session_state["last_plan_path"], key="run_plan_path"
        )
        run_approval_path = st.text_input(
            "Approval path", value=st.session_state["last_approval_path"], key="run_approval_path"
        )
        default_state_path = str(
            Path("runs") / f"state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        run_state_path = st.text_input(
            "State output path",
            value=st.session_state.get("last_state_path") or default_state_path,
            key="run_state_path",
        )

        _read_expected_artifact(run_plan_path, "migration_plan", "Run Plan path")
        _read_expected_artifact(run_approval_path, "approval", "Run Approval path")

        if st.button("Execute Migration", key="run_execute_button"):
            inputs_ok = all(
                [
                    _validate_artifact_path(run_plan_path, "migration_plan", "Run Plan path"),
                    _validate_artifact_path(
                        run_approval_path,
                        "approval",
                        "Run Approval path",
                    ),
                ]
            )
            if not inputs_ok:
                st.stop()
            try:
                state_path, stdout, stderr = _capture(
                    _run, config_path, run_plan_path, run_approval_path, run_state_path, fresh_run
                )
                st.session_state["last_state_path"] = state_path
                st.session_state["flash_message"] = f"Run complete. State saved to {state_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        latest_state = _read_expected_artifact(
            st.session_state["last_state_path"],
            "run_state",
            "Latest run state",
        )
        if latest_state:
            st.markdown("### Latest Execution Summary")
            _render_state_block(latest_state)

    with tabs[4]:
        st.subheader("Post-Migration Summary")
        post_plan_path = st.text_input(
            "Migration Plan File", value=st.session_state["last_plan_path"], key="post_plan_path"
        )
        post_state_path = st.text_input(
            "Run State File", value=st.session_state["last_state_path"], key="post_state_path"
        )
        post_pre_summary_path = st.text_input(
            "Pre-Migration Summary File",
            value=st.session_state["last_summary_path"],
            key="post_pre_summary_path",
        )
        post_report_path = st.text_input(
            "Verification Report File (Optional)",
            value=st.session_state["last_report_path"],
            key="post_report_path",
        )
        post_summary_path = st.text_input(
            "Post-Migration Summary Output File",
            value=str(Path(st.session_state["analysis_dir"]) / "post_migration_summary.json"),
            key="post_summary_output_path",
        )

        _read_expected_artifact(post_plan_path, "migration_plan", "Post Summary Plan file")
        _read_expected_artifact(post_state_path, "run_state", "Post Summary Run State file")
        _read_expected_artifact(
            post_pre_summary_path,
            "pre_migration_summary",
            "Post Summary Pre-Migration Summary file",
        )
        if post_report_path:
            _read_expected_artifact(
                post_report_path,
                "verification_report",
                "Post Summary Verification Report file",
            )

        if st.button("Build Post Summary", key="post_build_button"):
            inputs_ok = all(
                [
                    _validate_artifact_path(
                        post_plan_path,
                        "migration_plan",
                        "Post Summary Plan file",
                    ),
                    _validate_artifact_path(
                        post_state_path,
                        "run_state",
                        "Post Summary Run State file",
                    ),
                    _validate_artifact_path(
                        post_pre_summary_path,
                        "pre_migration_summary",
                        "Post Summary Pre-Migration Summary file",
                    ),
                    _validate_artifact_path(
                        post_report_path,
                        "verification_report",
                        "Post Summary Verification Report file",
                        required=False,
                    ),
                ]
            )
            if not inputs_ok:
                st.stop()
            try:
                created_path, stdout, stderr = _capture(
                    _summarize_post,
                    post_plan_path,
                    post_state_path,
                    post_pre_summary_path,
                    post_summary_path,
                    post_report_path,
                )
                st.session_state["last_post_summary_path"] = created_path
                st.session_state["last_failure_analysis_path"] = str(
                    Path(created_path).with_name("failure_analysis.json")
                )
                st.session_state["flash_message"] = f"Post summary written to {created_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        latest_post = _read_expected_artifact(
            st.session_state["last_post_summary_path"],
            "post_migration_summary",
            "Latest post-migration summary",
        )
        if latest_post:
            st.markdown("### Latest Post-Migration Summary")
            _render_post_summary_block(latest_post)
        failure_obj = _read_expected_artifact(
            st.session_state["last_failure_analysis_path"],
            "failure_analysis",
            "Failure analysis",
        )
        if failure_obj:
            with st.expander(
                "Post-Run Failure Analyst", expanded=bool(failure_obj.get("findings"))
            ):
                st.json(failure_obj)

    with tabs[5]:
        st.subheader("Artifacts")
        artifact_rows = [
            {
                "artifact": "Analysis Directory",
                "path": st.session_state["analysis_dir"],
                "exists": Path(st.session_state["analysis_dir"]).exists(),
            },
            {
                "artifact": "Plan",
                "path": st.session_state["last_plan_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_plan_path"]),
            },
            {
                "artifact": "Pre-Migration Summary",
                "path": st.session_state["last_summary_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_summary_path"]),
            },
            {
                "artifact": "Planner Critique",
                "path": st.session_state["last_critique_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_critique_path"]),
            },
            {
                "artifact": "Clarification Questions",
                "path": st.session_state["last_questions_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_questions_path"]),
            },
            {
                "artifact": "Plan Rationale",
                "path": st.session_state["last_rationale_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_rationale_path"]),
            },
            {
                "artifact": "Approval",
                "path": st.session_state["last_approval_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_approval_path"]),
            },
            {
                "artifact": "Run State",
                "path": st.session_state["last_state_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_state_path"]),
            },
            {
                "artifact": "Post Summary",
                "path": st.session_state["last_post_summary_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_post_summary_path"]),
            },
            {
                "artifact": "Failure Analysis",
                "path": st.session_state["last_failure_analysis_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_failure_analysis_path"]),
            },
        ]
        st.dataframe(artifact_rows, use_container_width=True, hide_index=True)
        latest_plan = _read_expected_artifact(
            st.session_state["last_plan_path"],
            "migration_plan",
            "Artifacts Plan",
        )
        if latest_plan:
            with st.expander("Raw plan JSON", expanded=False):
                st.json(latest_plan)


if __name__ == "__main__":
    main()

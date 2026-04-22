from __future__ import annotations

import contextlib
import importlib
import io
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

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
    "ollama": "amo.core.planners.ollama",
}

MIGRATION_MODES: List[MigrationMode] = [
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


def _generate_plan(manifest_path: str, planner: str) -> dict:
    module = _load_planner_module(planner)
    return validate_plan_document(module.generate_plan(manifest_path=manifest_path))


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
        "flash_message": "",
        "flash_kind": "success",
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def _read_if_exists(path: str) -> Dict[str, Any] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    return read_json(p)


def _artifact_exists(path: str) -> bool:
    return bool(path) and Path(path).exists()


def _analysis_artifact(name: str) -> str:
    return str(Path(st.session_state["analysis_dir"]) / name)


def _step_counts(plan_obj: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    if not plan_obj:
        return []
    for step in plan_obj.get("steps", []):
        op = step.get("op", "unknown")
        counts[op] = counts.get(op, 0) + 1
    return [{"op": op, "count": count} for op, count in sorted(counts.items())]


def _summary_metric(summary_obj: Dict[str, Any] | None, key: str, fallback: int = 0) -> int:
    if not summary_obj:
        return fallback
    value = summary_obj.get(key)
    if isinstance(value, list):
        return len(value)
    if isinstance(value, int):
        return value
    return fallback


def _table_recommendations(summary_obj: Dict[str, Any] | None) -> List[Dict[str, Any]]:
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


def _workflow_status_rows() -> List[Dict[str, Any]]:
    return [
        {
            "step": "Analyze",
            "status": "Done" if _artifact_exists(st.session_state["last_summary_path"]) else "Pending",
            "path": st.session_state["last_summary_path"] or "-",
        },
        {
            "step": "Approve",
            "status": "Done" if _artifact_exists(st.session_state["last_approval_path"]) else "Pending",
            "path": st.session_state["last_approval_path"] or "-",
        },
        {
            "step": "Run",
            "status": "Done" if _artifact_exists(st.session_state["last_state_path"]) else "Pending",
            "path": st.session_state["last_state_path"] or "-",
        },
        {
            "step": "Post Summary",
            "status": "Done" if _artifact_exists(st.session_state["last_post_summary_path"]) else "Pending",
            "path": st.session_state["last_post_summary_path"] or "-",
        },
    ]


def _render_overview(summary_obj: Dict[str, Any] | None, diff_obj: Dict[str, Any] | None, plan_obj: Dict[str, Any] | None) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Plan Steps", len((plan_obj or {}).get("steps", [])))
    c2.metric("Copy Candidates", len([row for row in _table_recommendations(summary_obj) if row.get("action") == "copy"]))
    c3.metric("Metadata Sync", len([row for row in _table_recommendations(summary_obj) if row.get("action") == "sync_metadata"]))
    c4.metric("Manual Review", _summary_metric(summary_obj, "manual_review_required"))
    c5.metric("Warnings", _summary_metric(summary_obj, "warnings"))

    if diff_obj:
        drift_rows = [
            {"category": "missing_tables_in_target", "count": len(diff_obj.get("missing_tables_in_target", []))},
            {"category": "extra_tables_in_target", "count": len(diff_obj.get("extra_tables_in_target", []))},
            {"category": "table_metadata_mismatches", "count": len(diff_obj.get("table_metadata_mismatches", []))},
            {"category": "missing_matviews_in_target", "count": len(diff_obj.get("missing_matviews_in_target", []))},
            {"category": "udf_differences", "count": len(diff_obj.get("udf_differences", []))},
        ]
        with st.expander("Drift Snapshot", expanded=False):
            st.dataframe(drift_rows, use_container_width=True, hide_index=True)

    if plan_obj:
        with st.expander("Plan Step Mix", expanded=False):
            st.dataframe(_step_counts(plan_obj), use_container_width=True, hide_index=True)


def _render_pre_summary_block(summary_obj: Dict[str, Any]) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Copy Candidates", len([row for row in _table_recommendations(summary_obj) if row.get("action") == "copy"]))
    c2.metric("Metadata Sync", len([row for row in _table_recommendations(summary_obj) if row.get("action") == "sync_metadata"]))
    c3.metric("Manual Review", _summary_metric(summary_obj, "manual_review_required"))
    c4.metric("Warnings", _summary_metric(summary_obj, "warnings"))
    st.text(render_pre_migration_summary(summary_obj))
    recommendations = _table_recommendations(summary_obj)
    if recommendations:
        st.markdown("### Table Recommendations")
        st.dataframe(recommendations, use_container_width=True, hide_index=True)


def _render_state_block(state_obj: Dict[str, Any]) -> None:
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


def _render_post_summary_block(summary_obj: Dict[str, Any]) -> None:
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


def _analyze(config_path: str, planner: str, mode: MigrationMode, out_dir: str) -> Dict[str, str]:
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

    write_json(source_manifest_path, source_manifest)
    write_json(target_manifest_path, target_manifest)

    manifest_diff = diff_manifests(source_manifest, target_manifest)
    write_json(diff_path, manifest_diff)

    plan_obj = _generate_plan(str(source_manifest_path), planner)
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

    return {
        "source_manifest": str(source_manifest_path),
        "target_manifest": str(target_manifest_path),
        "diff": str(diff_path),
        "plan": str(plan_path),
        "summary": str(pre_summary_path),
    }


def _approve(
    plan_path: str,
    summary_path: str,
    mode: MigrationMode,
    approved_by: str,
    allow_destructive: bool,
    include_tables: List[str],
    exclude_tables: List[str],
    approved_manual_review_items: List[str],
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


def _summarize_post(plan_path: str, state_path: str, pre_summary_path: str, out_path: str, report_path: str = "") -> str:
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
    Path(out_path).with_suffix(".md").write_text(render_post_migration_summary(summary), encoding="utf-8")
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
        planner = st.selectbox("Planner", options=list(PLANNER_MODULES), index=0, key="sidebar_planner")
        mode = st.selectbox("Migration mode", options=MIGRATION_MODES, index=0, key="sidebar_mode")
        analysis_dir = st.text_input("Analysis output directory", value=st.session_state["analysis_dir"], key="sidebar_analysis_dir")
        approved_by = st.text_input("Approved by", value="streamlit-user", key="sidebar_approved_by")
        fresh_run = st.checkbox("Fresh run", value=True, key="sidebar_fresh_run")
        st.markdown("### Workflow Status")
        st.dataframe(_workflow_status_rows(), use_container_width=True, hide_index=True)

    latest_summary = _read_if_exists(st.session_state["last_summary_path"])
    latest_plan = _read_if_exists(st.session_state["last_plan_path"])
    latest_diff = _read_if_exists(_analysis_artifact("manifest_diff.json"))

    st.markdown("## Workflow Dashboard")
    _render_overview(latest_summary, latest_diff, latest_plan)

    tabs = st.tabs(["Analyze", "Review", "Approve", "Run", "Post Summary", "Artifacts"])

    with tabs[0]:
        st.subheader("Analyze")
        st.write("Build source and target manifests, compute drift, generate a plan, and render a pre-migration summary.")
        if st.button("Run Analyze", type="primary", key="analyze_run_button"):
            try:
                artifacts, stdout, stderr = _capture(_analyze, config_path, planner, mode, analysis_dir)
                st.session_state["analysis_dir"] = analysis_dir
                st.session_state["last_plan_path"] = artifacts["plan"]
                st.session_state["last_summary_path"] = artifacts["summary"]
                st.session_state["flash_message"] = "Analysis completed."
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        summary_obj = _read_if_exists(st.session_state["last_summary_path"])
        if summary_obj:
            st.markdown("### Latest Pre-Migration Summary")
            _render_pre_summary_block(summary_obj)

    with tabs[1]:
        st.subheader("Review")
        review_summary_path = st.text_input("Summary path", value=st.session_state["last_summary_path"], key="review_summary_path")
        summary_obj = _read_if_exists(review_summary_path)
        if summary_obj:
            _render_pre_summary_block(summary_obj)
            with st.expander("Raw summary JSON", expanded=False):
                st.json(summary_obj)
        else:
            st.info("Run Analyze first or enter an existing pre_migration_summary.json path.")

    with tabs[2]:
        st.subheader("Approve")
        plan_path = st.text_input("Plan path", value=st.session_state["last_plan_path"], key="approve_plan_path")
        summary_path = st.text_input("Summary path", value=st.session_state["last_summary_path"], key="approve_summary_path")
        approval_path = st.text_input(
            "Approval output path",
            value=str(Path(st.session_state["analysis_dir"]) / "approval.json"),
            key="approve_output_path",
        )

        summary_obj = _read_if_exists(summary_path)
        table_options: List[str] = []
        manual_review_options: List[str] = []
        default_include: List[str] = []
        if summary_obj:
            table_options = [f"{item['schema']}.{item['table']}" for item in summary_obj.get("table_recommendations", [])]
            default_include = [
                f"{item['schema']}.{item['table']}"
                for item in summary_obj.get("table_recommendations", [])
                if item.get("action") in ("copy", "sync_metadata")
            ]
            manual_review_options = list(summary_obj.get("manual_review_required", []))

        include_tables = st.multiselect("Included tables", options=table_options, default=default_include, key="approve_include_tables")
        exclude_tables = st.multiselect("Excluded tables", options=table_options, default=[], key="approve_exclude_tables")
        approve_manual_review_items = st.multiselect(
            "Approved manual-review items",
            options=manual_review_options,
            default=[],
            key="approve_manual_review_items",
        )
        allow_destructive = st.checkbox("Allow destructive actions", value=False, key="approve_allow_destructive")
        notes = st.text_area("Approval notes", value="", key="approve_notes")

        if st.button("Create Approval", key="approve_create_button"):
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
        run_plan_path = st.text_input("Plan path ", value=st.session_state["last_plan_path"], key="run_plan_path")
        run_approval_path = st.text_input("Approval path", value=st.session_state["last_approval_path"], key="run_approval_path")
        default_state_path = str(Path("runs") / f"state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        run_state_path = st.text_input("State output path", value=st.session_state.get("last_state_path") or default_state_path, key="run_state_path")

        if st.button("Execute Migration", key="run_execute_button"):
            try:
                state_path, stdout, stderr = _capture(_run, config_path, run_plan_path, run_approval_path, run_state_path, fresh_run)
                st.session_state["last_state_path"] = state_path
                st.session_state["flash_message"] = f"Run complete. State saved to {state_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        latest_state = _read_if_exists(st.session_state["last_state_path"])
        if latest_state:
            st.markdown("### Latest Execution Summary")
            _render_state_block(latest_state)

    with tabs[4]:
        st.subheader("Post-Migration Summary")
        post_plan_path = st.text_input("Migration Plan File", value=st.session_state["last_plan_path"], key="post_plan_path")
        post_state_path = st.text_input("Run State File", value=st.session_state["last_state_path"], key="post_state_path")
        post_pre_summary_path = st.text_input("Pre-Migration Summary File", value=st.session_state["last_summary_path"], key="post_pre_summary_path")
        post_report_path = st.text_input("Verification Report File (Optional)", value=st.session_state["last_report_path"], key="post_report_path")
        post_summary_path = st.text_input(
            "Post-Migration Summary Output File",
            value=str(Path(st.session_state["analysis_dir"]) / "post_migration_summary.json"),
            key="post_summary_output_path",
        )

        if st.button("Build Post Summary", key="post_build_button"):
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
                st.session_state["flash_message"] = f"Post summary written to {created_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

        latest_post = _read_if_exists(st.session_state["last_post_summary_path"])
        if latest_post:
            st.markdown("### Latest Post-Migration Summary")
            _render_post_summary_block(latest_post)

    with tabs[5]:
        st.subheader("Artifacts")
        artifact_rows = [
            {"artifact": "Analysis Directory", "path": st.session_state["analysis_dir"], "exists": Path(st.session_state["analysis_dir"]).exists()},
            {"artifact": "Plan", "path": st.session_state["last_plan_path"] or "-", "exists": _artifact_exists(st.session_state["last_plan_path"])},
            {"artifact": "Pre-Migration Summary", "path": st.session_state["last_summary_path"] or "-", "exists": _artifact_exists(st.session_state["last_summary_path"])},
            {"artifact": "Approval", "path": st.session_state["last_approval_path"] or "-", "exists": _artifact_exists(st.session_state["last_approval_path"])},
            {"artifact": "Run State", "path": st.session_state["last_state_path"] or "-", "exists": _artifact_exists(st.session_state["last_state_path"])},
            {"artifact": "Post Summary", "path": st.session_state["last_post_summary_path"] or "-", "exists": _artifact_exists(st.session_state["last_post_summary_path"])},
        ]
        st.dataframe(artifact_rows, use_container_width=True, hide_index=True)
        latest_plan = _read_if_exists(st.session_state["last_plan_path"])
        if latest_plan:
            with st.expander("Raw plan JSON", expanded=False):
                st.json(latest_plan)


if __name__ == "__main__":
    main()


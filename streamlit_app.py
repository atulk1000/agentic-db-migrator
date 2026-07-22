from __future__ import annotations

import contextlib
import hashlib
import importlib
import inspect
import io
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st
import yaml

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
    build_post_migration_summary,
    build_pre_migration_summary,
    diff_manifests,
    read_json,
    render_post_migration_summary,
    render_pre_migration_summary,
    write_json,
)
from amo.core.config import load_config, load_env
from amo.core.executor import execute
from amo.core.planners.heuristic_planner import write_plan
from amo.core.planners.models import validate_plan_document
from amo.core.policy import build_approved_execution_bundle
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

DATABASE_TYPES = {
    "PostgreSQL": "postgresql",
}


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


def _default_streamlit_config_path() -> str:
    return str(Path("runs") / "streamlit_config.yaml")


def _capture(callable_obj, *args, **kwargs):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        result = callable_obj(*args, **kwargs)
    return result, stdout.getvalue(), stderr.getvalue()


def _database_config(
    *,
    db_type: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> dict[str, Any]:
    return {
        "type": db_type,
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }


def _database_config_with_env_password(
    *,
    db_type: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    password_env: str,
) -> dict[str, Any]:
    os.environ[password_env] = password
    return _database_config(
        db_type=db_type,
        host=host,
        port=port,
        database=database,
        user=user,
        password=f"${{{password_env}}}",
    )


def _build_browser_config(
    *,
    source: dict[str, Any],
    target: dict[str, Any],
    planner: str,
    max_partitions: int,
    batch_size: int,
    include_schemas: list[str],
    exclude_schemas: list[str],
    allow_destructive: bool,
    truncate_first: bool,
    sample_hash: bool,
    maintenance: str,
) -> dict[str, Any]:
    return {
        "app": {"name": "agentic-migration-orchestrator"},
        "engine": {
            "type": "copy",
            "auto_ddl": True,
            "allow_destructive": allow_destructive,
            "verify_inline": False,
            "copy": {
                "truncate_first": truncate_first,
                "spool_dir": None,
                "batchsize": batch_size,
            },
        },
        "source": source,
        "target": target,
        "migration": {
            "include_schemas": include_schemas,
            "exclude_schemas": exclude_schemas,
        },
        "exclude_schemas": exclude_schemas,
        "exclude_tables": ["spatial_ref_sys", "geometry_columns", "geography_columns"],
        "exclude_suffixes": [],
        "planning": {
            "planner": planner,
            "max_partitions": max_partitions,
            "default_batch_size": batch_size,
        },
        "post_migration": {"maintenance": maintenance},
        "verify": {
            "sample_hash": sample_hash,
            "sample_rows": 200,
            "checks": {
                "rowcount": True,
                "sample_hash": sample_hash,
                "indexes": False,
                "primary_keys": False,
                "matviews": False,
                "geometry": False,
            },
        },
    }


def _write_browser_config(config: dict[str, Any], out_path: str) -> str:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(config, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return str(path)


def _is_demo_target(target: dict[str, Any]) -> bool:
    host = str(target.get("host", "")).lower()
    return (
        host in ("localhost", "127.0.0.1", "::1")
        and int(target.get("port", 0) or 0) == 5434
        and target.get("database") == "targetdb"
        and target.get("user") == "target"
    )


def _reset_target_demo_db(target: dict[str, Any]) -> str:
    if not _is_demo_target(target):
        raise RuntimeError(
            "Target reset is only enabled for the local Docker demo target "
            "(localhost:5434/targetdb as user target)."
        )

    import psycopg2

    conn = psycopg2.connect(
        host=target["host"],
        port=target.get("port", 5432),
        dbname=target["database"],
        user=target["user"],
        password=target["password"],
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                DO $$
                DECLARE r record;
                BEGIN
                  FOR r IN
                    SELECT nspname
                    FROM pg_namespace
                    WHERE nspname NOT LIKE 'pg_%'
                      AND nspname <> 'information_schema'
                  LOOP
                    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', r.nspname);
                  END LOOP;
                END $$;
                CREATE SCHEMA IF NOT EXISTS public;
                """)
    finally:
        conn.close()
    return (
        "Target demo database reset. All non-system schemas were dropped and public was recreated."
    )


def _session_defaults() -> None:
    defaults = {
        "active_config_path": "config.yaml",
        "config_output_path": _default_streamlit_config_path(),
        "analysis_dir": _default_analysis_dir(),
        "last_source_manifest_path": "",
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
    if "approved_mode" in obj and (
        "summary_path" in obj or (obj.get("schema_version") == "2" and "summary" in obj)
    ):
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


def _overview_rows(summary_obj: dict[str, Any]) -> list[dict[str, Any]]:
    overview = summary_obj.get("overview", {})
    return [
        {"metric": "Mode", "value": overview.get("mode", "-")},
        {"metric": "Planner", "value": overview.get("planner", "-")},
        {"metric": "Source Tables", "value": overview.get("source_tables", 0)},
        {"metric": "Target Tables", "value": overview.get("target_tables", 0)},
        {"metric": "Copy Candidates", "value": overview.get("tables_to_copy", 0)},
        {
            "metric": "Metadata Sync Candidates",
            "value": overview.get("tables_to_sync_metadata", 0),
        },
        {"metric": "Manual Review Required", "value": overview.get("manual_review_count", 0)},
        {"metric": "Skipped Tables", "value": overview.get("skipped_tables", 0)},
    ]


def _drift_summary_rows(summary_obj: dict[str, Any]) -> list[dict[str, Any]]:
    drift = summary_obj.get("drift_summary", {})
    labels = {
        "source_tables": "Source Tables",
        "target_tables": "Target Tables",
        "missing_in_target": "Missing In Target",
        "missing_in_source": "Missing In Source",
        "metadata_match": "Metadata Match",
        "metadata_diff": "Metadata Diff",
    }
    return [{"metric": label, "value": drift.get(key, 0)} for key, label in labels.items()]


def _post_execution_rows(summary_obj: dict[str, Any]) -> list[dict[str, Any]]:
    execution = summary_obj.get("execution_overview", {})
    labels = {
        "total_steps": "Total Steps",
        "completed_steps": "Completed Steps",
        "failed_steps": "Failed Steps",
        "skipped_steps": "Skipped Steps",
        "success": "Success",
    }
    return [{"metric": label, "value": execution.get(key, 0)} for key, label in labels.items()]


def _post_verification_rows(summary_obj: dict[str, Any]) -> list[dict[str, Any]]:
    verification = summary_obj.get("verification_summary", {})
    rows = [
        {"metric": "Verification OK", "value": verification.get("ok", False)},
        {"metric": "Tables Checked", "value": verification.get("tables_checked", 0)},
    ]
    failed_tables = verification.get("failed_tables", [])
    rows.append({"metric": "Failed Tables", "value": len(failed_tables)})
    return rows


def _list_rows(values: list[Any], column: str) -> list[dict[str, Any]]:
    return [{column: value} for value in values]


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _safe_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _approval_overlap(include_tables: list[str], exclude_tables: list[str]) -> list[str]:
    return sorted(set(include_tables).intersection(exclude_tables))


def _approved_table_candidates(
    summary_obj: dict[str, Any],
    include_tables: list[str],
    exclude_tables: list[str],
    approved_manual_review_items: list[str],
    allow_destructive: bool,
    approved_mode: MigrationMode,
) -> list[str]:
    if approved_mode == "plan_only":
        return []

    included = set(include_tables)
    excluded = set(exclude_tables)
    manual = set(approved_manual_review_items)
    approved: list[str] = []
    for item in summary_obj.get("table_recommendations", []):
        key = f"{item.get('schema')}.{item.get('table')}"
        if key not in included or key in excluded:
            continue
        if item.get("action") == "manual_review":
            if key in manual and allow_destructive:
                approved.append(key)
            continue
        if item.get("action") in ("copy", "sync_metadata"):
            approved.append(key)
    return sorted(approved)


def _default_load_strategy(item: dict[str, Any], allow_destructive: bool) -> str:
    if item.get("action") != "copy":
        return "skip"
    if allow_destructive:
        return "truncate_reload"
    return "append_only"


def _strategy_options(item: dict[str, Any], allow_destructive: bool) -> list[str]:
    if item.get("action") != "copy":
        return ["skip"]

    options = ["append_only"]
    if item.get("upsert_eligible"):
        options.append("upsert")
    if allow_destructive:
        options.append("truncate_reload")
    options.append("skip")
    return options


def _table_strategy_payload(
    table_key: str, strategy: str, recommendation: dict[str, Any]
) -> dict[str, Any]:
    payload: dict[str, Any] = {"strategy": strategy}
    if strategy == "upsert":
        payload["conflict_key"] = list(recommendation.get("conflict_key") or [])
    return payload


def _workflow_status_rows() -> list[dict[str, Any]]:
    return [
        {
            "step": "Config",
            "status": (
                "Done" if _artifact_exists(st.session_state["active_config_path"]) else "Pending"
            ),
            "path": st.session_state["active_config_path"] or "-",
        },
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

    c_overview, c_drift = st.columns(2)
    with c_overview:
        st.markdown("### Overview")
        st.dataframe(_overview_rows(summary_obj), use_container_width=True, hide_index=True)
    with c_drift:
        st.markdown("### Drift Summary")
        st.dataframe(_drift_summary_rows(summary_obj), use_container_width=True, hide_index=True)

    warnings = summary_obj.get("preflight_warnings", [])
    if warnings:
        st.markdown("### Preflight Warnings")
        st.dataframe(_list_rows(warnings, "warning"), use_container_width=True, hide_index=True)

    manual_review = summary_obj.get("manual_review_required", [])
    if manual_review:
        st.markdown("### Manual Review Required")
        st.dataframe(_list_rows(manual_review, "table"), use_container_width=True, hide_index=True)

    destructive_actions = summary_obj.get("destructive_actions", [])
    if destructive_actions:
        st.markdown("### Destructive Actions")
        st.dataframe(
            _list_rows(destructive_actions, "action"), use_container_width=True, hide_index=True
        )

    if summary_obj.get("planner_recommendation"):
        st.info(summary_obj["planner_recommendation"])

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
    c1.metric("Completed Steps", execution_overview.get("completed_steps", 0))
    c2.metric("Failed Steps", execution_overview.get("failed_steps", 0))
    c3.metric("Tables Verified", verification_summary.get("tables_checked", 0))
    c4.metric("Verification OK", "Yes" if verification_summary.get("ok", False) else "No")

    c_execution, c_verification = st.columns(2)
    with c_execution:
        st.markdown("### Execution Overview")
        st.dataframe(_post_execution_rows(summary_obj), use_container_width=True, hide_index=True)
    with c_verification:
        st.markdown("### Verification Summary")
        st.dataframe(
            _post_verification_rows(summary_obj), use_container_width=True, hide_index=True
        )

    failed_steps = summary_obj.get("failed_steps", [])
    if failed_steps:
        st.markdown("### Failed Steps")
        st.dataframe(_list_rows(failed_steps, "step_id"), use_container_width=True, hide_index=True)

    failed_tables = verification_summary.get("failed_tables", [])
    if failed_tables:
        st.markdown("### Failed Tables")
        st.dataframe(_list_rows(failed_tables, "table"), use_container_width=True, hide_index=True)

    residual_manual_review = summary_obj.get("residual_manual_review", [])
    if residual_manual_review:
        st.markdown("### Residual Manual Review")
        st.dataframe(
            _list_rows(residual_manual_review, "table"), use_container_width=True, hide_index=True
        )

    next_actions = summary_obj.get("next_actions", [])
    if next_actions:
        st.markdown("### Next Actions")
        st.dataframe(
            _list_rows(next_actions, "next_action"), use_container_width=True, hide_index=True
        )

    notes = summary_obj.get("notes", [])
    if notes:
        st.markdown("### Notes")
        st.dataframe(_list_rows(notes, "note"), use_container_width=True, hide_index=True)

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
    source_manifest_path: str,
    mode: MigrationMode,
    approved_by: str,
    allow_destructive: bool,
    include_tables: list[str],
    exclude_tables: list[str],
    approved_manual_review_items: list[str],
    table_strategies: dict[str, Any],
    notes: str,
    out_path: str,
) -> str:
    approval = build_approval_document(
        plan_path=plan_path,
        summary_path=summary_path,
        source_manifest_path=source_manifest_path,
        approved_mode=mode,
        approved_by=approved_by,
        allow_destructive=allow_destructive,
        include_tables=include_tables or None,
        exclude_tables=exclude_tables or None,
        approved_manual_review_items=approved_manual_review_items or None,
        table_strategies=table_strategies or None,
        notes=notes or None,
    )
    write_json(out_path, approval)
    return out_path


def _run(config_path: str, plan_path: str, approval_path: str, state_path: str, fresh: bool) -> str:
    bundle = build_approved_execution_bundle(
        approval_path=approval_path,
        plan_path=plan_path or None,
    )
    load_env(".env")
    cfg = load_config(config_path)

    state_file = Path(state_path)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    if fresh and state_file.exists():
        state_file.unlink()

    if not bundle.filtered_plan.get("steps"):
        included = set(bundle.approval.included_tables)
        excluded = set(bundle.approval.excluded_tables)
        overlap = sorted(included.intersection(excluded))
        if overlap:
            raise RuntimeError(
                "Approval filters removed every plan step because the same tables are both "
                f"included and excluded: {', '.join(overlap)}. Recreate the approval with "
                "those tables removed from Excluded tables."
            )
        raise RuntimeError(
            "Approval filters removed every plan step. Recreate the approval with at least "
            "one copy or metadata-sync table included, or use Analyze/Review only for plan-only mode."
        )
    execute(cfg=cfg, bundle=bundle, state_path=str(state_file))
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
        st.caption("Active config file")
        st.code(st.session_state["active_config_path"], language="text")
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

    workflow_tabs = [
        "Config",
        "Analyze",
        "Review",
        "Approve",
        "Run",
        "Post Summary",
        "Artifacts",
    ]
    selected_tab = st.radio(
        "Workflow step",
        options=workflow_tabs,
        horizontal=True,
        label_visibility="collapsed",
        key="active_workflow_tab",
    )
    config_path = st.session_state["active_config_path"]

    if selected_tab == "Config":
        st.subheader("Config")
        st.write(
            "Create or select the connection config used by Analyze, Run, CLI commands, and MCP tools."
        )

        st.markdown("### Use Existing Config")
        existing_config_path = st.text_input(
            "Existing Config File",
            value=st.session_state["active_config_path"],
            key="config_existing_path",
        )
        if st.button("Use Existing Config", key="config_use_existing_button"):
            if Path(existing_config_path).exists():
                st.session_state["active_config_path"] = existing_config_path
                st.session_state["flash_message"] = f"Active config set to {existing_config_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            else:
                st.error(f"Config file does not exist: `{existing_config_path}`")

        st.markdown("### Build Connection Config")
        st.caption(
            "PostgreSQL is supported today. The database-type dropdown is the adapter seam "
            "for adding more engines later without changing the workflow."
        )

        source_col, target_col = st.columns(2)
        db_type_options = list(DATABASE_TYPES)
        with source_col:
            st.markdown("#### Source Database")
            source_db_type_label = st.selectbox(
                "Source Database Type",
                options=db_type_options,
                index=0,
                key="config_source_db_type",
            )
            source_host = st.text_input("Source Host", value="localhost", key="config_source_host")
            source_port = st.number_input(
                "Source Port",
                min_value=1,
                max_value=65535,
                value=5433,
                key="config_source_port",
            )
            source_database = st.text_input(
                "Source Database Name", value="sourcedb", key="config_source_database"
            )
            source_user = st.text_input("Source User", value="source", key="config_source_user")
            source_password = st.text_input(
                "Source Password",
                value="source",
                type="password",
                key="config_source_password",
            )

        with target_col:
            st.markdown("#### Target Database")
            target_db_type_label = st.selectbox(
                "Target Database Type",
                options=db_type_options,
                index=0,
                key="config_target_db_type",
            )
            target_host = st.text_input("Target Host", value="localhost", key="config_target_host")
            target_port = st.number_input(
                "Target Port",
                min_value=1,
                max_value=65535,
                value=5434,
                key="config_target_port",
            )
            target_database = st.text_input(
                "Target Database Name", value="targetdb", key="config_target_database"
            )
            target_user = st.text_input("Target User", value="target", key="config_target_user")
            target_password = st.text_input(
                "Target Password",
                value="target",
                type="password",
                key="config_target_password",
            )

            target_preview = _database_config(
                db_type=DATABASE_TYPES[target_db_type_label],
                host=target_host,
                port=int(target_port),
                database=target_database,
                user=target_user,
                password=target_password,
            )
            st.markdown("#### Demo Reset")
            st.warning(
                "Demo only: this drops every non-system schema in the target database and "
                "recreates `public`. Use only with the local Docker target."
            )
            reset_confirmation = st.text_input(
                "Type RESET TARGET to enable reset",
                value="",
                key="config_reset_target_confirmation",
            )
            reset_enabled = _is_demo_target(target_preview) and reset_confirmation == "RESET TARGET"
            if not _is_demo_target(target_preview):
                st.caption(
                    "Reset is disabled unless target is localhost:5434/targetdb as user target."
                )
            if st.button(
                "Reset Target Demo DB",
                key="config_reset_target_button",
                disabled=not reset_enabled,
            ):
                try:
                    reset_message, stdout, stderr = _capture(_reset_target_demo_db, target_preview)
                    st.session_state["flash_message"] = reset_message
                    st.session_state["flash_kind"] = "success"
                    st.rerun()
                except Exception as exc:
                    st.exception(exc)

        st.markdown("### Migration Defaults")
        c1, c2, c3 = st.columns(3)
        with c1:
            config_batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                value=20000,
                step=1000,
                key="config_batch_size",
            )
            config_max_partitions = st.number_input(
                "Max Partitions",
                min_value=1,
                value=16,
                step=1,
                key="config_max_partitions",
            )
        with c2:
            st.info(
                "Generated configs are non-destructive. Destructive authority is selected only "
                "in the approval step with a per-table truncate_reload strategy."
            )
            config_allow_destructive = False
            config_truncate_first = False
        with c3:
            config_sample_hash = st.checkbox(
                "Enable Sample Hash Verification",
                value=False,
                key="config_sample_hash",
            )
            config_maintenance = st.selectbox(
                "Post-Migration Maintenance",
                options=["auto", "analyze_only", "vacuum_analyze", "off"],
                index=0,
                key="config_maintenance",
            )

        include_schemas_text = st.text_input(
            "Include Schemas (comma-separated, blank means all non-system schemas)",
            value="",
            key="config_include_schemas",
        )
        exclude_schemas_text = st.text_input(
            "Exclude Schemas (comma-separated)",
            value="pg_catalog, information_schema",
            key="config_exclude_schemas",
        )
        config_output_path = st.text_input(
            "Generated Config Output File",
            value=st.session_state["config_output_path"],
            key="config_output_path_input",
        )

        if st.button("Generate Config", type="primary", key="config_generate_button"):
            required_values = {
                "Source Host": source_host,
                "Source Database Name": source_database,
                "Source User": source_user,
                "Target Host": target_host,
                "Target Database Name": target_database,
                "Target User": target_user,
            }
            missing = [label for label, value in required_values.items() if not value.strip()]
            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
                st.stop()

            browser_config = _build_browser_config(
                source=_database_config_with_env_password(
                    db_type=DATABASE_TYPES[source_db_type_label],
                    host=source_host,
                    port=int(source_port),
                    database=source_database,
                    user=source_user,
                    password=source_password,
                    password_env="SRC_PASSWORD",
                ),
                target=_database_config_with_env_password(
                    db_type=DATABASE_TYPES[target_db_type_label],
                    host=target_host,
                    port=int(target_port),
                    database=target_database,
                    user=target_user,
                    password=target_password,
                    password_env="DST_PASSWORD",
                ),
                planner=planner,
                max_partitions=int(config_max_partitions),
                batch_size=int(config_batch_size),
                include_schemas=_split_csv(include_schemas_text),
                exclude_schemas=_split_csv(exclude_schemas_text),
                allow_destructive=config_allow_destructive,
                truncate_first=config_truncate_first,
                sample_hash=config_sample_hash,
                maintenance=config_maintenance,
            )
            created_path = _write_browser_config(browser_config, config_output_path)
            st.session_state["active_config_path"] = created_path
            st.session_state["config_output_path"] = created_path
            st.session_state["flash_message"] = (
                f"Config written to {created_path} with password environment placeholders."
            )
            st.session_state["flash_kind"] = "success"
            st.rerun()

        if Path(st.session_state["active_config_path"]).exists():
            with st.expander("Active Config Preview", expanded=False):
                try:
                    preview = load_config(st.session_state["active_config_path"])
                    redacted_preview = {
                        **preview,
                        "source": {**preview.get("source", {}), "password": "***"},
                        "target": {**preview.get("target", {}), "password": "***"},
                    }
                    st.json(redacted_preview)
                except Exception as exc:
                    st.warning(f"Could not preview config yet: {exc}")

    if selected_tab == "Analyze":
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
                st.session_state["last_source_manifest_path"] = artifacts["source_manifest"]
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

    if selected_tab == "Review":
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

    if selected_tab == "Approve":
        st.subheader("Approve")
        plan_path = st.text_input(
            "Plan path", value=st.session_state["last_plan_path"], key="approve_plan_path"
        )
        summary_path = st.text_input(
            "Summary path", value=st.session_state["last_summary_path"], key="approve_summary_path"
        )
        source_manifest_path = st.text_input(
            "Source manifest path",
            value=st.session_state["last_source_manifest_path"],
            key="approve_source_manifest_path",
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
        _read_expected_artifact(
            source_manifest_path,
            "manifest",
            "Approve Source manifest path",
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
        approval_scope_key = _safe_key(summary_path or "no-summary")

        include_tables = st.multiselect(
            "Included tables",
            options=table_options,
            default=default_include,
            key=f"approve_include_tables_{approval_scope_key}",
        )
        exclude_tables = st.multiselect(
            "Excluded tables",
            options=table_options,
            default=[],
            key=f"approve_exclude_tables_{approval_scope_key}",
        )
        approve_manual_review_items = st.multiselect(
            "Approved manual-review items",
            options=manual_review_options,
            default=[],
            key=f"approve_manual_review_items_{approval_scope_key}",
        )
        allow_destructive = st.checkbox(
            "Allow destructive actions", value=False, key="approve_allow_destructive"
        )
        table_strategies: dict[str, Any] = {}
        recommendation_map = {
            f"{item.get('schema')}.{item.get('table')}": item
            for item in (summary_obj or {}).get("table_recommendations", [])
        }
        strategy_tables = [table for table in include_tables if table not in set(exclude_tables)]
        if strategy_tables:
            st.markdown("### Per-Table Load Strategy")
            st.caption(
                "`append_only` does not truncate. `upsert` requires a validated primary key. "
                "`truncate_reload` is only available when destructive actions are allowed."
            )
            for table_key in strategy_tables:
                recommendation = recommendation_map.get(table_key)
                if not recommendation:
                    continue
                action = recommendation.get("action")
                if action != "copy":
                    st.caption(f"{table_key}: action={action}; no data load strategy required.")
                    continue

                c_table, c_strategy, c_key = st.columns([3, 2, 3])
                options = _strategy_options(recommendation, allow_destructive)
                default_strategy = _default_load_strategy(recommendation, allow_destructive)
                default_index = (
                    options.index(default_strategy) if default_strategy in options else 0
                )
                with c_table:
                    st.write(table_key)
                    st.caption(
                        f"risk={recommendation.get('risk_level')} "
                        f"rows={recommendation.get('estimated_rows', '-')}"
                    )
                with c_strategy:
                    strategy = st.selectbox(
                        "Strategy",
                        options=options,
                        index=default_index,
                        key=(
                            f"approve_strategy_{approval_scope_key}_"
                            f"{_safe_key(table_key)}_{allow_destructive}"
                        ),
                    )
                with c_key:
                    conflict_key = recommendation.get("conflict_key") or []
                    key_text = ", ".join(conflict_key) if conflict_key else "-"
                    st.write(f"Key: {key_text}")
                    st.caption(f"key_readiness={recommendation.get('key_readiness')}")

                table_strategies[table_key] = _table_strategy_payload(
                    table_key, strategy, recommendation
                )
        notes = st.text_area("Approval notes", value="", key="approve_notes")
        overlapping_tables = _approval_overlap(include_tables, exclude_tables)
        if overlapping_tables:
            st.error(
                "Tables cannot be both included and excluded. Remove these from Excluded "
                f"tables before creating approval: {', '.join(overlapping_tables)}"
            )
        if summary_obj and not overlapping_tables:
            approved_candidates = _approved_table_candidates(
                summary_obj=summary_obj,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                approved_manual_review_items=approve_manual_review_items,
                allow_destructive=allow_destructive,
                approved_mode=mode,
            )
            approved_candidates = [
                table
                for table in approved_candidates
                if table_strategies.get(table, {}).get("strategy") != "skip"
            ]
            st.caption(
                f"Executable table candidates after approval filters: {len(approved_candidates)}"
            )

        if st.button("Create Approval", key="approve_create_button"):
            inputs_ok = all(
                [
                    _validate_artifact_path(plan_path, "migration_plan", "Approve Plan path"),
                    _validate_artifact_path(
                        summary_path,
                        "pre_migration_summary",
                        "Approve Summary path",
                    ),
                    _validate_artifact_path(
                        source_manifest_path,
                        "manifest",
                        "Approve Source manifest path",
                    ),
                ]
            )
            if not inputs_ok:
                st.stop()
            if overlapping_tables:
                st.stop()
            approved_candidates = _approved_table_candidates(
                summary_obj=summary_obj or {},
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                approved_manual_review_items=approve_manual_review_items,
                allow_destructive=allow_destructive,
                approved_mode=mode,
            )
            approved_candidates = [
                table
                for table in approved_candidates
                if table_strategies.get(table, {}).get("strategy") != "skip"
            ]
            if not approved_candidates and mode != "plan_only":
                st.error(
                    "Approval would create an executable plan with 0 tables. Include at least "
                    "one copy or metadata-sync table before creating approval."
                )
                st.stop()
            try:
                created_path, stdout, stderr = _capture(
                    _approve,
                    plan_path,
                    summary_path,
                    source_manifest_path,
                    mode,
                    approved_by,
                    allow_destructive,
                    include_tables,
                    exclude_tables,
                    approve_manual_review_items,
                    table_strategies,
                    notes,
                    approval_path,
                )
                st.session_state["last_approval_path"] = created_path
                st.session_state["flash_message"] = f"Approval written to {created_path}"
                st.session_state["flash_kind"] = "success"
                st.rerun()
            except Exception as exc:
                st.exception(exc)

    if selected_tab == "Run":
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

    if selected_tab == "Post Summary":
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

    if selected_tab == "Artifacts":
        st.subheader("Artifacts")
        artifact_rows = [
            {
                "artifact": "Active Config File",
                "path": st.session_state["active_config_path"] or "-",
                "exists": _artifact_exists(st.session_state["active_config_path"]),
            },
            {
                "artifact": "Analysis Directory",
                "path": st.session_state["analysis_dir"],
                "exists": Path(st.session_state["analysis_dir"]).exists(),
            },
            {
                "artifact": "Migration Plan File",
                "path": st.session_state["last_plan_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_plan_path"]),
            },
            {
                "artifact": "Pre-Migration Summary File",
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
                "artifact": "Approval File",
                "path": st.session_state["last_approval_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_approval_path"]),
            },
            {
                "artifact": "Run State File",
                "path": st.session_state["last_state_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_state_path"]),
            },
            {
                "artifact": "Post-Migration Summary Output File",
                "path": st.session_state["last_post_summary_path"] or "-",
                "exists": _artifact_exists(st.session_state["last_post_summary_path"]),
            },
            {
                "artifact": "Failure Analysis File",
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

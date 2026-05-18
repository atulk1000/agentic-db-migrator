from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any


def _table_key(schema: str | None, table: str | None) -> str | None:
    if not schema or not table:
        return None
    return f"{schema}.{table}"


def _copy_tables(plan: dict[str, Any]) -> set[str]:
    return {
        key
        for step in plan.get("steps", [])
        if step.get("op") == "copy_table"
        for key in [_table_key(step.get("schema"), step.get("table"))]
        if key
    }


def _verified_tables(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        key: step.get("validate", {})
        for step in plan.get("steps", [])
        if step.get("op") == "verify_table"
        for key in [_table_key(step.get("schema"), step.get("table"))]
        if key
    }


def _recommendation_map(pre_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        f"{item.get('schema')}.{item.get('table')}": item
        for item in pre_summary.get("table_recommendations", [])
    }


def build_planner_critique(
    plan: dict[str, Any],
    pre_summary: dict[str, Any],
    manifest_diff: dict[str, Any],
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    copy_tables = _copy_tables(plan)
    verified = _verified_tables(plan)
    recommendations = _recommendation_map(pre_summary)

    if not plan.get("steps"):
        findings.append(
            {
                "severity": "high",
                "category": "empty_plan",
                "message": "Plan contains no executable steps.",
                "recommendation": "Regenerate the plan before approval.",
            }
        )

    for table in sorted(copy_tables - set(verified)):
        findings.append(
            {
                "severity": "high",
                "category": "missing_verification",
                "table": table,
                "message": "Copied table has no verify_table step.",
                "recommendation": "Add rowcount verification before approval.",
            }
        )

    for table, recommendation in recommendations.items():
        if recommendation.get("risk_level") == "high" and table in verified:
            validate = verified[table]
            if not validate.get("sample_hash"):
                findings.append(
                    {
                        "severity": "medium",
                        "category": "verification_depth",
                        "table": table,
                        "message": "High-risk table is not using sample-hash verification.",
                        "recommendation": "Use rowcount_and_sample_hash for this table.",
                    }
                )

    for table in pre_summary.get("manual_review_required", []):
        findings.append(
            {
                "severity": "medium",
                "category": "manual_review",
                "table": table,
                "message": "Table requires explicit operator review before execution.",
                "recommendation": "Clarify desired action before creating approval.",
            }
        )

    for warning in manifest_diff.get("warnings", []):
        findings.append(
            {
                "severity": "low",
                "category": "preflight_warning",
                "message": warning,
                "recommendation": "Review before approving migration scope.",
            }
        )

    severity_weight = {"high": 25, "medium": 10, "low": 3}
    score = max(
        0,
        100 - sum(severity_weight.get(item["severity"], 0) for item in findings),
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agent": "planner_critic",
        "score": score,
        "status": "needs_review" if findings else "ready_for_approval",
        "finding_count": len(findings),
        "findings": findings,
    }


def build_clarification_questions(
    pre_summary: dict[str, Any],
    manifest_diff: dict[str, Any],
) -> dict[str, Any]:
    questions: list[dict[str, Any]] = []

    for table in pre_summary.get("manual_review_required", []):
        questions.append(
            {
                "id": f"manual_review:{table}",
                "table": table,
                "question": f"How should `{table}` be handled?",
                "choices": ["skip", "metadata_sync_only", "full_refresh_after_review"],
                "recommended": "skip",
                "reason": "The planner routed this table to manual review.",
                "required_before_run": True,
            }
        )

    for item in manifest_diff.get("tables", []):
        table = f"{item.get('schema')}.{item.get('table')}"
        if item.get("status") == "missing_in_source":
            questions.append(
                {
                    "id": f"target_only:{table}",
                    "table": table,
                    "question": f"`{table}` exists only in target. Should it be preserved?",
                    "choices": ["preserve_target_table", "drop_after_manual_approval"],
                    "recommended": "preserve_target_table",
                    "reason": "Target-only objects can contain QA-specific data.",
                    "required_before_run": True,
                }
            )
        if item.get("structural_drift"):
            questions.append(
                {
                    "id": f"structural_drift:{table}",
                    "table": table,
                    "question": f"`{table}` has structural drift. What is the desired policy?",
                    "choices": ["skip", "recreate_target_after_review", "metadata_only"],
                    "recommended": "skip",
                    "reason": ", ".join(item.get("structural_drift", [])),
                    "required_before_run": True,
                }
            )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agent": "clarification_agent",
        "question_count": len(questions),
        "questions": questions,
    }


def render_plan_rationale(
    plan: dict[str, Any],
    pre_summary: dict[str, Any],
    critique: dict[str, Any],
) -> str:
    op_counts = Counter(step.get("op") for step in plan.get("steps", []))
    overview = pre_summary.get("overview", {})
    lines = [
        "# Planner Rationale",
        "",
        f"- Planner: `{overview.get('planner', plan.get('planner', 'unknown'))}`",
        f"- Mode: `{overview.get('mode', 'unknown')}`",
        f"- Critic status: `{critique.get('status')}` with score `{critique.get('score')}`",
        "",
        "## Step Mix",
    ]
    lines.extend(f"- `{op}`: {count}" for op, count in sorted(op_counts.items()))
    lines.extend(["", "## Table Decisions"])
    for item in pre_summary.get("table_recommendations", []):
        table = f"{item.get('schema')}.{item.get('table')}"
        lines.append(
            f"- `{table}`: action=`{item.get('action')}`, strategy=`{item.get('transfer_strategy')}`, "
            f"verification=`{item.get('verification_depth')}`, risk=`{item.get('risk_level')}`. "
            f"{item.get('rationale', '')}"
        )
    if critique.get("findings"):
        lines.extend(["", "## Critic Findings"])
        for finding in critique["findings"]:
            table = f" `{finding['table']}`" if finding.get("table") else ""
            lines.append(
                f"- {finding.get('severity', 'info').upper()}{table}: "
                f"{finding.get('message')} Recommendation: {finding.get('recommendation')}"
            )
    return "\n".join(lines) + "\n"


def build_failure_analysis(
    plan: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any] | None = None,
    pre_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    completed = state.get("completed", {})
    failed_steps = [
        {"step_id": step_id, "payload": payload}
        for step_id, payload in completed.items()
        if not payload.get("ok", False)
    ]
    failed_tables = [
        item
        for item in (report or {}).get("results", [])
        if not item.get("ok", False)
    ]
    plan_step_map = {step.get("id"): step for step in plan.get("steps", [])}
    findings: list[dict[str, Any]] = []

    for failure in failed_steps:
        step = plan_step_map.get(failure["step_id"], {})
        findings.append(
            {
                "severity": "high",
                "category": "execution_failure",
                "step_id": failure["step_id"],
                "op": step.get("op"),
                "table": _table_key(step.get("schema"), step.get("table")),
                "message": str(failure["payload"].get("error") or failure["payload"]),
                "recommended_next_action": "Inspect the failing step, fix root cause, then rerun with the same state file or a fresh state after cleanup.",
            }
        )

    for item in failed_tables:
        findings.append(
            {
                "severity": "high",
                "category": "verification_failure",
                "table": _table_key(item.get("schema"), item.get("table")),
                "message": "Source and target verification did not match.",
                "recommended_next_action": "Compare row counts/sample hashes and rerun the affected table after resolving drift.",
                "details": item,
            }
        )

    if pre_summary and pre_summary.get("manual_review_required"):
        findings.append(
            {
                "severity": "medium",
                "category": "residual_manual_review",
                "message": "Manual-review items remain from pre-migration planning.",
                "recommended_next_action": "Resolve or explicitly approve manual-review items before cutover.",
                "tables": pre_summary.get("manual_review_required", []),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agent": "post_run_failure_analyst",
        "status": "needs_attention" if findings else "clean",
        "finding_count": len(findings),
        "findings": findings,
    }


def render_failure_analysis(analysis: dict[str, Any]) -> str:
    lines = [
        "# Post-Run Failure Analysis",
        "",
        f"- Status: `{analysis.get('status')}`",
        f"- Findings: `{analysis.get('finding_count', 0)}`",
        "",
    ]
    if not analysis.get("findings"):
        lines.append("No execution or verification failures were detected.")
        return "\n".join(lines) + "\n"

    lines.append("## Findings")
    for finding in analysis.get("findings", []):
        target = f" `{finding['table']}`" if finding.get("table") else ""
        lines.append(
            f"- {finding.get('severity', 'info').upper()}{target}: "
            f"{finding.get('message')} Next: {finding.get('recommended_next_action')}"
        )
    return "\n".join(lines) + "\n"

from __future__ import annotations

from amo.core.agentic import (
    build_clarification_questions,
    build_failure_analysis,
    build_planner_critique,
    render_failure_analysis,
    render_plan_rationale,
)


def test_agentic_artifacts_flag_missing_verification_and_manual_review():
    plan = {
        "planner": "heuristic_v2",
        "steps": [
            {"id": "s1", "op": "copy_table", "schema": "public", "table": "users"},
        ],
    }
    pre_summary = {
        "overview": {"planner": "heuristic_v2", "mode": "safe_sync"},
        "manual_review_required": ["public.orders"],
        "table_recommendations": [
            {
                "schema": "public",
                "table": "users",
                "action": "copy",
                "transfer_strategy": "full_copy",
                "verification_depth": "rowcount",
                "risk_level": "low",
                "rationale": "small table",
            },
            {
                "schema": "public",
                "table": "orders",
                "action": "manual_review",
                "transfer_strategy": "full_copy",
                "verification_depth": "rowcount_and_sample_hash",
                "risk_level": "high",
                "rationale": "structural drift",
            },
        ],
    }
    manifest_diff = {
        "warnings": ["Target contains one extra table."],
        "tables": [
            {
                "schema": "public",
                "table": "orders",
                "status": "metadata_diff",
                "structural_drift": ["columns"],
            }
        ],
    }

    critique = build_planner_critique(plan, pre_summary, manifest_diff)
    questions = build_clarification_questions(pre_summary, manifest_diff)
    rationale = render_plan_rationale(plan, pre_summary, critique)

    assert critique["status"] == "needs_review"
    assert any(item["category"] == "missing_verification" for item in critique["findings"])
    assert questions["question_count"] >= 1
    assert "Planner Rationale" in rationale


def test_failure_analysis_reports_verification_failures():
    analysis = build_failure_analysis(
        plan={"steps": [{"id": "s1", "op": "copy_table", "schema": "public", "table": "users"}]},
        state={"completed": {"s1": {"ok": True}}},
        report={
            "ok": False,
            "tables_checked": 1,
            "results": [{"schema": "public", "table": "users", "ok": False}],
        },
    )
    rendered = render_failure_analysis(analysis)

    assert analysis["status"] == "needs_attention"
    assert "verification_failure" in str(analysis["findings"])
    assert "Post-Run Failure Analysis" in rendered

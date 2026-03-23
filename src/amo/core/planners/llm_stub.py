from __future__ import annotations

from amo.core.planners.heuristic_planner import generate_plan as generate_heuristic_plan
from amo.core.planners.models import validate_plan_document


def generate_fallback_plan(manifest_path: str, planner_name: str, reason: str | None = None) -> dict:
    plan = generate_heuristic_plan(manifest_path=manifest_path)
    plan["planner"] = planner_name
    plan["planner_metadata"] = {
        "mode": "heuristic_fallback",
        "reason": reason or "LLM adapter is not configured yet.",
    }
    return validate_plan_document(plan)

from __future__ import annotations

from amo.core.planners.llm_stub import generate_fallback_plan


def generate_plan(manifest_path: str) -> dict:
    return generate_fallback_plan(manifest_path=manifest_path, planner_name="ollama_stub")

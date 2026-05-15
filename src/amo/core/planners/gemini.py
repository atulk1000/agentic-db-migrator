from __future__ import annotations

import json
import os
from pathlib import Path

from amo.core.planners.llm_common import parse_normalize_and_validate_llm_plan
from amo.core.planners.llm_stub import generate_fallback_plan
from amo.core.planners.models import MigrationManifest
from amo.core.planners.prompting import (
    PROMPT_VERSION,
    build_migration_prompt,
    load_optional_json,
)

DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"


def _request_gemini_plan(api_key: str, model: str, prompt: str) -> str:
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
        ),
    )
    return (response.text or "").strip()


def generate_plan(
    manifest_path: str,
    context_path: str | None = None,
    objective: str | None = None,
) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY")
    model = os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)

    if not api_key:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="gemini_stub",
            reason="GEMINI_API_KEY is not set.",
        )

    manifest = MigrationManifest.model_validate(
        json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    )
    planning_context = load_optional_json(context_path)
    prompt = build_migration_prompt(
        manifest, planning_context=planning_context, objective=objective
    )

    try:
        raw_text = _request_gemini_plan(api_key=api_key, model=model, prompt=prompt)
        if not raw_text:
            raise ValueError("Gemini returned an empty response.")

        return parse_normalize_and_validate_llm_plan(
            raw_plan=raw_text,
            manifest=manifest,
            planner_name="gemini",
            planner_metadata={
                "mode": "live_api",
                "provider": "gemini",
                "model": model,
                "prompt_version": PROMPT_VERSION,
                "used_context": bool(planning_context),
            },
        )
    except Exception as exc:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="gemini_stub",
            reason=f"Gemini API call failed or produced an invalid plan: {exc}",
        )

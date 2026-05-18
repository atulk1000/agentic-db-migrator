from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from amo.core.planners.llm_common import parse_normalize_and_validate_llm_plan
from amo.core.planners.llm_stub import generate_fallback_plan
from amo.core.planners.models import MigrationManifest
from amo.core.planners.prompting import (
    PROMPT_VERSION,
    build_migration_prompt,
    load_optional_json,
)

DEFAULT_OPENAI_MODEL = "gpt-4.1-mini"


def _plan_json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["version", "planner", "strategy", "source", "steps"],
        "properties": {
            "version": {"type": "string"},
            "generated_at": {"type": ["string", "null"]},
            "planner": {"type": "string"},
            "strategy": {"type": "string"},
            "source": {"type": "object", "additionalProperties": True},
            "planner_metadata": {"type": "object", "additionalProperties": True},
            "steps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": True,
                    "required": ["id", "op", "schema"],
                    "properties": {
                        "id": {"type": "string"},
                        "op": {"type": "string"},
                        "schema": {"type": "string"},
                        "table": {"type": ["string", "null"]},
                        "validate": {"type": "object", "additionalProperties": True},
                    },
                },
            },
        },
    }


def _extract_response_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if output_text:
        return str(output_text).strip()

    for item in getattr(response, "output", []) or []:
        for content in getattr(item, "content", []) or []:
            text = getattr(content, "text", None)
            if text:
                return str(text).strip()
            if isinstance(content, dict) and content.get("text"):
                return str(content["text"]).strip()

    raise ValueError("OpenAI returned no text output.")


def _request_openai_plan(api_key: str, model: str, prompt: str) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    system_message = (
        "You are a PostgreSQL migration planner. Return only JSON that matches "
        "the requested migration plan contract. Never emit SQL for execution."
    )

    try:
        response = client.responses.create(
            model=model,
            input=[
                {"role": "developer", "content": system_message},
                {"role": "user", "content": prompt},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "migration_plan",
                    "schema": _plan_json_schema(),
                    "strict": False,
                }
            },
            temperature=0.1,
        )
        return _extract_response_text(response)
    except Exception as responses_exc:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )
            return (response.choices[0].message.content or "").strip()
        except Exception as chat_exc:
            raise RuntimeError(
                "OpenAI Responses and Chat Completions planner calls both failed: "
                f"responses={responses_exc}; chat={chat_exc}"
            ) from chat_exc


def _request_openai_repair(
    api_key: str,
    model: str,
    prompt: str,
    raw_plan: str | dict[str, Any],
    error: str,
) -> str:
    repair_prompt = (
        f"{prompt}\n\n"
        "The previous JSON response failed validation. Repair it once.\n"
        "Return ONLY a corrected JSON object. Do not add markdown.\n\n"
        f"Validation error:\n{error}\n\n"
        f"Invalid response:\n{raw_plan}"
    )
    return _request_openai_plan(api_key=api_key, model=model, prompt=repair_prompt)


def generate_plan(
    manifest_path: str,
    context_path: str | None = None,
    objective: str | None = None,
) -> dict:
    api_key = os.environ.get("OPENAI_API_KEY")
    model = os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)

    if not api_key:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="openai_stub",
            reason="OPENAI_API_KEY is not set.",
        )

    manifest = MigrationManifest.model_validate(
        json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    )
    planning_context = load_optional_json(context_path)
    prompt = build_migration_prompt(
        manifest, planning_context=planning_context, objective=objective
    )

    try:
        raw_text = _request_openai_plan(api_key=api_key, model=model, prompt=prompt)
        if not raw_text:
            raise ValueError("OpenAI returned an empty response.")

        return parse_normalize_and_validate_llm_plan(
            raw_plan=raw_text,
            manifest=manifest,
            planner_name="openai",
            planner_metadata={
                "mode": "live_api",
                "provider": "openai",
                "model": model,
                "prompt_version": PROMPT_VERSION,
                "used_context": bool(planning_context),
            },
            repair_callback=lambda raw_plan, error: _request_openai_repair(
                api_key=api_key,
                model=model,
                prompt=prompt,
                raw_plan=raw_plan,
                error=error,
            ),
        )
    except Exception as exc:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="openai_stub",
            reason=f"OpenAI API call failed or produced an invalid plan: {exc}",
        )

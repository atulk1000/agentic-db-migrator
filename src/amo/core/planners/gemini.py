from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from amo.core.planners.llm_stub import generate_fallback_plan
from amo.core.planners.llm_common import parse_normalize_and_validate_llm_plan, root_tables
from amo.core.planners.models import MigrationManifest


DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
def _build_prompt(manifest: MigrationManifest) -> str:
    allowed_tables = ", ".join(sorted(f"{table.schema_name}.{table.table}" for table in root_tables(manifest)))
    return (
        "You are planning a PostgreSQL migration.\n"
        "Return ONLY JSON. No markdown. No explanations.\n"
        "The plan MUST use exactly these top-level fields: "
        "version, generated_at, planner, strategy, source, planner_metadata, steps.\n"
        "Each step MUST use only these fields: "
        "id, op, schema, table, estimated_rows, estimated_bytes, has_geometry, primary_key, validate, indexes, fks, matviews, udfs.\n"
        "Do NOT use name, mode, fk_name, notes, commentary, or any other extra keys.\n"
        "Allowed op values: ensure_schema, create_udfs, ensure_table, copy_table, sync_sequences, "
        "create_indexes, add_fks, create_matviews, create_mv_indexes, verify_table.\n"
        "Rules by op:\n"
        "- ensure_schema: schema required, table must be null.\n"
        "- create_udfs: schema required, udfs non-empty, table must be null.\n"
        "- ensure_table: schema and table required.\n"
        "- copy_table: schema and table required.\n"
        "- sync_sequences: schema and table required. table is always a TABLE name, never a sequence name.\n"
        "- create_indexes: schema and table required, indexes non-empty.\n"
        "- add_fks: schema required, fks non-empty, table must be null.\n"
        "- create_matviews: schema required, matviews non-empty, table must be null.\n"
        "- create_mv_indexes: schema required, indexes non-empty, table must be null.\n"
        "- verify_table: schema and table required, validate must contain rowcount:boolean and may contain sample_hash:boolean and sample_rows:int.\n"
        "Allowed validate values:\n"
        '- {"rowcount": true, "sample_hash": false, "sample_rows": 50}\n'
        '- {"rowcount": true, "sample_hash": true, "sample_rows": 50}\n'
        "Use only schema.table values from this list:\n"
        f"{allowed_tables}\n"
        "Prefer the order: ensure_schema, create_udfs, ensure_table, copy_table, sync_sequences, create_indexes, verify_table, add_fks, create_matviews, create_mv_indexes.\n"
        "For small tables, set sample_hash true. For larger tables, set sample_hash false.\n"
        "If a step is not valid under these rules, omit it instead of inventing unsupported fields.\n"
        "Manifest JSON:\n"
        f"{manifest.model_dump_json(by_alias=True, exclude_none=True)}"
    )


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


def generate_plan(manifest_path: str) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY")
    model = os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)

    if not api_key:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="gemini_stub",
            reason="GEMINI_API_KEY is not set.",
        )

    manifest = MigrationManifest.model_validate(json.loads(Path(manifest_path).read_text()))
    prompt = _build_prompt(manifest)

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
            },
        )
    except Exception as exc:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="gemini_stub",
            reason=f"Gemini API call failed or produced an invalid plan: {exc}",
        )

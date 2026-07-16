from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path
from typing import Any

from amo.core.planners.llm_common import root_tables
from amo.core.planners.models import MigrationManifest

PROMPT_VERSION = "migration_planner_v1"


def load_optional_json(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def build_migration_prompt(
    manifest: MigrationManifest,
    planning_context: dict[str, Any] | None = None,
    objective: str | None = None,
) -> str:
    allowed_tables = ", ".join(
        sorted(f"{table.schema_name}.{table.table}" for table in root_tables(manifest))
    )
    template = (
        files("amo.core.planners")
        .joinpath("prompts", f"{PROMPT_VERSION}.md")
        .read_text(encoding="utf-8")
    )
    context_json = json.dumps(planning_context or {}, indent=2, sort_keys=True)
    return template.format(
        allowed_tables=allowed_tables,
        objective=objective or "Build a safe PostgreSQL migration plan for user review.",
        manifest_json=manifest.model_dump_json(by_alias=True, exclude_none=True),
        planning_context_json=context_json,
    )

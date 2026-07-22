from __future__ import annotations

import json
import os
import tempfile
from importlib.metadata import distribution, entry_points
from pathlib import Path

from amo.cli import app
from amo.core.planners.gemini import generate_plan as generate_gemini_plan
from amo.core.planners.models import MigrationManifest
from amo.core.planners.openai import generate_plan as generate_openai_plan
from amo.core.planners.prompting import build_migration_prompt


def main() -> None:
    manifest = {
        "version": "v2",
        "source": {"host": "source-db", "port": 5432, "database": "sourcedb"},
        "include_schemas": ["public"],
        "tables": [],
        "matviews": [],
        "matview_indexes": [],
        "udfs": [],
        "schema_grants": {},
        "errors": [],
    }
    prompt = build_migration_prompt(MigrationManifest.model_validate(manifest))
    assert "PostgreSQL" in prompt

    with tempfile.TemporaryDirectory() as temp_dir:
        manifest_path = Path(temp_dir) / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        os.environ.pop("OPENAI_API_KEY", None)
        os.environ.pop("GEMINI_API_KEY", None)
        assert generate_openai_plan(str(manifest_path))["planner"] == "openai_stub"
        assert generate_gemini_plan(str(manifest_path))["planner"] == "gemini_stub"

    console_scripts = {entry.name for entry in entry_points(group="console_scripts")}
    assert {"amo", "amo-mcp"}.issubset(console_scripts)
    assert app is not None

    installed_files = [
        str(item).replace("\\", "/") for item in distribution("agentic-db-migrator").files or []
    ]
    assert any(
        path.endswith("amo/core/planners/prompts/migration_planner_v1.md")
        for path in installed_files
    )
    forbidden_names = {".env", "config.yaml", "state.json", "report.json"}
    assert not any(Path(path).name in forbidden_names for path in installed_files)
    assert not any("screenshots/" in path or path.startswith("runs/") for path in installed_files)


if __name__ == "__main__":
    main()

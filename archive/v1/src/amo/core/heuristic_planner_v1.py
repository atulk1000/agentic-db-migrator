from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_manifest(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    return json.loads(p.read_text())


def generate_plan(manifest_path: str, strategy: str = "largest_first") -> dict[str, Any]:
    """
    Simple heuristic planner:
      - sorts tables by estimated_rows desc (None last)
      - creates one migration step per table
    Output schema:
      { "generated_at": ..., "strategy": ..., "steps": [ ... ] }
    """
    manifest = _load_manifest(manifest_path)
    tables: list[dict[str, Any]] = manifest.get("tables", [])

    def sort_key(t: dict[str, Any]) -> int:
        est = t.get("estimated_rows")
        if est is None:
            return -1
        try:
            return int(est)
        except Exception:
            return -1

    # Largest first is nice for demos (see progress early)
    if strategy == "largest_first":
        tables_sorted = sorted(tables, key=sort_key, reverse=True)
    else:
        tables_sorted = sorted(tables, key=lambda x: (x.get("schema", ""), x.get("table", "")))

    steps: list[dict[str, Any]] = []
    for i, t in enumerate(tables_sorted, start=1):
        steps.append(
            {
                "id": f"step_{i:03d}",
                "op": "copy_table",
                "schema": t["schema"],
                "table": t["table"],
                "estimated_rows": t.get("estimated_rows"),
                "has_geometry": t.get("has_geometry", False),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "planner": "heuristic",
        "strategy": strategy,
        "source": manifest.get("source", {}),
        "steps": steps,
    }


def write_plan(plan: dict[str, Any], out_path: str | Path) -> None:
    p = Path(out_path)
    p.write_text(json.dumps(plan, indent=2, sort_keys=True))

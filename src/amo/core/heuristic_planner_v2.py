from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_manifest(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


def _sort_key_bytes_then_rows(t: Dict[str, Any]) -> Tuple[int, int]:
    b = t.get("estimated_bytes")
    r = t.get("estimated_rows")
    bb = int(b) if isinstance(b, int) else -1
    rr = int(r) if isinstance(r, int) else -1
    return (bb, rr)


def generate_plan(manifest_path: str, strategy: str = "largest_first") -> Dict[str, Any]:
    """
    V2 plan:
      - ensure_schema (per schema)
      - ensure_table (per table)
      - copy_table (per table or per partition child)
      - sync_sequences (per table)
      - create_indexes (per table)
      - add_fks (per schema, last)
      - create_matviews (per schema, after tables)
      - create_mv_indexes (per schema)
      - create_udfs (per schema, optional; can be earlier if you prefer)
    """
    manifest = _load_manifest(manifest_path)
    tables: List[Dict[str, Any]] = manifest.get("tables", [])
    matviews: List[Dict[str, Any]] = manifest.get("matviews", [])
    mv_indexes: List[Dict[str, Any]] = manifest.get("matview_indexes", [])
    udfs: List[Dict[str, Any]] = manifest.get("udfs", [])

    # Planner knobs (hardcoded defaults; you can move to cfg later)
    verify_small_tables = True
    small_table_rows = 100_000  # sample hash only under this

    # Filter out partition parents from copy; instead copy their children
    expanded: List[Dict[str, Any]] = []
    for t in tables:
        part = t.get("partition", {}) or {}
        if part.get("is_partition_parent") and part.get("children"):
            # skip parent copy, add children pseudo-records
            for ch in part["children"]:
                expanded.append(
                    {
                        **t,
                        "schema": ch["schema"],
                        "table": ch["table"],
                        "partition_child_of": f'{t["schema"]}.{t["table"]}',
                        "partition_bound": ch.get("bound"),
                        "partition": {"is_partition_parent": False, "partition_key": None, "children": []},
                    }
                )
        else:
            expanded.append(t)

    if strategy == "largest_first":
        expanded_sorted = sorted(expanded, key=_sort_key_bytes_then_rows, reverse=True)
    else:
        expanded_sorted = sorted(expanded, key=lambda x: (x.get("schema", ""), x.get("table", "")))

    include_schemas = list({t["schema"] for t in expanded_sorted})
    include_schemas.sort()

    steps: List[Dict[str, Any]] = []
    step_i = 1

    # 1) ensure_schema
    for schema in include_schemas:
        steps.append({"id": f"step_{step_i:04d}", "op": "ensure_schema", "schema": schema})
        step_i += 1

        # UDFs (optional): create before matviews if your matviews call functions
        schema_udfs = [u for u in udfs if u.get("schema") == schema]
        if schema_udfs:
            steps.append(
                {
                    "id": f"step_{step_i:04d}",
                    "op": "create_udfs",
                    "schema": schema,
                    "udfs": schema_udfs,
                }
            )
            step_i += 1

    # 2) table pipeline
    # Collect FK defs per schema to add at end
    fks_by_schema: Dict[str, List[Dict[str, Any]]] = {s: [] for s in include_schemas}

    for t in expanded_sorted:
        schema = t["schema"]
        table = t["table"]

        # ensure_table (auto-ddl behavior in executor; harmless if already exists)
        steps.append(
            {
                "id": f"step_{step_i:04d}",
                "op": "ensure_table",
                "schema": schema,
                "table": table,
                "source_table": t.get("partition_child_of"),
            }
        )
        step_i += 1

        # copy_table
        validate = {"rowcount": True, "sample_hash": False, "sample_rows": 50}
        est_rows = t.get("estimated_rows")
        if verify_small_tables and isinstance(est_rows, int) and est_rows <= small_table_rows:
            validate["sample_hash"] = True

        steps.append(
            {
                "id": f"step_{step_i:04d}",
                "op": "copy_table",
                "schema": schema,
                "table": table,
                "estimated_rows": t.get("estimated_rows"),
                "estimated_bytes": t.get("estimated_bytes"),
                "has_geometry": t.get("has_geometry", False),
                "primary_key": t.get("primary_key", []),
                "validate": validate,
            }
        )
        step_i += 1

        # post-copy sequence sync
        steps.append({"id": f"step_{step_i:04d}", "op": "sync_sequences", "schema": schema, "table": table})
        step_i += 1

        # rebuild indexes (after data)
        idxs = t.get("indexes", []) or []
        if idxs:
            steps.append(
                {
                    "id": f"step_{step_i:04d}",
                    "op": "create_indexes",
                    "schema": schema,
                    "table": table,
                    "indexes": idxs,
                }
            )
            step_i += 1

        # gather FK defs for end-of-schema
        for fk in (t.get("foreign_keys") or []):
            fks_by_schema.setdefault(schema, []).append({"schema": schema, "table": table, **fk})

        # optional verify step (if you want verification in the same run loop)
        steps.append(
            {
                "id": f"step_{step_i:04d}",
                "op": "verify_table",
                "schema": schema,
                "table": table,
                "validate": validate,
            }
        )
        step_i += 1

    # 3) add FKs last per schema
    for schema in include_schemas:
        fks = fks_by_schema.get(schema) or []
        if fks:
            steps.append({"id": f"step_{step_i:04d}", "op": "add_fks", "schema": schema, "fks": fks})
            step_i += 1

    # 4) matviews after tables
    for schema in include_schemas:
        schema_mvs = [m for m in matviews if m.get("schema") == schema]
        if schema_mvs:
            steps.append({"id": f"step_{step_i:04d}", "op": "create_matviews", "schema": schema, "matviews": schema_mvs})
            step_i += 1

        schema_mv_idxs = [i for i in mv_indexes if i.get("schema") == schema]
        if schema_mv_idxs:
            steps.append({"id": f"step_{step_i:04d}", "op": "create_mv_indexes", "schema": schema, "indexes": schema_mv_idxs})
            step_i += 1

    return {
        "version": "v2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "planner": "heuristic_v2",
        "strategy": strategy,
        "source": manifest.get("source", {}),
        "steps": steps,
    }


def write_plan(plan: Dict[str, Any], out_path: str | Path) -> None:
    Path(out_path).write_text(json.dumps(plan, indent=2, sort_keys=True))

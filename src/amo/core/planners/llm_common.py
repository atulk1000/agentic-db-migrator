from __future__ import annotations

import json
from typing import Any, Dict, Optional

from amo.core.planners.models import MigrationManifest, ManifestTable, validate_plan_document


DEFAULT_SAMPLE_ROWS = 50


def table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def root_tables(manifest: MigrationManifest) -> list[ManifestTable]:
    partition_children = {
        table_key(child.schema_name, child.table)
        for table in manifest.tables
        for child in table.partition.children
    }
    return [
        table
        for table in manifest.tables
        if table_key(table.schema_name, table.table) not in partition_children
    ]


def default_validate(table: ManifestTable) -> Dict[str, Any]:
    estimated_rows = int(table.estimated_rows or 0)
    sample_hash = estimated_rows <= 100_000
    return {
        "rowcount": True,
        "sample_hash": sample_hash,
        "sample_rows": DEFAULT_SAMPLE_ROWS,
    }


def infer_table_from_name(schema: str, name: Optional[str], known_tables: Dict[str, ManifestTable]) -> Optional[str]:
    if not name:
        return None

    direct_key = table_key(schema, name)
    if direct_key in known_tables:
        return name

    for _, table in known_tables.items():
        if table.schema_name != schema:
            continue
        if name.startswith(f"{table.table}_"):
            return table.table
    return None


def normalize_validate(op: str, step: Dict[str, Any], table_info: Optional[ManifestTable]) -> Dict[str, Any]:
    validate = dict(step.get("validate") or {})
    if op not in ("verify_table", "copy_table"):
        return validate

    mode = step.get("mode")
    if "rowcount" not in validate:
        validate["rowcount"] = True

    if "sample_hash" not in validate:
        if mode == "sample_hash":
            validate["sample_hash"] = True
        elif mode == "rowcount":
            validate["sample_hash"] = False
        else:
            validate["sample_hash"] = default_validate(table_info)["sample_hash"] if table_info else False

    if "sample_rows" not in validate:
        validate["sample_rows"] = DEFAULT_SAMPLE_ROWS

    return validate


def normalize_llm_plan(plan: Dict[str, Any], manifest: MigrationManifest) -> Dict[str, Any]:
    known_tables = {table_key(table.schema_name, table.table): table for table in root_tables(manifest)}

    normalized = dict(plan)
    normalized.setdefault("version", "v2")
    normalized.setdefault("source", manifest.source.model_dump(mode="python"))
    normalized.setdefault("planner_metadata", {})

    new_steps = []
    for index, raw_step in enumerate(plan.get("steps", []), start=1):
        step = dict(raw_step)
        op = step.get("op")
        schema = step.get("schema")
        table = step.get("table")
        if not table:
            table = infer_table_from_name(schema or "", step.get("name"), known_tables)
        if table:
            step["table"] = table

        if not step.get("id"):
            step["id"] = f"step_{index:04d}"

        table_info = known_tables.get(table_key(schema, table)) if schema and table else None

        if table_info is not None:
            step.setdefault("estimated_rows", table_info.estimated_rows)
            step.setdefault("estimated_bytes", table_info.estimated_bytes)
            step.setdefault("has_geometry", table_info.has_geometry)
            step.setdefault("primary_key", list(table_info.primary_key))

        if op in ("copy_table", "verify_table"):
            step["validate"] = normalize_validate(op, step, table_info)

        if op == "create_indexes" and table_info is not None and not step.get("indexes"):
            step["indexes"] = [index_info.model_dump(mode="python") for index_info in table_info.indexes]

        if op == "add_fks":
            if not step.get("fks") and schema and table:
                mapped = known_tables.get(table_key(schema, table))
                if mapped is not None:
                    step["fks"] = [
                        {"schema": schema, "table": mapped.table, **fk.model_dump(mode="python")}
                        for fk in mapped.foreign_keys
                    ]

        for key in ("name", "mode", "fk_name", "notes", "commentary"):
            step.pop(key, None)

        new_steps.append(step)

    normalized["steps"] = new_steps
    return normalized


def parse_normalize_and_validate_llm_plan(
    raw_plan: str | Dict[str, Any],
    manifest: MigrationManifest,
    planner_name: str,
    planner_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    parsed = json.loads(raw_plan) if isinstance(raw_plan, str) else dict(raw_plan)
    normalized = normalize_llm_plan(parsed, manifest)
    normalized["planner"] = planner_name
    normalized.setdefault("planner_metadata", {})
    if planner_metadata:
        normalized["planner_metadata"].update(planner_metadata)
    return validate_plan_document(normalized)

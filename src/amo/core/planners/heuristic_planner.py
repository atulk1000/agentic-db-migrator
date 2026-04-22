from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from amo.core.planners.models import ManifestTable, MigrationManifest, validate_plan_document


def _load_manifest(path: str | Path) -> MigrationManifest:
    return MigrationManifest.model_validate(json.loads(Path(path).read_text()))


def _sort_key_bytes_then_rows(table: ManifestTable) -> Tuple[int, int]:
    estimated_bytes = table.estimated_bytes if isinstance(table.estimated_bytes, int) else -1
    estimated_rows = table.estimated_rows if isinstance(table.estimated_rows, int) else -1
    return (estimated_bytes, estimated_rows)


def _table_ref(table: ManifestTable) -> Tuple[str, str]:
    return (table.schema_name, table.table)


def _iter_included_schemas(tables: Iterable[ManifestTable]) -> List[str]:
    schemas = set()
    for table in tables:
        schemas.add(table.schema_name)
        if table.partition.is_partition_parent:
            schemas.update(child.schema_name for child in table.partition.children)
    return sorted(schemas)


def _append_step(steps: List[Dict[str, Any]], step_i: int, **payload: Any) -> int:
    steps.append({"id": f"step_{step_i:04d}", **payload})
    return step_i + 1


def _choose_transfer_hints(table: ManifestTable) -> Dict[str, Any]:
    chunk_column: Optional[str] = None
    columns_by_name = {column.name: column for column in table.columns}

    partition_key = table.partition.partition_key or ""
    if "(" in partition_key and ")" in partition_key:
        chunk_candidate = partition_key.partition("(")[2].rpartition(")")[0].split(",")[0].strip().strip('"')
        if chunk_candidate in columns_by_name:
            chunk_column = chunk_candidate

    if not chunk_column:
        for candidate in ("created_at", "updated_at", "event_ts", "id"):
            if candidate in columns_by_name:
                chunk_column = candidate
                break

    estimated_rows = int(table.estimated_rows or 0)
    chunk_count = 1 if estimated_rows <= 250_000 else max(2, min(16, (estimated_rows // 250_000) + 1))
    engine = "spark_jdbc" if estimated_rows >= 1_000_000 else "copy"
    geometry_mode = "postgis_native_copy" if table.has_geometry else "default"

    return {
        "engine": engine,
        "chunk_column": chunk_column,
        "chunk_count": chunk_count,
        "geometry_mode": geometry_mode,
    }


def _matview_strategy(matview: Dict[str, Any]) -> str:
    estimated_bytes = matview.get("estimated_bytes") or 0
    if matview.get("has_geometry") or estimated_bytes >= 100_000_000:
        return "staged_rebuild"
    return "direct_rebuild"


def generate_plan(manifest_path: str, strategy: str = "largest_first") -> Dict[str, Any]:
    """
    V2 plan:
      - ensure_schema (per schema)
      - create_udfs (per schema, optional)
      - ensure_table (per root table and partition child)
      - copy_table (per regular table or per partition child)
      - sync_sequences (per root table)
      - create_indexes (per root table)
      - verify_table (per root table)
      - add_fks (per schema, last)
      - create_matviews (per schema)
      - create_mv_indexes (per schema)
    """
    manifest = _load_manifest(manifest_path)
    tables = manifest.tables
    matviews = [item.model_dump(mode="python", by_alias=True) for item in manifest.matviews]
    mv_indexes = [item.model_dump(mode="python", by_alias=True) for item in manifest.matview_indexes]
    udfs = [item.model_dump(mode="python", by_alias=True) for item in manifest.udfs]
    schema_grants = {
        schema: [grant.model_dump(mode="python") for grant in grants]
        for schema, grants in manifest.schema_grants.items()
    }

    verify_small_tables = True
    small_table_rows = 100_000

    partition_children = {
        (child.schema_name, child.table)
        for table in tables
        for child in table.partition.children
    }
    root_tables = [table for table in tables if _table_ref(table) not in partition_children]

    if strategy == "largest_first":
        root_tables = sorted(root_tables, key=_sort_key_bytes_then_rows, reverse=True)
    else:
        root_tables = sorted(root_tables, key=lambda table: (table.schema_name, table.table))

    include_schemas = _iter_included_schemas(root_tables)

    steps: List[Dict[str, Any]] = []
    step_i = 1

    for schema in include_schemas:
        step_i = _append_step(steps, step_i, op="ensure_schema", schema=schema)
        schema_udfs = [item for item in udfs if item["schema"] == schema]
        if schema_udfs:
            step_i = _append_step(steps, step_i, op="create_udfs", schema=schema, udfs=schema_udfs)
        if schema_grants.get(schema):
            step_i = _append_step(steps, step_i, op="apply_grants", schema=schema, grants=schema_grants[schema])

    fks_by_schema: Dict[str, List[Dict[str, Any]]] = {schema: [] for schema in include_schemas}
    grants_by_schema: Dict[str, List[Dict[str, Any]]] = {schema: [] for schema in include_schemas}

    for table_info in root_tables:
        schema = table_info.schema_name
        table = table_info.table
        partition = table_info.partition

        validate = {"rowcount": True, "sample_hash": False, "sample_rows": 50}
        if verify_small_tables and isinstance(table_info.estimated_rows, int) and table_info.estimated_rows <= small_table_rows:
            validate["sample_hash"] = True
        if partition.is_partition_parent and partition.children:
            validate["partition_fidelity"] = True
        transfer = _choose_transfer_hints(table_info)

        step_i = _append_step(steps, step_i, op="ensure_table", schema=schema, table=table)

        if partition.is_partition_parent and partition.children:
            for child in partition.children:
                step_i = _append_step(steps, step_i, op="ensure_table", schema=child.schema_name, table=child.table)
                step_i = _append_step(
                    steps,
                    step_i,
                    op="copy_table",
                    schema=child.schema_name,
                    table=child.table,
                    estimated_rows=table_info.estimated_rows,
                    estimated_bytes=table_info.estimated_bytes,
                    has_geometry=table_info.has_geometry,
                    geometry_columns=list(table_info.geometry_columns),
                    primary_key=list(table_info.primary_key),
                    validate=validate,
                    transfer=transfer,
                )
        else:
            step_i = _append_step(
                steps,
                step_i,
                op="copy_table",
                schema=schema,
                table=table,
                estimated_rows=table_info.estimated_rows,
                estimated_bytes=table_info.estimated_bytes,
                has_geometry=table_info.has_geometry,
                geometry_columns=list(table_info.geometry_columns),
                primary_key=list(table_info.primary_key),
                validate=validate,
                transfer=transfer,
            )

        step_i = _append_step(steps, step_i, op="sync_sequences", schema=schema, table=table)

        indexes = [index.model_dump(mode="python") for index in table_info.indexes]
        if indexes:
            step_i = _append_step(steps, step_i, op="create_indexes", schema=schema, table=table, indexes=indexes)

        for fk in table_info.foreign_keys:
            fks_by_schema.setdefault(schema, []).append(
                {"schema": schema, "table": table, **fk.model_dump(mode="python")}
            )
        for grant in table_info.grants:
            grants_by_schema.setdefault(schema, []).append(grant.model_dump(mode="python"))

        step_i = _append_step(steps, step_i, op="verify_table", schema=schema, table=table, validate=validate)

        maintenance_op = "vacuum_analyze_table" if isinstance(table_info.estimated_rows, int) and table_info.estimated_rows >= 100_000 else "analyze_table"
        step_i = _append_step(
            steps,
            step_i,
            op=maintenance_op,
            schema=schema,
            table=table,
            maintenance={"cluster_if_needed": bool(indexes and any(index.get("cluster_statement") for index in indexes))},
        )

    for schema in include_schemas:
        fks = fks_by_schema.get(schema) or []
        if fks:
            step_i = _append_step(steps, step_i, op="add_fks", schema=schema, fks=fks)
        table_grants = grants_by_schema.get(schema) or []
        if table_grants:
            step_i = _append_step(steps, step_i, op="apply_grants", schema=schema, grants=table_grants)

    for schema in include_schemas:
        schema_mvs = []
        staged_mvs = []
        for item in matviews:
            if item["schema"] != schema:
                continue
            strategy = _matview_strategy(item)
            item = dict(item)
            item["strategy"] = strategy
            if strategy == "staged_rebuild":
                item["staging_table"] = f"qa_{item['name']}_staging"
                staged_mvs.append(item)
            schema_mvs.append(item)
        if staged_mvs:
            step_i = _append_step(steps, step_i, op="stage_matviews", schema=schema, matviews=staged_mvs)
        if schema_mvs:
            step_i = _append_step(steps, step_i, op="create_matviews", schema=schema, matviews=schema_mvs)

        schema_mv_indexes = [item for item in mv_indexes if item["schema"] == schema]
        if schema_mv_indexes:
            step_i = _append_step(steps, step_i, op="create_mv_indexes", schema=schema, indexes=schema_mv_indexes)
        matview_grants = [grant for mv in schema_mvs for grant in mv.get("grants", [])]
        if matview_grants:
            step_i = _append_step(steps, step_i, op="apply_grants", schema=schema, grants=matview_grants)

    return validate_plan_document(
        {
            "version": "v2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "planner": "heuristic_v2",
            "strategy": strategy,
            "source": manifest.source.model_dump(mode="python", by_alias=True),
            "steps": steps,
        }
    )


def write_plan(plan: Dict[str, Any], out_path: str | Path) -> None:
    Path(out_path).write_text(json.dumps(plan, indent=2, sort_keys=True))

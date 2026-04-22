from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from amo.core.manifest_builder import build_manifest
from amo.core.planners.models import ManifestTable, MigrationManifest, validate_plan_document
from amo.core.workflow_models import (
    ApprovalDocument,
    ManifestDiffDocument,
    ManifestDiffSummary,
    MigrationMode,
    PostMigrationExecutionOverview,
    PostMigrationSummary,
    PostMigrationVerificationOverview,
    PreMigrationOverview,
    PreMigrationSummary,
    RiskLevel,
    TableAction,
    TableDiffEntry,
    TableRecommendation,
    TransferStrategy,
    VerificationDepth,
)


CHUNKABLE_TYPE_MARKERS = ("int", "numeric", "date", "timestamp")
TIMESTAMP_COLUMN_HINTS = ("created_at", "updated_at", "event_ts", "timestamp", "ts")


def read_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


def write_json(path: str | Path, obj: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True))


def build_database_manifest(cfg: Dict[str, Any], db_key: str) -> Dict[str, Any]:
    return build_manifest(cfg, db_key=db_key)


def load_manifest(path: str | Path) -> MigrationManifest:
    return MigrationManifest.model_validate(read_json(path))


def load_diff(path: str | Path) -> ManifestDiffDocument:
    return ManifestDiffDocument.model_validate(read_json(path))


def load_pre_summary(path: str | Path) -> PreMigrationSummary:
    return PreMigrationSummary.model_validate(read_json(path))


def load_approval(path: str | Path) -> ApprovalDocument:
    return ApprovalDocument.model_validate(read_json(path))


def _table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _root_tables(manifest: MigrationManifest) -> List[ManifestTable]:
    partition_children = {
        _table_key(child.schema_name, child.table)
        for table in manifest.tables
        for child in table.partition.children
    }
    return [
        table
        for table in manifest.tables
        if _table_key(table.schema_name, table.table) not in partition_children
    ]


def _column_signature(table: ManifestTable) -> List[Tuple[str, Optional[str], bool, str, Optional[str]]]:
    return [
        (
            column.name,
            column.type_sql,
            column.not_null,
            column.attidentity,
            (column.default_sql or "").strip(),
        )
        for column in table.columns
    ]


def _normalized_index_defs(table: ManifestTable) -> List[str]:
    return sorted((index.index_definition or "").strip() for index in table.indexes if index.index_definition)


def _normalized_fk_defs(table: ManifestTable) -> List[str]:
    return sorted((fk.definition or "").strip() for fk in table.foreign_keys if fk.definition)


def _partition_signature(table: ManifestTable) -> Tuple[bool, Optional[str], Tuple[str, ...]]:
    children = tuple(sorted(_table_key(child.schema_name, child.table) for child in table.partition.children))
    return (table.partition.is_partition_parent, table.partition.partition_key, children)


def diff_manifests(source_manifest: Dict[str, Any], target_manifest: Dict[str, Any]) -> Dict[str, Any]:
    source = MigrationManifest.model_validate(source_manifest)
    target = MigrationManifest.model_validate(target_manifest)

    source_tables = {_table_key(table.schema_name, table.table): table for table in _root_tables(source)}
    target_tables = {_table_key(table.schema_name, table.table): table for table in _root_tables(target)}

    entries: List[TableDiffEntry] = []

    for key in sorted(source_tables):
        source_table = source_tables[key]
        target_table = target_tables.get(key)
        if target_table is None:
            entries.append(
                TableDiffEntry(
                    schema_name=source_table.schema_name,
                    table=source_table.table,
                    status="missing_in_target",
                    structure_compatible=True,
                    source_present=True,
                    target_present=False,
                    source_estimated_rows=source_table.estimated_rows,
                )
            )
            continue

        structural_drift: List[str] = []
        auxiliary_drift: List[str] = []

        if _column_signature(source_table) != _column_signature(target_table):
            structural_drift.append("columns")
        if list(source_table.primary_key) != list(target_table.primary_key):
            structural_drift.append("primary_key")
        if _partition_signature(source_table) != _partition_signature(target_table):
            structural_drift.append("partition_layout")
        if source_table.has_geometry != target_table.has_geometry:
            structural_drift.append("geometry")
        if _normalized_index_defs(source_table) != _normalized_index_defs(target_table):
            auxiliary_drift.append("indexes")
        if _normalized_fk_defs(source_table) != _normalized_fk_defs(target_table):
            auxiliary_drift.append("foreign_keys")

        status = "metadata_match"
        if structural_drift or auxiliary_drift:
            status = "metadata_diff"

        entries.append(
            TableDiffEntry(
                schema_name=source_table.schema_name,
                table=source_table.table,
                status=status,
                structure_compatible=not structural_drift,
                source_present=True,
                target_present=True,
                source_estimated_rows=source_table.estimated_rows,
                target_estimated_rows=target_table.estimated_rows,
                structural_drift=structural_drift,
                auxiliary_drift=auxiliary_drift,
            )
        )

    for key in sorted(set(target_tables) - set(source_tables)):
        target_table = target_tables[key]
        entries.append(
            TableDiffEntry(
                schema_name=target_table.schema_name,
                table=target_table.table,
                status="missing_in_source",
                structure_compatible=False,
                source_present=False,
                target_present=True,
                target_estimated_rows=target_table.estimated_rows,
            )
        )

    summary = ManifestDiffSummary(
        source_tables=len(source_tables),
        target_tables=len(target_tables),
        missing_in_target=sum(1 for item in entries if item.status == "missing_in_target"),
        missing_in_source=sum(1 for item in entries if item.status == "missing_in_source"),
        metadata_match=sum(1 for item in entries if item.status == "metadata_match"),
        metadata_diff=sum(1 for item in entries if item.status == "metadata_diff"),
    )

    warnings = []
    target_only = [entry for entry in entries if entry.status == "missing_in_source"]
    if target_only:
        warnings.append(f"Target contains {len(target_only)} table(s) not present in source.")

    return ManifestDiffDocument(summary=summary, tables=entries, warnings=warnings).model_dump(mode="python", by_alias=True)


def _extract_partition_chunk_column(table: ManifestTable) -> Optional[str]:
    partition_key = table.partition.partition_key or ""
    match = re.search(r"\(([^)]+)\)", partition_key)
    if not match:
        return None
    raw = match.group(1).split(",")[0].strip()
    return raw.strip('"')


def _is_chunkable_type(type_sql: Optional[str]) -> bool:
    lowered = (type_sql or "").lower()
    return any(marker in lowered for marker in CHUNKABLE_TYPE_MARKERS)


def _choose_chunk_column(table: ManifestTable) -> Optional[str]:
    partition_column = _extract_partition_chunk_column(table)
    columns = {column.name: column for column in table.columns}
    if partition_column and partition_column in columns and _is_chunkable_type(columns[partition_column].type_sql):
        return partition_column

    for name in TIMESTAMP_COLUMN_HINTS:
        column = columns.get(name)
        if column and _is_chunkable_type(column.type_sql):
            return column.name

    for pk_name in table.primary_key:
        column = columns.get(pk_name)
        if column and _is_chunkable_type(column.type_sql):
            return column.name

    for column in table.columns:
        if column.name.endswith("_id") and _is_chunkable_type(column.type_sql):
            return column.name

    return None


def _determine_chunk_count(table: ManifestTable, chunk_column: Optional[str]) -> int:
    estimated_rows = table.estimated_rows or 0
    if not chunk_column or estimated_rows <= 250_000:
        return 1
    return max(2, min(16, (estimated_rows // 250_000) + 1))


def _determine_transfer_strategy(table: ManifestTable, chunk_count: int) -> TransferStrategy:
    if table.partition.is_partition_parent and table.partition.children:
        return "partition_wise_copy"
    if chunk_count > 1:
        return "chunked_copy"
    return "full_copy"


def _risk_level(score: int) -> RiskLevel:
    if score >= 70:
        return "high"
    if score >= 35:
        return "medium"
    return "low"


def _score_risk(table: ManifestTable, diff_entry: TableDiffEntry, chunk_column: Optional[str]) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []

    estimated_rows = table.estimated_rows or 0
    if estimated_rows >= 1_000_000:
        score += 30
        reasons.append("large row volume")
    elif estimated_rows >= 100_000:
        score += 15
        reasons.append("moderate row volume")

    if not table.primary_key:
        score += 25
        reasons.append("missing primary key")

    if diff_entry.structural_drift:
        score += 30
        reasons.append("structural metadata drift")

    if diff_entry.auxiliary_drift:
        score += 10
        reasons.append("index or foreign key drift")

    if table.partition.is_partition_parent:
        score += 10
        reasons.append("partitioned table")

    if table.has_geometry:
        score += 10
        reasons.append("geometry columns")

    if estimated_rows >= 500_000 and not chunk_column and not table.partition.is_partition_parent:
        score += 20
        reasons.append("large table without a safe chunk column")

    return min(score, 100), reasons


def _choose_verification_depth(table: ManifestTable, risk_score: int) -> VerificationDepth:
    estimated_rows = table.estimated_rows or 0
    if estimated_rows <= 100_000 or risk_score >= 60:
        return "rowcount_and_sample_hash"
    return "rowcount"


def _recommend_action(mode: MigrationMode, diff_entry: TableDiffEntry) -> TableAction:
    if diff_entry.status == "missing_in_source":
        return "manual_review"

    if mode == "plan_only":
        if diff_entry.status == "metadata_diff" and not diff_entry.structure_compatible:
            return "manual_review"
        return "copy" if diff_entry.status == "missing_in_target" else "skip"

    if mode == "missing_only":
        return "copy" if diff_entry.status == "missing_in_target" else "skip"

    if mode == "metadata_diff_only":
        if diff_entry.status == "metadata_match":
            return "skip"
        if diff_entry.status == "missing_in_target":
            return "copy"
        if diff_entry.structure_compatible:
            return "sync_metadata" if diff_entry.auxiliary_drift else "copy"
        return "manual_review"

    if mode == "data_diff_only":
        return "copy" if diff_entry.structure_compatible or diff_entry.status == "missing_in_target" else "manual_review"

    if mode in ("full_refresh", "safe_sync"):
        return "copy" if diff_entry.structure_compatible or diff_entry.status == "missing_in_target" else "manual_review"

    return "manual_review"


def _build_recommendation(table: ManifestTable, diff_entry: TableDiffEntry, mode: MigrationMode) -> TableRecommendation:
    chunk_column = _choose_chunk_column(table)
    chunk_count = _determine_chunk_count(table, chunk_column)
    transfer_strategy = _determine_transfer_strategy(table, chunk_count)
    risk_score, risk_reasons = _score_risk(table, diff_entry, chunk_column)
    action = _recommend_action(mode, diff_entry)
    verification_depth = _choose_verification_depth(table, risk_score)
    warnings = list(risk_reasons)

    manual_review_required = action == "manual_review"
    if diff_entry.status == "missing_in_target":
        warnings.append("table is missing in target")
    if diff_entry.structural_drift:
        warnings.append("structural drift must be reviewed before execution")
    if transfer_strategy == "chunked_copy" and not chunk_column:
        warnings.append("chunking requested without a chunk column; fallback to full copy")

    concurrency_hint = min(chunk_count, 4)
    rationale_bits = [
        f"action={action}",
        f"strategy={transfer_strategy}",
        f"risk={risk_score}",
    ]
    if chunk_column:
        rationale_bits.append(f"chunk_column={chunk_column}")

    return TableRecommendation(
        schema_name=table.schema_name,
        table=table.table,
        diff_status=diff_entry.status,
        action=action,
        transfer_strategy=transfer_strategy,
        chunk_column=chunk_column,
        chunk_count=chunk_count,
        concurrency_hint=concurrency_hint,
        verification_depth=verification_depth,
        risk_score=risk_score,
        risk_level=_risk_level(risk_score),
        warnings=warnings,
        manual_review_required=manual_review_required,
        rationale=", ".join(rationale_bits),
    )


def build_pre_migration_summary(
    source_manifest: Dict[str, Any],
    target_manifest: Dict[str, Any],
    manifest_diff: Dict[str, Any],
    plan: Dict[str, Any],
    migration_mode: MigrationMode = "safe_sync",
) -> Dict[str, Any]:
    source = MigrationManifest.model_validate(source_manifest)
    diff = ManifestDiffDocument.model_validate(manifest_diff)

    diff_map = {_table_key(item.schema_name, item.table): item for item in diff.tables}
    recommendations = [
        _build_recommendation(table, diff_map[_table_key(table.schema_name, table.table)], migration_mode)
        for table in _root_tables(source)
        if _table_key(table.schema_name, table.table) in diff_map
    ]

    manual_review_required = [
        _table_key(item.schema_name, item.table)
        for item in recommendations
        if item.manual_review_required
    ]

    destructive_actions = [
        f"{item.schema_name}.{item.table}: structural target drift blocks safe automatic overwrite"
        for item in recommendations
        if item.diff_status == "metadata_diff" and item.manual_review_required
    ]

    preflight_warnings = list(diff.warnings)
    if manual_review_required:
        preflight_warnings.append(f"{len(manual_review_required)} table(s) require manual review.")

    overview = PreMigrationOverview(
        mode=migration_mode,
        planner=plan.get("planner", "unknown"),
        source_tables=diff.summary.source_tables,
        target_tables=diff.summary.target_tables,
        tables_to_copy=sum(1 for item in recommendations if item.action == "copy"),
        tables_to_sync_metadata=sum(1 for item in recommendations if item.action == "sync_metadata"),
        manual_review_count=len(manual_review_required),
        skipped_tables=sum(1 for item in recommendations if item.action == "skip"),
    )

    planner_recommendation = (
        "Review manual-review items before execution." if manual_review_required else "Plan can proceed after user approval."
    )

    return PreMigrationSummary(
        overview=overview,
        drift_summary=diff.summary,
        preflight_warnings=preflight_warnings,
        manual_review_required=manual_review_required,
        destructive_actions=destructive_actions,
        table_recommendations=recommendations,
        planner_recommendation=planner_recommendation,
    ).model_dump(mode="python", by_alias=True)


def render_pre_migration_summary(summary: Dict[str, Any]) -> str:
    doc = PreMigrationSummary.model_validate(summary)
    lines = [
        f"Pre-Migration Summary",
        f"Mode: {doc.overview.mode}",
        f"Planner: {doc.overview.planner}",
        f"Source tables: {doc.overview.source_tables}",
        f"Target tables: {doc.overview.target_tables}",
        f"Copy candidates: {doc.overview.tables_to_copy}",
        f"Metadata-sync candidates: {doc.overview.tables_to_sync_metadata}",
        f"Manual review required: {doc.overview.manual_review_count}",
        f"Skipped: {doc.overview.skipped_tables}",
        "",
        "Warnings:",
    ]
    if doc.preflight_warnings:
        lines.extend(f"- {warning}" for warning in doc.preflight_warnings)
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Table Recommendations:")
    for recommendation in doc.table_recommendations:
        lines.append(
            f"- {_table_key(recommendation.schema_name, recommendation.table)}: "
            f"action={recommendation.action}, strategy={recommendation.transfer_strategy}, "
            f"chunk_column={recommendation.chunk_column or 'n/a'}, chunk_count={recommendation.chunk_count}, "
            f"verification={recommendation.verification_depth}, risk={recommendation.risk_level}"
        )
    return "\n".join(lines)


def build_approval_document(
    plan_path: str | Path,
    summary_path: str | Path,
    approved_mode: MigrationMode,
    approved_by: str = "manual",
    allow_destructive: bool = False,
    include_tables: Optional[Iterable[str]] = None,
    exclude_tables: Optional[Iterable[str]] = None,
    approved_manual_review_items: Optional[Iterable[str]] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    summary = load_pre_summary(summary_path)

    default_included = [
        _table_key(item.schema_name, item.table)
        for item in summary.table_recommendations
        if item.action in ("copy", "sync_metadata")
    ]

    include_list = sorted(set(include_tables or default_included))
    exclude_list = sorted(set(exclude_tables or []))

    approval = ApprovalDocument(
        approved_mode=approved_mode,
        approved_at=datetime.now(timezone.utc).isoformat(),
        approved_by=approved_by,
        plan_path=str(plan_path),
        summary_path=str(summary_path),
        allow_destructive=allow_destructive,
        included_tables=include_list,
        excluded_tables=exclude_list,
        approved_manual_review_items=sorted(set(approved_manual_review_items or [])),
        notes=notes,
    )
    return approval.model_dump(mode="python", by_alias=True)


def filter_plan_for_approval(
    plan: Dict[str, Any],
    summary: Dict[str, Any],
    approval: Dict[str, Any],
) -> Dict[str, Any]:
    plan_obj = validate_plan_document(plan)
    summary_doc = PreMigrationSummary.model_validate(summary)
    approval_doc = ApprovalDocument.model_validate(approval)

    recommendation_map = {
        _table_key(item.schema_name, item.table): item
        for item in summary_doc.table_recommendations
    }
    included_tables = set(approval_doc.included_tables)
    excluded_tables = set(approval_doc.excluded_tables)
    approved_manual = set(approval_doc.approved_manual_review_items)

    approved_actions: Dict[str, str] = {}
    for key, recommendation in recommendation_map.items():
        if key in excluded_tables:
            continue
        if key not in included_tables:
            continue
        if recommendation.action == "manual_review":
            if key in approved_manual and approval_doc.allow_destructive:
                approved_actions[key] = "copy"
            continue
        if approval_doc.approved_mode == "plan_only":
            continue
        approved_actions[key] = recommendation.action

    approved_schemas = {key.split(".", 1)[0] for key in approved_actions}
    filtered_steps: List[Dict[str, Any]] = []
    partition_parent_keys = {
        _table_key(step.get("schema"), step.get("table"))
        for step in plan_obj.get("steps", [])
        if step.get("op") == "verify_table"
        and bool((step.get("validate") or {}).get("partition_fidelity", False))
        and step.get("schema")
        and step.get("table")
    }
    active_partition_parent: Optional[str] = None

    for step in plan_obj.get("steps", []):
        op = step.get("op")
        schema = step.get("schema")
        table = step.get("table")
        table_key = _table_key(schema, table) if schema and table else None

        if active_partition_parent:
            if op in ("ensure_table", "copy_table") and table_key not in approved_actions:
                filtered_steps.append(step)
                continue
            if table_key == active_partition_parent and op in (
                "sync_sequences",
                "create_indexes",
                "verify_table",
                "analyze_table",
                "vacuum_analyze_table",
            ):
                filtered_steps.append(step)
                if op in ("analyze_table", "vacuum_analyze_table", "verify_table"):
                    active_partition_parent = None
                continue
            active_partition_parent = None

        if op == "ensure_schema":
            if schema in approved_schemas:
                filtered_steps.append(step)
            continue

        if op == "create_udfs":
            if approval_doc.approved_mode != "data_diff_only" and schema in approved_schemas:
                filtered_steps.append(step)
            continue

        if op == "add_fks":
            fks = [
                fk for fk in step.get("fks", [])
                if _table_key(fk.get("schema", schema), fk.get("table", "")) in approved_actions
            ]
            if fks:
                updated_step = dict(step)
                updated_step["fks"] = fks
                filtered_steps.append(updated_step)
            continue

        if op in ("create_matviews", "create_mv_indexes"):
            if approval_doc.approved_mode != "data_diff_only" and schema in approved_schemas:
                filtered_steps.append(step)
            continue

        if not table_key or table_key not in approved_actions:
            continue

        action = approved_actions[table_key]
        if op == "ensure_table" and table_key in partition_parent_keys:
            active_partition_parent = table_key
        if action == "sync_metadata" and op not in ("ensure_table", "create_indexes"):
            continue

        filtered_steps.append(step)

    filtered_plan = dict(plan_obj)
    filtered_plan["steps"] = filtered_steps
    filtered_plan["approval"] = {
        "approved_mode": approval_doc.approved_mode,
        "approved_tables": sorted(approved_actions),
    }
    return filtered_plan


def build_post_migration_summary(
    plan: Dict[str, Any],
    state: Dict[str, Any],
    report: Optional[Dict[str, Any]] = None,
    pre_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    steps = plan.get("steps", [])
    completed = state.get("completed", {})
    failed_steps = [step_id for step_id, payload in completed.items() if not payload.get("ok", False)]
    execution = PostMigrationExecutionOverview(
        total_steps=len(steps),
        completed_steps=len(completed),
        failed_steps=len(failed_steps),
        skipped_steps=max(len(steps) - len(completed), 0),
        success=not failed_steps and bool((report or {}).get("ok", True)),
    )

    failed_tables = [
        _table_key(item.get("schema", ""), item.get("table", ""))
        for item in (report or {}).get("results", [])
        if not item.get("ok", False)
    ]
    verification = PostMigrationVerificationOverview(
        ok=bool((report or {}).get("ok", not failed_tables)),
        tables_checked=int((report or {}).get("tables_checked", 0)),
        failed_tables=failed_tables,
    )

    residual_manual_review: List[str] = []
    if pre_summary:
        residual_manual_review = list(
            PreMigrationSummary.model_validate(pre_summary).manual_review_required
        )

    next_actions: List[str] = []
    if failed_steps:
        next_actions.append("Review failed steps in the state file before retrying.")
    if failed_tables:
        next_actions.append("Inspect verification mismatches before cutover.")
    if residual_manual_review:
        next_actions.append("Resolve outstanding manual-review items.")
    if not next_actions:
        next_actions.append("Migration completed cleanly. Review the post-migration report and proceed to cutover.")

    return PostMigrationSummary(
        execution_overview=execution,
        verification_summary=verification,
        failed_steps=failed_steps,
        residual_manual_review=residual_manual_review,
        next_actions=next_actions,
        notes=[],
    ).model_dump(mode="python", by_alias=True)


def render_post_migration_summary(summary: Dict[str, Any]) -> str:
    doc = PostMigrationSummary.model_validate(summary)
    lines = [
        "Post-Migration Summary",
        f"Success: {doc.execution_overview.success}",
        f"Steps completed: {doc.execution_overview.completed_steps}/{doc.execution_overview.total_steps}",
        f"Failed steps: {doc.execution_overview.failed_steps}",
        f"Tables verified: {doc.verification_summary.tables_checked}",
        f"Verification ok: {doc.verification_summary.ok}",
        "",
        "Next Actions:",
    ]
    lines.extend(f"- {item}" for item in doc.next_actions)
    return "\n".join(lines)

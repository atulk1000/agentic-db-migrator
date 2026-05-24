from __future__ import annotations

import json
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from amo.core.manifest_builder import build_manifest
from amo.core.planners.models import ManifestTable, MigrationManifest, validate_plan_document
from amo.core.workflow_models import (
    ApprovalDocument,
    KeyReadinessStatus,
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
    TableStrategy,
    TransferStrategy,
    VerificationDepth,
)

CHUNKABLE_TYPE_MARKERS = ("int", "numeric", "date", "timestamp")
TIMESTAMP_COLUMN_HINTS = ("created_at", "updated_at", "event_ts", "timestamp", "ts")


def read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def write_json(path: str | Path, obj: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True))


def build_database_manifest(cfg: dict[str, Any], db_key: str) -> dict[str, Any]:
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


def _root_tables(manifest: MigrationManifest) -> list[ManifestTable]:
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


def _column_signature(
    table: ManifestTable,
) -> list[tuple[str, str | None, bool, str, str | None]]:
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


def _normalized_index_defs(table: ManifestTable) -> list[str]:
    return sorted(
        (index.index_definition or "").strip() for index in table.indexes if index.index_definition
    )


def _normalized_fk_defs(table: ManifestTable) -> list[str]:
    return sorted((fk.definition or "").strip() for fk in table.foreign_keys if fk.definition)


def _partition_signature(table: ManifestTable) -> tuple[bool, str | None, tuple[str, ...]]:
    children = tuple(
        sorted(_table_key(child.schema_name, child.table) for child in table.partition.children)
    )
    return (table.partition.is_partition_parent, table.partition.partition_key, children)


def diff_manifests(
    source_manifest: dict[str, Any], target_manifest: dict[str, Any]
) -> dict[str, Any]:
    source = MigrationManifest.model_validate(source_manifest)
    target = MigrationManifest.model_validate(target_manifest)

    source_tables = {
        _table_key(table.schema_name, table.table): table for table in _root_tables(source)
    }
    target_tables = {
        _table_key(table.schema_name, table.table): table for table in _root_tables(target)
    }

    entries: list[TableDiffEntry] = []

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

        structural_drift: list[str] = []
        auxiliary_drift: list[str] = []

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

    return ManifestDiffDocument(summary=summary, tables=entries, warnings=warnings).model_dump(
        mode="python", by_alias=True
    )


def _extract_partition_chunk_column(table: ManifestTable) -> str | None:
    partition_key = table.partition.partition_key or ""
    match = re.search(r"\(([^)]+)\)", partition_key)
    if not match:
        return None
    raw = match.group(1).split(",")[0].strip()
    return raw.strip('"')


def _is_chunkable_type(type_sql: str | None) -> bool:
    lowered = (type_sql or "").lower()
    return any(marker in lowered for marker in CHUNKABLE_TYPE_MARKERS)


def _choose_chunk_column(table: ManifestTable) -> str | None:
    partition_column = _extract_partition_chunk_column(table)
    columns = {column.name: column for column in table.columns}
    if (
        partition_column
        and partition_column in columns
        and _is_chunkable_type(columns[partition_column].type_sql)
    ):
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


def _determine_chunk_count(table: ManifestTable, chunk_column: str | None) -> int:
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


def _score_risk(
    table: ManifestTable, diff_entry: TableDiffEntry, chunk_column: str | None
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

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
        return (
            "copy"
            if diff_entry.structure_compatible or diff_entry.status == "missing_in_target"
            else "manual_review"
        )

    if mode in ("full_refresh", "safe_sync"):
        return (
            "copy"
            if diff_entry.structure_compatible or diff_entry.status == "missing_in_target"
            else "manual_review"
        )

    return "manual_review"


def _determine_key_readiness(
    source_table: ManifestTable, target_table: ManifestTable | None
) -> tuple[KeyReadinessStatus, list[str], bool]:
    source_pk = list(source_table.primary_key)
    target_pk = list(target_table.primary_key) if target_table else []

    if source_pk and target_pk and source_pk == target_pk:
        return "primary_key", source_pk, True
    if source_pk and not target_pk:
        return "source_only_key", source_pk, False
    if target_pk and not source_pk:
        return "target_only_key", target_pk, False
    if source_pk and target_pk and source_pk != target_pk:
        return "ambiguous_key", [], False
    return "no_key", [], False


def _build_recommendation(
    table: ManifestTable,
    diff_entry: TableDiffEntry,
    mode: MigrationMode,
    target_table: ManifestTable | None = None,
) -> TableRecommendation:
    chunk_column = _choose_chunk_column(table)
    chunk_count = _determine_chunk_count(table, chunk_column)
    transfer_strategy = _determine_transfer_strategy(table, chunk_count)
    risk_score, risk_reasons = _score_risk(table, diff_entry, chunk_column)
    action = _recommend_action(mode, diff_entry)
    verification_depth = _choose_verification_depth(table, risk_score)
    key_readiness, conflict_key, upsert_eligible = _determine_key_readiness(table, target_table)
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
        key_readiness=key_readiness,
        conflict_key=conflict_key,
        upsert_eligible=upsert_eligible,
    )


def build_pre_migration_summary(
    source_manifest: dict[str, Any],
    target_manifest: dict[str, Any],
    manifest_diff: dict[str, Any],
    plan: dict[str, Any],
    migration_mode: MigrationMode = "safe_sync",
) -> dict[str, Any]:
    source = MigrationManifest.model_validate(source_manifest)
    target = MigrationManifest.model_validate(target_manifest)
    diff = ManifestDiffDocument.model_validate(manifest_diff)

    diff_map = {_table_key(item.schema_name, item.table): item for item in diff.tables}
    target_map = {_table_key(table.schema_name, table.table): table for table in target.tables}
    recommendations = [
        _build_recommendation(
            table,
            diff_map[_table_key(table.schema_name, table.table)],
            migration_mode,
            target_map.get(_table_key(table.schema_name, table.table)),
        )
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
        tables_to_sync_metadata=sum(
            1 for item in recommendations if item.action == "sync_metadata"
        ),
        manual_review_count=len(manual_review_required),
        skipped_tables=sum(1 for item in recommendations if item.action == "skip"),
    )

    planner_recommendation = (
        "Review manual-review items before execution."
        if manual_review_required
        else "Plan can proceed after user approval."
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


def render_pre_migration_summary(summary: dict[str, Any]) -> str:
    doc = PreMigrationSummary.model_validate(summary)
    lines = [
        "Pre-Migration Summary",
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


def _normalize_table_strategies(
    raw_strategies: dict[str, Any] | None,
    summary: PreMigrationSummary,
) -> dict[str, TableStrategy]:
    if not raw_strategies:
        return {}

    recommendation_map = {
        _table_key(item.schema_name, item.table): item for item in summary.table_recommendations
    }
    normalized: dict[str, TableStrategy] = {}
    for table_key, raw in raw_strategies.items():
        if table_key not in recommendation_map:
            raise ValueError(f"Strategy references unknown table: {table_key}")

        strategy_obj = raw if isinstance(raw, TableStrategy) else TableStrategy.model_validate(raw)
        recommendation = recommendation_map[table_key]

        if strategy_obj.strategy == "upsert" and not strategy_obj.conflict_key:
            strategy_obj = strategy_obj.model_copy(
                update={"conflict_key": list(recommendation.conflict_key)}
            )

        normalized[table_key] = strategy_obj

    return normalized


def build_approval_document(
    plan_path: str | Path,
    summary_path: str | Path,
    approved_mode: MigrationMode,
    approved_by: str = "manual",
    allow_destructive: bool = False,
    include_tables: Iterable[str] | None = None,
    exclude_tables: Iterable[str] | None = None,
    approved_manual_review_items: Iterable[str] | None = None,
    table_strategies: dict[str, Any] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    summary = load_pre_summary(summary_path)
    normalized_strategies = _normalize_table_strategies(table_strategies, summary)

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
        table_strategies=normalized_strategies,
        notes=notes,
    )
    return approval.model_dump(mode="python", by_alias=True)


def filter_plan_for_approval(
    plan: dict[str, Any],
    summary: dict[str, Any],
    approval: dict[str, Any],
) -> dict[str, Any]:
    plan_obj = validate_plan_document(plan)
    summary_doc = PreMigrationSummary.model_validate(summary)
    approval_doc = ApprovalDocument.model_validate(approval)

    recommendation_map = {
        _table_key(item.schema_name, item.table): item for item in summary_doc.table_recommendations
    }
    included_tables = set(approval_doc.included_tables)
    excluded_tables = set(approval_doc.excluded_tables)
    approved_manual = set(approval_doc.approved_manual_review_items)
    table_strategies = approval_doc.table_strategies

    approved_actions: dict[str, str] = {}
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
        if table_strategies.get(key, TableStrategy(strategy="append_only")).strategy == "skip":
            continue
        approved_actions[key] = recommendation.action

    approved_schemas = {key.split(".", 1)[0] for key in approved_actions}
    filtered_steps: list[dict[str, Any]] = []
    partition_parent_keys = {
        _table_key(step.get("schema"), step.get("table"))
        for step in plan_obj.get("steps", [])
        if step.get("op") == "verify_table"
        and bool((step.get("validate") or {}).get("partition_fidelity", False))
        and step.get("schema")
        and step.get("table")
    }
    active_partition_parent: str | None = None

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
                fk
                for fk in step.get("fks", [])
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
        table_strategy = table_strategies.get(table_key)
        if op == "ensure_table" and table_key in partition_parent_keys:
            active_partition_parent = table_key
        if action == "sync_metadata" and op not in ("ensure_table", "create_indexes"):
            continue

        if table_strategy and op == "copy_table":
            updated_step = dict(step)
            transfer = dict(updated_step.get("transfer") or {})
            transfer["load_strategy"] = table_strategy.strategy
            if table_strategy.conflict_key:
                transfer["conflict_key"] = list(table_strategy.conflict_key)
            updated_step["transfer"] = transfer
            if table_strategy.strategy == "upsert":
                updated_step["op"] = "upsert_table"
                updated_step["conflict_key"] = list(table_strategy.conflict_key)
            filtered_steps.append(updated_step)
            continue

        filtered_steps.append(step)

    filtered_plan = dict(plan_obj)
    filtered_plan["steps"] = filtered_steps
    filtered_plan["approval"] = {
        "approved_mode": approval_doc.approved_mode,
        "approved_tables": sorted(approved_actions),
        "table_strategies": {
            key: strategy.model_dump(mode="python")
            for key, strategy in approval_doc.table_strategies.items()
            if key in approved_actions
        },
    }
    return filtered_plan


def _strategy_for_table(approval_doc: ApprovalDocument, table_key: str) -> TableStrategy | None:
    return approval_doc.table_strategies.get(table_key)


def validate_approval_strategies(
    summary: dict[str, Any],
    approval: dict[str, Any],
) -> list[dict[str, Any]]:
    summary_doc = PreMigrationSummary.model_validate(summary)
    approval_doc = ApprovalDocument.model_validate(approval)
    recommendation_map = {
        _table_key(item.schema_name, item.table): item for item in summary_doc.table_recommendations
    }
    included_tables = set(approval_doc.included_tables)
    excluded_tables = set(approval_doc.excluded_tables)
    errors: list[dict[str, Any]] = []

    for table_key, table_strategy in approval_doc.table_strategies.items():
        recommendation = recommendation_map.get(table_key)
        if not recommendation:
            errors.append(
                {
                    "table": table_key,
                    "strategy": table_strategy.strategy,
                    "reason": "strategy references a table that is not in the pre-migration summary",
                }
            )
            continue
        if table_key in excluded_tables:
            errors.append(
                {
                    "table": table_key,
                    "strategy": table_strategy.strategy,
                    "reason": "excluded tables cannot also declare an execution strategy",
                }
            )
        if table_key not in included_tables and table_strategy.strategy != "skip":
            errors.append(
                {
                    "table": table_key,
                    "strategy": table_strategy.strategy,
                    "reason": "strategy table is not included in the approval scope",
                }
            )
        if table_strategy.strategy == "upsert":
            if not table_strategy.conflict_key:
                errors.append(
                    {
                        "table": table_key,
                        "strategy": table_strategy.strategy,
                        "reason": "upsert requires a conflict key",
                    }
                )
            elif not recommendation.upsert_eligible:
                errors.append(
                    {
                        "table": table_key,
                        "strategy": table_strategy.strategy,
                        "reason": f"table is not upsert-ready: {recommendation.key_readiness}",
                    }
                )
            elif list(table_strategy.conflict_key) != list(recommendation.conflict_key):
                errors.append(
                    {
                        "table": table_key,
                        "strategy": table_strategy.strategy,
                        "reason": "upsert conflict key must match the validated primary key",
                    }
                )
        if table_strategy.strategy == "truncate_reload" and not approval_doc.allow_destructive:
            errors.append(
                {
                    "table": table_key,
                    "strategy": table_strategy.strategy,
                    "reason": "truncate_reload requires allow_destructive=true in approval",
                }
            )

    return errors


def build_dry_run_preview(
    plan: dict[str, Any],
    summary: dict[str, Any],
    approval: dict[str, Any],
) -> dict[str, Any]:
    strategy_errors = validate_approval_strategies(summary, approval)
    if strategy_errors:
        return {
            "ok": False,
            "blocked": strategy_errors,
            "approved_tables": [],
            "steps": [],
            "destructive_actions": [],
        }

    filtered_plan = filter_plan_for_approval(plan=plan, summary=summary, approval=approval)
    approval_doc = ApprovalDocument.model_validate(approval)
    summary_doc = PreMigrationSummary.model_validate(summary)
    recommendation_map = {
        _table_key(item.schema_name, item.table): item for item in summary_doc.table_recommendations
    }

    approved_tables: list[dict[str, Any]] = []
    for table_key in filtered_plan.get("approval", {}).get("approved_tables", []):
        recommendation = recommendation_map.get(table_key)
        table_strategy = _strategy_for_table(approval_doc, table_key)
        strategy = table_strategy.strategy if table_strategy else "config_default"
        conflict_key = table_strategy.conflict_key if table_strategy else []
        approved_tables.append(
            {
                "table": table_key,
                "action": recommendation.action if recommendation else None,
                "strategy": strategy,
                "conflict_key": conflict_key,
                "key_readiness": recommendation.key_readiness if recommendation else None,
            }
        )

    step_rows = [
        {
            "id": step.get("id"),
            "op": step.get("op"),
            "schema": step.get("schema"),
            "table": step.get("table"),
            "strategy": (step.get("transfer") or {}).get("load_strategy"),
            "conflict_key": step.get("conflict_key")
            or (step.get("transfer") or {}).get("conflict_key", []),
        }
        for step in filtered_plan.get("steps", [])
    ]
    destructive_actions = [
        item["table"] for item in approved_tables if item.get("strategy") == "truncate_reload"
    ]

    ok = bool(step_rows) or approval_doc.approved_mode == "plan_only"
    return {
        "ok": ok,
        "blocked": (
            []
            if ok
            else [
                {
                    "reason": "filtered approval produced no executable steps",
                    "approved_mode": approval_doc.approved_mode,
                }
            ]
        ),
        "approved_mode": approval_doc.approved_mode,
        "approved_tables": approved_tables,
        "destructive_actions": destructive_actions,
        "step_count": len(step_rows),
        "steps": step_rows,
        "filtered_plan": filtered_plan,
    }


def render_dry_run_preview(preview: dict[str, Any]) -> str:
    lines = [
        "Dry-Run Preview",
        f"Status: {'ready' if preview.get('ok') else 'blocked'}",
        f"Approved mode: {preview.get('approved_mode', '-')}",
        f"Step count: {preview.get('step_count', 0)}",
        "",
        "Approved Tables:",
    ]
    for item in preview.get("approved_tables", []):
        key = item.get("table")
        strategy = item.get("strategy")
        conflict_key = ",".join(item.get("conflict_key") or []) or "-"
        lines.append(f"- {key}: strategy={strategy}, conflict_key={conflict_key}")
    if not preview.get("approved_tables"):
        lines.append("- none")

    destructive = preview.get("destructive_actions") or []
    lines.extend(["", "Destructive Actions:"])
    if destructive:
        for table in destructive:
            lines.append(f"- {table}: truncate_reload")
    else:
        lines.append("- none")

    blocked = preview.get("blocked") or []
    lines.extend(["", "Blocked Items:"])
    if blocked:
        for item in blocked:
            lines.append(f"- {item.get('table', '-')}: {item.get('reason', 'blocked')}")
    else:
        lines.append("- none")

    lines.extend(["", "Execution Steps:"])
    for step in preview.get("steps", []):
        table = (
            _table_key(step["schema"], step["table"]) if step.get("table") else step.get("schema")
        )
        strategy = step.get("strategy") or "-"
        lines.append(f"- {step.get('id')}: {step.get('op')} {table} strategy={strategy}")
    if not preview.get("steps"):
        lines.append("- none")

    return "\n".join(lines)


def _step_table_key(step: dict[str, Any]) -> str | None:
    schema = step.get("schema")
    table = step.get("table")
    if schema and table:
        return _table_key(schema, table)
    return None


def build_execution_graph(plan: dict[str, Any]) -> dict[str, Any]:
    plan_for_validation = dict(plan)
    plan_for_validation.pop("approval", None)
    plan_for_validation.pop("retry", None)
    plan_obj = validate_plan_document(plan_for_validation)
    steps = plan_obj.get("steps", [])
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str]] = set()
    schema_nodes: dict[str, str] = {}
    table_ddl_nodes: dict[str, str] = {}
    table_data_nodes: dict[str, str] = {}

    def add_edge(source: str | None, target: str | None, reason: str) -> None:
        if not source or not target or source == target:
            return
        key = (source, target, reason)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append({"from": source, "to": target, "reason": reason})

    for index, step in enumerate(steps):
        step_id = step.get("id") or f"step_{index + 1:04d}"
        op = step.get("op")
        schema = step.get("schema")
        table = step.get("table")
        table_key = _step_table_key(step)
        nodes.append(
            {
                "id": step_id,
                "index": index,
                "op": op,
                "schema": schema,
                "table": table,
                "table_key": table_key,
                "group": table_key or schema or "global",
            }
        )

        if index > 0:
            previous_id = steps[index - 1].get("id") or f"step_{index:04d}"
            add_edge(previous_id, step_id, "execution_order")

        if op == "ensure_schema" and schema:
            schema_nodes[schema] = step_id
            continue

        if schema:
            add_edge(schema_nodes.get(schema), step_id, "schema_exists")

        if op == "ensure_table" and table_key:
            table_ddl_nodes[table_key] = step_id
            continue

        if table_key and op in ("copy_table", "upsert_table"):
            add_edge(table_ddl_nodes.get(table_key), step_id, "table_exists")
            table_data_nodes[table_key] = step_id
            continue

        if table_key and op in (
            "sync_sequences",
            "create_indexes",
            "verify_table",
            "analyze_table",
            "vacuum_analyze_table",
        ):
            add_edge(table_data_nodes.get(table_key), step_id, "data_loaded")

        if op == "add_fks":
            for fk in step.get("fks", []):
                add_edge(
                    table_data_nodes.get(_table_key(fk.get("schema", schema), fk.get("table", ""))),
                    step_id,
                    "referenced_table_loaded",
                )

        if op == "apply_grants" and schema:
            for table_key_for_schema, data_step_id in table_data_nodes.items():
                if table_key_for_schema.startswith(f"{schema}."):
                    add_edge(data_step_id, step_id, "schema_objects_ready")

    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


def render_execution_graph(graph: dict[str, Any]) -> str:
    lines = [
        "Execution Graph",
        f"Nodes: {graph.get('node_count', 0)}",
        f"Edges: {graph.get('edge_count', 0)}",
        "",
        "Nodes:",
    ]
    for node in graph.get("nodes", []):
        target = node.get("table_key") or node.get("schema") or "-"
        lines.append(f"- {node.get('id')}: {node.get('op')} {target}")

    lines.extend(["", "Edges:"])
    if graph.get("edges"):
        for edge in graph.get("edges", []):
            lines.append(f"- {edge.get('from')} -> {edge.get('to')}: {edge.get('reason')}")
    else:
        lines.append("- none")

    return "\n".join(lines)


def build_retry_plan(
    plan: dict[str, Any],
    state: dict[str, Any],
    mode: str = "failed_only",
    table: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_for_validation = dict(plan)
    plan_for_validation.pop("approval", None)
    plan_for_validation.pop("retry", None)
    plan_obj = validate_plan_document(plan_for_validation)
    steps = plan_obj.get("steps", [])
    completed = state.get("completed", {})
    failed_step_ids = [
        step_id for step_id, payload in completed.items() if not payload.get("ok", False)
    ]
    step_index = {
        step.get("id") or f"step_{index + 1:04d}": index for index, step in enumerate(steps)
    }
    failed_indexes = [step_index[step_id] for step_id in failed_step_ids if step_id in step_index]
    selected_indexes: set[int] = set()
    selected_reason = ""

    if mode not in ("failed_only", "from_failed_step", "table"):
        raise ValueError("retry mode must be failed_only, from_failed_step, or table")

    if mode == "from_failed_step":
        if failed_indexes:
            first_failed = min(failed_indexes)
            selected_indexes = set(range(first_failed, len(steps)))
            selected_reason = f"from first failed step index {first_failed}"
    elif mode == "table":
        if not table:
            raise ValueError("table retry mode requires a schema-qualified table")
        selected_indexes = {
            index for index, step in enumerate(steps) if _step_table_key(step) == table
        }
        selected_reason = f"all steps for {table}"
    else:
        failed_tables = {
            _step_table_key(steps[index])
            for index in failed_indexes
            if _step_table_key(steps[index])
        }
        failed_tables.discard(None)
        for failed_table in failed_tables:
            table_indexes = [
                index for index, step in enumerate(steps) if _step_table_key(step) == failed_table
            ]
            failed_for_table = [
                index for index in failed_indexes if _step_table_key(steps[index]) == failed_table
            ]
            if failed_for_table:
                first_failed_for_table = min(failed_for_table)
                selected_indexes.update(
                    index for index in table_indexes if index >= first_failed_for_table
                )
        selected_reason = f"failed tables from failed step onward: {sorted(failed_tables)}"

    selected_schemas = {
        steps[index].get("schema") for index in selected_indexes if steps[index].get("schema")
    }
    selected_tables = {
        _step_table_key(steps[index]) for index in selected_indexes if _step_table_key(steps[index])
    }
    for index, step in enumerate(steps):
        if step.get("op") == "ensure_schema" and step.get("schema") in selected_schemas:
            selected_indexes.add(index)
        if step.get("op") == "ensure_table" and _step_table_key(step) in selected_tables:
            selected_indexes.add(index)

    retry_steps = [step for index, step in enumerate(steps) if index in selected_indexes]
    retry_plan = dict(plan_obj)
    if "approval" in plan:
        retry_plan["approval"] = plan["approval"]
    retry_plan["steps"] = retry_steps
    retry_plan["retry"] = {
        "mode": mode,
        "table": table,
        "source_failed_steps": failed_step_ids,
        "reason": selected_reason,
    }
    summary = {
        "mode": mode,
        "table": table,
        "source_failed_steps": failed_step_ids,
        "retry_step_count": len(retry_steps),
        "retry_step_ids": [step.get("id") for step in retry_steps],
        "reason": selected_reason,
        "ok": bool(retry_steps),
    }
    if not retry_steps:
        summary["blocked_reason"] = "No retryable steps matched the requested retry mode."
    return retry_plan, summary


def build_post_migration_summary(
    plan: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any] | None = None,
    pre_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    steps = plan.get("steps", [])
    completed = state.get("completed", {})
    failed_steps = [
        step_id for step_id, payload in completed.items() if not payload.get("ok", False)
    ]
    execution = PostMigrationExecutionOverview(
        total_steps=len(steps),
        completed_steps=len(completed),
        failed_steps=len(failed_steps),
        skipped_steps=max(len(steps) - len(completed), 0),
        success=not failed_steps and bool((report or {}).get("ok", True)),
    )

    inline_verify_results = [
        payload.get("verify")
        for payload in completed.values()
        if isinstance(payload, dict) and payload.get("verify")
    ]
    report_results = (report or {}).get("results", [])
    verification_results = report_results or inline_verify_results
    failed_tables = [
        _table_key(item.get("schema", ""), item.get("table", ""))
        for item in verification_results
        if not item.get("ok", False)
    ]
    tables_checked = int(report.get("tables_checked", 0)) if report else len(verification_results)
    verification = PostMigrationVerificationOverview(
        ok=bool((report or {}).get("ok", not failed_tables)),
        tables_checked=tables_checked,
        failed_tables=failed_tables,
    )

    residual_manual_review: list[str] = []
    if pre_summary:
        residual_manual_review = list(
            PreMigrationSummary.model_validate(pre_summary).manual_review_required
        )

    next_actions: list[str] = []
    if failed_steps:
        next_actions.append("Review failed steps in the state file before retrying.")
    if failed_tables:
        next_actions.append("Inspect verification mismatches before cutover.")
    if residual_manual_review:
        next_actions.append("Resolve outstanding manual-review items.")
    if not next_actions:
        next_actions.append(
            "Migration completed cleanly. Review the post-migration report and proceed to cutover."
        )

    return PostMigrationSummary(
        execution_overview=execution,
        verification_summary=verification,
        failed_steps=failed_steps,
        residual_manual_review=residual_manual_review,
        next_actions=next_actions,
        notes=[],
    ).model_dump(mode="python", by_alias=True)


def render_post_migration_summary(summary: dict[str, Any]) -> str:
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

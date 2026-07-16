from __future__ import annotations

import copy
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from amo.core.analysis import filter_plan_for_approval, validate_approval_strategies
from amo.core.integrity import ArtifactIntegrityError, verify_artifact
from amo.core.planners.models import MigrationManifest, validate_plan_document
from amo.core.workflow_models import ApprovalDocument, PreMigrationSummary


class ApprovalPolicyError(ValueError):
    """Raised when an approval cannot authorize an execution request."""


class ExecutableMetadataError(ApprovalPolicyError):
    """Raised when a plan references metadata not present in the approved manifest."""


@dataclass(frozen=True)
class ApprovedExecutionBundle:
    approval: ApprovalDocument
    validated_plan: dict[str, Any]
    filtered_plan: dict[str, Any]
    source_manifest: MigrationManifest
    pre_migration_summary: PreMigrationSummary
    plan_path: Path
    summary_path: Path
    source_manifest_path: Path

    @property
    def plan_sha256(self) -> str:
        return self.approval.plan.sha256

    def integrity_report(self) -> dict[str, Any]:
        return {
            "status": "validated",
            "artifacts": {
                "plan": {
                    "path": str(self.plan_path),
                    "sha256": self.approval.plan.sha256,
                },
                "summary": {
                    "path": str(self.summary_path),
                    "sha256": self.approval.summary.sha256,
                },
                "source_manifest": {
                    "path": str(self.source_manifest_path),
                    "sha256": self.approval.source_manifest.sha256,
                },
            },
        }

    def with_filtered_plan(self, plan: dict[str, Any]) -> ApprovedExecutionBundle:
        validated = validate_plan_document(plan)
        approved_steps = {step["id"]: step for step in self.filtered_plan.get("steps", [])}
        candidate_steps = validated.get("steps", [])
        candidate_ids = [step["id"] for step in candidate_steps]
        unknown_ids = sorted(set(candidate_ids) - set(approved_steps))
        if unknown_ids:
            raise ApprovalPolicyError(
                "Retry plan broadens the approved execution scope with step(s): "
                + ", ".join(unknown_ids)
            )
        changed_ids = [
            step["id"] for step in candidate_steps if step != approved_steps.get(step["id"])
        ]
        if changed_ids:
            raise ApprovalPolicyError(
                "Retry plan changes approved step payload(s): " + ", ".join(changed_ids)
            )
        approved_order = {
            step["id"]: index for index, step in enumerate(self.filtered_plan["steps"])
        }
        candidate_order = [approved_order[step_id] for step_id in candidate_ids]
        if candidate_order != sorted(candidate_order):
            raise ApprovalPolicyError("Retry plan changes the approved execution order.")
        return replace(self, filtered_plan=validated)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _grant_key(grant: dict[str, Any], default_schema: str | None = None) -> tuple[str, ...]:
    return (
        str(grant.get("object_type") or ""),
        str(grant.get("schema") or default_schema or ""),
        str(grant.get("object_name") or ""),
        str(grant.get("grantee") or ""),
        str(grant.get("privilege_type") or ""),
    )


def _hydrate_named_items(
    *,
    requested: list[dict[str, Any]],
    trusted: list[dict[str, Any]],
    name_field: str,
    definition_field: str | None,
    label: str,
) -> list[dict[str, Any]]:
    by_name = {
        str(item.get(name_field)): item for item in trusted if item.get(name_field) is not None
    }
    hydrated: list[dict[str, Any]] = []
    for item in requested:
        trusted_item = by_name.get(str(item.get(name_field)))
        if trusted_item is None and definition_field and item.get(definition_field):
            trusted_item = next(
                (
                    candidate
                    for candidate in trusted
                    if candidate.get(definition_field) == item.get(definition_field)
                ),
                None,
            )
        if trusted_item is None:
            identity = item.get(name_field) or item.get(definition_field) or "unknown"
            raise ExecutableMetadataError(f"Plan references unknown {label}: {identity}")
        hydrated.append(copy.deepcopy(trusted_item))
    return hydrated


def hydrate_executable_metadata(
    plan: dict[str, Any], manifest: MigrationManifest
) -> dict[str, Any]:
    """Replace executable metadata in a planner proposal with approved manifest values."""

    hydrated_plan = copy.deepcopy(plan)
    table_map = {_table_key(table.schema_name, table.table): table for table in manifest.tables}
    manifest_schemas = set(manifest.include_schemas)
    manifest_schemas.update(table.schema_name for table in manifest.tables)
    manifest_schemas.update(view.schema_name for view in manifest.matviews)
    manifest_schemas.update(udf.schema_name for udf in manifest.udfs)
    manifest_schemas.update(manifest.schema_grants)

    udf_map = {
        _table_key(udf.schema_name, udf.name): udf.model_dump(mode="python", by_alias=True)
        for udf in manifest.udfs
    }
    matview_map = {
        _table_key(view.schema_name, view.name): view.model_dump(mode="python", by_alias=True)
        for view in manifest.matviews
    }
    matview_indexes_by_schema: dict[str, list[dict[str, Any]]] = {}
    for index in manifest.matview_indexes:
        matview_indexes_by_schema.setdefault(index.schema_name, []).append(
            {
                "index_name": index.index_name,
                "index_definition": index.index_definition,
                "cluster_statement": index.cluster_statement,
            }
        )

    trusted_grants: dict[tuple[str, ...], dict[str, Any]] = {}
    for schema, grants in manifest.schema_grants.items():
        for grant in grants:
            dumped = grant.model_dump(mode="python")
            trusted_grants[_grant_key(dumped, schema)] = dumped
    for table in manifest.tables:
        for grant in table.grants:
            dumped = grant.model_dump(mode="python")
            trusted_grants[_grant_key(dumped, table.schema_name)] = dumped
    for view in manifest.matviews:
        for grant in view.grants:
            dumped = grant.model_dump(mode="python")
            trusted_grants[_grant_key(dumped, view.schema_name)] = dumped

    table_ops = {
        "ensure_table",
        "copy_table",
        "upsert_table",
        "sync_sequences",
        "create_indexes",
        "verify_table",
        "analyze_table",
        "vacuum_analyze_table",
    }

    new_steps: list[dict[str, Any]] = []
    for step in hydrated_plan.get("steps", []):
        hydrated_step = copy.deepcopy(step)
        op = hydrated_step.get("op")
        schema = hydrated_step.get("schema")
        table = hydrated_step.get("table")

        if not schema or schema not in manifest_schemas:
            raise ExecutableMetadataError(f"Plan references unknown source schema: {schema}")

        if op in table_ops:
            key = _table_key(schema, table or "")
            if key not in table_map:
                raise ExecutableMetadataError(f"Plan references unknown source table: {key}")
            source_table = table_map[key]
            hydrated_step["has_geometry"] = source_table.has_geometry
            hydrated_step["geometry_columns"] = list(source_table.geometry_columns)
            hydrated_step["primary_key"] = list(source_table.primary_key)
            transfer = copy.deepcopy(hydrated_step.get("transfer") or {})
            transfer.pop("load_strategy", None)
            transfer.pop("conflict_key", None)
            chunk_column = transfer.get("chunk_column")
            source_columns = {column.name for column in source_table.columns}
            if chunk_column and chunk_column not in source_columns:
                raise ExecutableMetadataError(
                    f"Plan references unknown chunk column on {key}: {chunk_column}"
                )
            hydrated_step["transfer"] = transfer
            hydrated_step["conflict_key"] = []

        if op == "create_udfs":
            trusted: list[dict[str, Any]] = []
            for item in hydrated_step.get("udfs", []):
                item_schema = item.get("schema") or schema
                key = _table_key(item_schema, item.get("name") or "")
                if key not in udf_map:
                    raise ExecutableMetadataError(f"Plan references unknown source UDF: {key}")
                trusted.append(copy.deepcopy(udf_map[key]))
            hydrated_step["udfs"] = trusted

        elif op == "create_indexes":
            source_table = table_map[_table_key(schema, table)]
            trusted_indexes = [item.model_dump(mode="python") for item in source_table.indexes]
            hydrated_step["indexes"] = _hydrate_named_items(
                requested=hydrated_step.get("indexes", []),
                trusted=trusted_indexes,
                name_field="index_name",
                definition_field="index_definition",
                label=f"index on {schema}.{table}",
            )

        elif op == "create_mv_indexes":
            hydrated_step["indexes"] = _hydrate_named_items(
                requested=hydrated_step.get("indexes", []),
                trusted=matview_indexes_by_schema.get(schema, []),
                name_field="index_name",
                definition_field="index_definition",
                label=f"materialized-view index in {schema}",
            )

        elif op == "add_fks":
            trusted_fks: list[dict[str, Any]] = []
            for foreign_key in hydrated_step.get("fks", []):
                fk_schema = foreign_key.get("schema") or schema
                fk_table = foreign_key.get("table")
                source_table = table_map.get(_table_key(fk_schema, fk_table or ""))
                if source_table is None:
                    raise ExecutableMetadataError(
                        f"Plan references foreign key on unknown table: {fk_schema}.{fk_table}"
                    )
                source_fk = next(
                    (
                        item
                        for item in source_table.foreign_keys
                        if item.name == foreign_key.get("name")
                    ),
                    None,
                )
                if source_fk is None:
                    raise ExecutableMetadataError(
                        f"Plan references unknown foreign key: {fk_schema}.{fk_table}."
                        f"{foreign_key.get('name')}"
                    )
                trusted_fks.append(
                    {
                        "schema": fk_schema,
                        "table": fk_table,
                        **source_fk.model_dump(mode="python"),
                    }
                )
            hydrated_step["fks"] = trusted_fks

        elif op in ("create_matviews", "stage_matviews"):
            trusted_views: list[dict[str, Any]] = []
            for view in hydrated_step.get("matviews", []):
                view_schema = view.get("schema") or schema
                key = _table_key(view_schema, view.get("name") or "")
                if key not in matview_map:
                    raise ExecutableMetadataError(
                        f"Plan references unknown materialized view: {key}"
                    )
                trusted_view = copy.deepcopy(matview_map[key])
                if view.get("strategy") == "staged_rebuild":
                    trusted_view["strategy"] = "staged_rebuild"
                    trusted_view["staging_table"] = f"qa_{trusted_view['name']}_staging"
                trusted_views.append(trusted_view)
            hydrated_step["matviews"] = trusted_views

        elif op == "apply_grants":
            hydrated_grants: list[dict[str, Any]] = []
            for grant in hydrated_step.get("grants", []):
                key = _grant_key(grant, schema)
                trusted_grant = trusted_grants.get(key)
                if trusted_grant is None:
                    raise ExecutableMetadataError(
                        "Plan references a grant that is not present in the approved source manifest: "
                        f"{'.'.join(key)}"
                    )
                hydrated_grants.append(copy.deepcopy(trusted_grant))
            hydrated_step["grants"] = hydrated_grants

        new_steps.append(hydrated_step)

    hydrated_plan["steps"] = new_steps
    return validate_plan_document(hydrated_plan)


def _validate_summary_scope(
    summary: PreMigrationSummary,
    approval: ApprovalDocument,
    manifest: MigrationManifest,
) -> None:
    overlap = sorted(set(approval.included_tables) & set(approval.excluded_tables))
    if overlap:
        raise ApprovalPolicyError(
            "Approval cannot include and exclude the same table(s): " + ", ".join(overlap)
        )
    manifest_tables = {_table_key(table.schema_name, table.table) for table in manifest.tables}
    summary_tables = {
        _table_key(item.schema_name, item.table) for item in summary.table_recommendations
    }
    unknown_summary_tables = sorted(summary_tables - manifest_tables)
    if unknown_summary_tables:
        raise ApprovalPolicyError(
            "Pre-migration summary references tables absent from the approved source manifest: "
            + ", ".join(unknown_summary_tables)
        )
    approved_tables = set(approval.included_tables) | set(approval.excluded_tables)
    approved_tables.update(approval.approved_manual_review_items)
    approved_tables.update(approval.table_strategies)
    unknown_approval_tables = sorted(approved_tables - summary_tables)
    if unknown_approval_tables:
        raise ApprovalPolicyError(
            "Approval references tables absent from the approved summary: "
            + ", ".join(unknown_approval_tables)
        )


def build_approved_execution_bundle(
    *,
    approval_path: str | Path,
    plan_path: str | Path | None = None,
    summary_path: str | Path | None = None,
    source_manifest_path: str | Path | None = None,
) -> ApprovedExecutionBundle:
    approval_file = Path(approval_path)
    try:
        raw_approval = _read_json(approval_file)
    except (OSError, json.JSONDecodeError) as exc:
        raise ApprovalPolicyError(f"Approval artifact cannot be read: {exc}") from exc
    if raw_approval.get("schema_version") != "2":
        raise ApprovalPolicyError(
            "Approval artifact predates schema v2 integrity binding and cannot execute. "
            "Regenerate approval after reviewing the plan, summary, and source manifest."
        )
    try:
        approval = ApprovalDocument.model_validate(raw_approval)
    except ValidationError as exc:
        raise ApprovalPolicyError(f"Approval artifact is invalid: {exc}") from exc

    try:
        verified_plan_path = verify_artifact(approval.plan, override_path=plan_path, label="plan")
        verified_summary_path = verify_artifact(
            approval.summary, override_path=summary_path, label="summary"
        )
        verified_manifest_path = verify_artifact(
            approval.source_manifest,
            override_path=source_manifest_path,
            label="source manifest",
        )
    except ArtifactIntegrityError as exc:
        raise ApprovalPolicyError(str(exc)) from exc

    try:
        validated_plan = validate_plan_document(_read_json(verified_plan_path))
        source_manifest = MigrationManifest.model_validate(_read_json(verified_manifest_path))
        summary = PreMigrationSummary.model_validate(_read_json(verified_summary_path))
    except (ValidationError, OSError, json.JSONDecodeError) as exc:
        raise ApprovalPolicyError(f"Approved artifact schema validation failed: {exc}") from exc
    _validate_summary_scope(summary, approval, source_manifest)

    strategy_errors = validate_approval_strategies(
        summary.model_dump(mode="python", by_alias=True),
        approval.model_dump(mode="python", by_alias=True),
    )
    if strategy_errors:
        reasons = "; ".join(
            f"{item.get('table', '-')}: {item.get('reason', 'invalid strategy')}"
            for item in strategy_errors
        )
        raise ApprovalPolicyError(f"Approval strategy validation failed: {reasons}")

    hydrated_plan = hydrate_executable_metadata(validated_plan, source_manifest)
    try:
        filtered_plan = filter_plan_for_approval(
            plan=hydrated_plan,
            summary=summary.model_dump(mode="python", by_alias=True),
            approval=approval.model_dump(mode="python", by_alias=True),
        )
        filtered_plan = validate_plan_document(filtered_plan)
    except ValidationError as exc:
        raise ApprovalPolicyError(f"Approved plan filtering failed validation: {exc}") from exc

    return ApprovedExecutionBundle(
        approval=approval,
        validated_plan=hydrated_plan,
        filtered_plan=filtered_plan,
        source_manifest=source_manifest,
        pre_migration_summary=summary,
        plan_path=verified_plan_path,
        summary_path=verified_summary_path,
        source_manifest_path=verified_manifest_path,
    )

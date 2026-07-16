from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MigrationMode = Literal[
    "full_refresh",
    "missing_only",
    "metadata_diff_only",
    "data_diff_only",
    "safe_sync",
    "plan_only",
]

TableDiffStatus = Literal[
    "missing_in_target",
    "missing_in_source",
    "metadata_match",
    "metadata_diff",
]

TableAction = Literal[
    "copy",
    "sync_metadata",
    "manual_review",
    "skip",
]

LoadStrategy = Literal[
    "append_only",
    "upsert",
    "truncate_reload",
    "skip",
]

KeyReadinessStatus = Literal[
    "primary_key",
    "matching_unique_key",
    "source_only_key",
    "target_only_key",
    "no_key",
    "ambiguous_key",
]

TransferStrategy = Literal[
    "full_copy",
    "partition_wise_copy",
    "chunked_copy",
]

VerificationDepth = Literal[
    "rowcount",
    "rowcount_and_sample_hash",
]

RiskLevel = Literal["low", "medium", "high"]


class ManifestDiffSummary(BaseModel):
    source_tables: int = 0
    target_tables: int = 0
    missing_in_target: int = 0
    missing_in_source: int = 0
    metadata_match: int = 0
    metadata_diff: int = 0


class TableDiffEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    status: TableDiffStatus
    structure_compatible: bool = False
    source_present: bool = True
    target_present: bool = False
    source_estimated_rows: int | None = None
    target_estimated_rows: int | None = None
    structural_drift: list[str] = Field(default_factory=list)
    auxiliary_drift: list[str] = Field(default_factory=list)


class ManifestDiffDocument(BaseModel):
    source_role: str = "source"
    target_role: str = "target"
    summary: ManifestDiffSummary
    tables: list[TableDiffEntry] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TableRecommendation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    diff_status: TableDiffStatus
    action: TableAction
    transfer_strategy: TransferStrategy
    chunk_column: str | None = None
    chunk_count: int = 1
    concurrency_hint: int = 1
    verification_depth: VerificationDepth = "rowcount"
    risk_score: int = 0
    risk_level: RiskLevel = "low"
    warnings: list[str] = Field(default_factory=list)
    manual_review_required: bool = False
    rationale: str = ""
    key_readiness: KeyReadinessStatus = "no_key"
    conflict_key: list[str] = Field(default_factory=list)
    upsert_eligible: bool = False


class PreMigrationOverview(BaseModel):
    mode: MigrationMode
    planner: str
    source_tables: int = 0
    target_tables: int = 0
    tables_to_copy: int = 0
    tables_to_sync_metadata: int = 0
    manual_review_count: int = 0
    skipped_tables: int = 0


class PreMigrationSummary(BaseModel):
    overview: PreMigrationOverview
    drift_summary: ManifestDiffSummary
    preflight_warnings: list[str] = Field(default_factory=list)
    manual_review_required: list[str] = Field(default_factory=list)
    destructive_actions: list[str] = Field(default_factory=list)
    table_recommendations: list[TableRecommendation] = Field(default_factory=list)
    planner_recommendation: str = ""


class TableStrategy(BaseModel):
    strategy: LoadStrategy
    conflict_key: list[str] = Field(default_factory=list)
    staging_schema: str | None = None
    preserve_target_only_columns: bool = True

    @field_validator("conflict_key")
    @classmethod
    def reject_empty_conflict_key_parts(cls, value: list[str]) -> list[str]:
        return [part for part in value if part]


class ArtifactDigest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    sha256: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("artifact path cannot be empty")
        return value

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        normalized = value.lower()
        if not re.fullmatch(r"[0-9a-f]{64}", normalized):
            raise ValueError("sha256 must be a 64-character lowercase hexadecimal digest")
        return normalized


class ApprovalDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["2"] = "2"
    approved_mode: MigrationMode
    approved_at: str
    approved_by: str = "manual"
    plan: ArtifactDigest
    summary: ArtifactDigest
    source_manifest: ArtifactDigest
    allow_destructive: bool = False
    included_tables: list[str] = Field(default_factory=list)
    excluded_tables: list[str] = Field(default_factory=list)
    approved_manual_review_items: list[str] = Field(default_factory=list)
    table_strategies: dict[str, TableStrategy] = Field(default_factory=dict)
    notes: str | None = None

    @property
    def plan_path(self) -> str:
        return self.plan.path

    @property
    def summary_path(self) -> str:
        return self.summary.path

    @property
    def source_manifest_path(self) -> str:
        return self.source_manifest.path


class PostMigrationExecutionOverview(BaseModel):
    total_steps: int = 0
    completed_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    success: bool = False


class PostMigrationVerificationOverview(BaseModel):
    ok: bool = False
    tables_checked: int = 0
    failed_tables: list[str] = Field(default_factory=list)


class PostMigrationSummary(BaseModel):
    execution_overview: PostMigrationExecutionOverview
    verification_summary: PostMigrationVerificationOverview
    failed_steps: list[str] = Field(default_factory=list)
    residual_manual_review: list[str] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


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
    source_estimated_rows: Optional[int] = None
    target_estimated_rows: Optional[int] = None
    structural_drift: List[str] = Field(default_factory=list)
    auxiliary_drift: List[str] = Field(default_factory=list)


class ManifestDiffDocument(BaseModel):
    source_role: str = "source"
    target_role: str = "target"
    summary: ManifestDiffSummary
    tables: List[TableDiffEntry] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class TableRecommendation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    diff_status: TableDiffStatus
    action: TableAction
    transfer_strategy: TransferStrategy
    chunk_column: Optional[str] = None
    chunk_count: int = 1
    concurrency_hint: int = 1
    verification_depth: VerificationDepth = "rowcount"
    risk_score: int = 0
    risk_level: RiskLevel = "low"
    warnings: List[str] = Field(default_factory=list)
    manual_review_required: bool = False
    rationale: str = ""


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
    preflight_warnings: List[str] = Field(default_factory=list)
    manual_review_required: List[str] = Field(default_factory=list)
    destructive_actions: List[str] = Field(default_factory=list)
    table_recommendations: List[TableRecommendation] = Field(default_factory=list)
    planner_recommendation: str = ""


class ApprovalDocument(BaseModel):
    approved_mode: MigrationMode
    approved_at: str
    approved_by: str = "manual"
    plan_path: str
    summary_path: str
    allow_destructive: bool = False
    included_tables: List[str] = Field(default_factory=list)
    excluded_tables: List[str] = Field(default_factory=list)
    approved_manual_review_items: List[str] = Field(default_factory=list)
    notes: Optional[str] = None


class PostMigrationExecutionOverview(BaseModel):
    total_steps: int = 0
    completed_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    success: bool = False


class PostMigrationVerificationOverview(BaseModel):
    ok: bool = False
    tables_checked: int = 0
    failed_tables: List[str] = Field(default_factory=list)


class PostMigrationSummary(BaseModel):
    execution_overview: PostMigrationExecutionOverview
    verification_summary: PostMigrationVerificationOverview
    failed_steps: List[str] = Field(default_factory=list)
    residual_manual_review: List[str] = Field(default_factory=list)
    next_actions: List[str] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)

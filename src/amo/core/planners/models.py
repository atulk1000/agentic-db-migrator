from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceRef(BaseModel):
    host: str | None = None
    port: int | None = None
    database: str | None = None


class ManifestColumn(BaseModel):
    name: str
    type_sql: str | None = None
    udt_name: str | None = None
    not_null: bool = False
    attidentity: str = ""
    default_sql: str | None = None
    nextval_sequences: list[str] = Field(default_factory=list)
    geometry_type: str | None = None
    geometry_srid: int | None = None


class PartitionChild(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    bound: str | None = None


class PartitionInfo(BaseModel):
    is_partition_parent: bool = False
    partition_key: str | None = None
    children: list[PartitionChild] = Field(default_factory=list)


class ForeignKeyInfo(BaseModel):
    name: str
    definition: str
    ref_schema: str | None = None
    ref_table: str | None = None


class IndexInfo(BaseModel):
    index_name: str | None = None
    index_definition: str
    cluster_statement: str | None = None


class GrantInfo(BaseModel):
    grantee: str
    privilege_type: str
    object_type: str | None = None
    schema: str | None = None
    object_name: str | None = None


class ManifestTable(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    primary_key: list[str] = Field(default_factory=list)
    has_geometry: bool = False
    geometry_columns: list[str] = Field(default_factory=list)
    columns: list[ManifestColumn] = Field(default_factory=list)
    partition: PartitionInfo = Field(default_factory=PartitionInfo)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    grants: list[GrantInfo] = Field(default_factory=list)


class MaterializedViewInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    name: str
    definition: str
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    has_geometry: bool = False
    geometry_columns: list[str] = Field(default_factory=list)
    strategy: str | None = None
    staging_table: str | None = None
    grants: list[GrantInfo] = Field(default_factory=list)


class MaterializedViewIndexInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    matview: str
    index_name: str | None = None
    index_definition: str
    cluster_statement: str | None = None


class UdfInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    name: str
    create_statement: str


class MigrationManifest(BaseModel):
    version: str = "v2"
    generated_at: str | None = None
    source: SourceRef = Field(default_factory=SourceRef)
    include_schemas: list[str] = Field(default_factory=list)
    tables: list[ManifestTable] = Field(default_factory=list)
    matviews: list[MaterializedViewInfo] = Field(default_factory=list)
    matview_indexes: list[MaterializedViewIndexInfo] = Field(default_factory=list)
    udfs: list[UdfInfo] = Field(default_factory=list)
    schema_grants: dict[str, list[GrantInfo]] = Field(default_factory=dict)
    errors: list[dict[str, Any]] = Field(default_factory=list)


PlanOp = Literal[
    "ensure_schema",
    "create_udfs",
    "ensure_table",
    "copy_table",
    "sync_sequences",
    "create_indexes",
    "add_fks",
    "create_matviews",
    "stage_matviews",
    "create_mv_indexes",
    "apply_grants",
    "verify_table",
    "analyze_table",
    "vacuum_analyze_table",
]


class PlanStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    op: PlanOp
    schema_name: str = Field(alias="schema")
    table: str | None = None
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    has_geometry: bool = False
    geometry_columns: list[str] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    validation: dict[str, Any] = Field(default_factory=dict, alias="validate")
    indexes: list[IndexInfo] = Field(default_factory=list)
    fks: list[dict[str, Any]] = Field(default_factory=list)
    matviews: list[MaterializedViewInfo] = Field(default_factory=list)
    udfs: list[UdfInfo] = Field(default_factory=list)
    grants: list[GrantInfo] = Field(default_factory=list)
    transfer: dict[str, Any] = Field(default_factory=dict)
    maintenance: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_step_shape(self) -> PlanStep:
        table_required_ops = {
            "ensure_table",
            "copy_table",
            "sync_sequences",
            "create_indexes",
            "verify_table",
            "analyze_table",
            "vacuum_analyze_table",
        }

        if self.op in table_required_ops and not self.table:
            raise ValueError(f"{self.op} requires a table value")

        if self.op == "verify_table" and "rowcount" not in self.validation:
            raise ValueError("verify_table requires validate.rowcount")

        if self.op == "add_fks" and not self.fks:
            raise ValueError("add_fks requires at least one foreign key entry")

        if self.op == "create_udfs" and not self.udfs:
            raise ValueError("create_udfs requires at least one udf")

        if self.op == "create_matviews" and not self.matviews:
            raise ValueError("create_matviews requires at least one materialized view")

        if self.op == "stage_matviews" and not self.matviews:
            raise ValueError("stage_matviews requires at least one materialized view")

        if self.op == "apply_grants" and not self.grants:
            raise ValueError("apply_grants requires at least one grant entry")

        return self


class MigrationPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "v2"
    generated_at: str | None = None
    planner: str
    strategy: str
    source: SourceRef = Field(default_factory=SourceRef)
    steps: list[PlanStep] = Field(default_factory=list)
    planner_metadata: dict[str, Any] = Field(default_factory=dict)


def load_manifest_document(path: str | Path) -> MigrationManifest:
    return MigrationManifest.model_validate(json.loads(Path(path).read_text()))


def validate_plan_document(plan: dict[str, Any]) -> dict[str, Any]:
    return MigrationPlan.model_validate(plan).model_dump(mode="python", by_alias=True)

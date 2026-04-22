from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceRef(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None


class ManifestColumn(BaseModel):
    name: str
    type_sql: Optional[str] = None
    udt_name: Optional[str] = None
    not_null: bool = False
    attidentity: str = ""
    default_sql: Optional[str] = None
    nextval_sequences: List[str] = Field(default_factory=list)
    geometry_type: Optional[str] = None
    geometry_srid: Optional[int] = None


class PartitionChild(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    bound: Optional[str] = None


class PartitionInfo(BaseModel):
    is_partition_parent: bool = False
    partition_key: Optional[str] = None
    children: List[PartitionChild] = Field(default_factory=list)


class ForeignKeyInfo(BaseModel):
    name: str
    definition: str
    ref_schema: Optional[str] = None
    ref_table: Optional[str] = None


class IndexInfo(BaseModel):
    index_name: Optional[str] = None
    index_definition: str
    cluster_statement: Optional[str] = None


class GrantInfo(BaseModel):
    grantee: str
    privilege_type: str
    object_type: Optional[str] = None
    schema: Optional[str] = None
    object_name: Optional[str] = None


class ManifestTable(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    estimated_rows: Optional[int] = None
    estimated_bytes: Optional[int] = None
    primary_key: List[str] = Field(default_factory=list)
    has_geometry: bool = False
    geometry_columns: List[str] = Field(default_factory=list)
    columns: List[ManifestColumn] = Field(default_factory=list)
    partition: PartitionInfo = Field(default_factory=PartitionInfo)
    foreign_keys: List[ForeignKeyInfo] = Field(default_factory=list)
    indexes: List[IndexInfo] = Field(default_factory=list)
    grants: List[GrantInfo] = Field(default_factory=list)


class MaterializedViewInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    name: str
    definition: str
    estimated_rows: Optional[int] = None
    estimated_bytes: Optional[int] = None
    has_geometry: bool = False
    geometry_columns: List[str] = Field(default_factory=list)
    strategy: Optional[str] = None
    staging_table: Optional[str] = None
    grants: List[GrantInfo] = Field(default_factory=list)


class MaterializedViewIndexInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    matview: str
    index_name: Optional[str] = None
    index_definition: str
    cluster_statement: Optional[str] = None


class UdfInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    name: str
    create_statement: str


class MigrationManifest(BaseModel):
    version: str = "v2"
    generated_at: Optional[str] = None
    source: SourceRef = Field(default_factory=SourceRef)
    include_schemas: List[str] = Field(default_factory=list)
    tables: List[ManifestTable] = Field(default_factory=list)
    matviews: List[MaterializedViewInfo] = Field(default_factory=list)
    matview_indexes: List[MaterializedViewIndexInfo] = Field(default_factory=list)
    udfs: List[UdfInfo] = Field(default_factory=list)
    schema_grants: Dict[str, List[GrantInfo]] = Field(default_factory=dict)
    errors: List[Dict[str, Any]] = Field(default_factory=list)


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
    table: Optional[str] = None
    estimated_rows: Optional[int] = None
    estimated_bytes: Optional[int] = None
    has_geometry: bool = False
    geometry_columns: List[str] = Field(default_factory=list)
    primary_key: List[str] = Field(default_factory=list)
    validation: Dict[str, Any] = Field(default_factory=dict, alias="validate")
    indexes: List[IndexInfo] = Field(default_factory=list)
    fks: List[Dict[str, Any]] = Field(default_factory=list)
    matviews: List[MaterializedViewInfo] = Field(default_factory=list)
    udfs: List[UdfInfo] = Field(default_factory=list)
    grants: List[GrantInfo] = Field(default_factory=list)
    transfer: Dict[str, Any] = Field(default_factory=dict)
    maintenance: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_step_shape(self) -> "PlanStep":
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
    generated_at: Optional[str] = None
    planner: str
    strategy: str
    source: SourceRef = Field(default_factory=SourceRef)
    steps: List[PlanStep] = Field(default_factory=list)
    planner_metadata: Dict[str, Any] = Field(default_factory=dict)


def load_manifest_document(path: str | Path) -> MigrationManifest:
    return MigrationManifest.model_validate(json.loads(Path(path).read_text()))


def validate_plan_document(plan: Dict[str, Any]) -> Dict[str, Any]:
    return MigrationPlan.model_validate(plan).model_dump(mode="python", by_alias=True)

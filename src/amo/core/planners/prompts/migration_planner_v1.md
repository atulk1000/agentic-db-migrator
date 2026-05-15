You are planning a PostgreSQL migration.

Return ONLY JSON. Do not use markdown. Do not include commentary outside the JSON object.

Objective:
{objective}

Planner responsibilities:
- choose table ordering
- choose transfer strategy hints
- choose verification depth
- preserve partitioned tables and parent/child relationships
- route destructive or ambiguous changes to manual review through plan metadata and conservative operation choices
- flag risky materialized views, UDFs, foreign keys, and partition layouts through planner_metadata

Hard safety rules:
- The executor only supports allowlisted operations.
- Do not invent SQL for direct execution.
- Do not invent table names, schema names, operation names, or fields.
- If unsure, prefer fewer safe steps over speculative steps.

The plan MUST use exactly these top-level fields:
version, generated_at, planner, strategy, source, planner_metadata, steps.

Each step MUST use only these fields:
id, op, schema, table, estimated_rows, estimated_bytes, has_geometry, primary_key, validate, indexes, fks, matviews, udfs, transfer, maintenance, grants, geometry_columns.

Do NOT use name, mode, fk_name, notes, commentary, rationale, sql, query, or any other extra keys inside steps.

Allowed op values:
ensure_schema, create_udfs, ensure_table, copy_table, sync_sequences, create_indexes, add_fks, apply_grants, stage_matviews, create_matviews, create_mv_indexes, verify_table, analyze_table, vacuum_analyze_table.

Rules by op:
- ensure_schema: schema required, table must be null.
- create_udfs: schema required, udfs non-empty, table must be null.
- ensure_table: schema and table required.
- copy_table: schema and table required.
- sync_sequences: schema and table required. table is always a table name, never a sequence name.
- create_indexes: schema and table required, indexes non-empty.
- add_fks: schema required, fks non-empty, table must be null.
- apply_grants: schema required, grants non-empty, table must be null.
- stage_matviews: schema required, matviews non-empty, table must be null.
- create_matviews: schema required, matviews non-empty, table must be null.
- create_mv_indexes: schema required, indexes non-empty, table must be null.
- verify_table: schema and table required, validate must contain rowcount:boolean and may contain sample_hash:boolean and sample_rows:int.
- analyze_table and vacuum_analyze_table: schema and table required.

Allowed validate values:
- {{"rowcount": true, "sample_hash": false, "sample_rows": 50}}
- {{"rowcount": true, "sample_hash": true, "sample_rows": 50}}

Use only schema.table values from this list:
{allowed_tables}

Prefer this high-level order:
ensure_schema, create_udfs, ensure_table, copy_table, sync_sequences, create_indexes, verify_table, analyze_table, add_fks, apply_grants, stage_matviews, create_matviews, create_mv_indexes.

For small or risky tables, set sample_hash true. For larger low-risk tables, rowcount-only verification is acceptable.

Manifest JSON:
{manifest_json}

Planning context JSON:
{planning_context_json}

# PRD: Core Migration Engine Upgrade v0.2.0

## Goal

Upgrade Agentic DB Migration Orchestrator from an approval-gated table copy workflow into a more production-realistic migration control plane with table-level load strategies, safe upsert execution, stronger preflight validation, dry-run previews, resumable retries, dependency-aware execution, and structured failure diagnosis.

This release keeps the existing safety boundary:

> The planner recommends, the human approves, and deterministic code executes only validated operations.

## Release Version

Planned release: `v0.2.0`

Current package baseline: `0.1.0`

This is a minor-version release because it adds new migration capabilities and approval semantics while preserving the existing CLI, Streamlit, and artifact-first workflow.

## Selected Updates

This PRD covers 7 core updates:

1. Per-table load strategy selection
2. Staging-table upsert engine
3. Primary-key and unique-key readiness checks
4. Dry-run execution preview
5. Retry and resume for failed tables
6. Dependency-aware execution graph
7. Structured failure classification

## Problem

The current system is strong for safe, reviewable Postgres-to-Postgres migration planning, but its table copy behavior is still too coarse for real environment refreshes.

Today, approval can scope which tables run, and destructive behavior is guarded globally. However, real migrations often need mixed behavior:

- truncate and reload reference tables
- upsert dimension or business tables
- append event/audit tables
- skip sensitive or manually managed tables
- retry only failed tables after a partial run

The current copy path cannot express those choices per table, and it does not provide a true upsert mode. It also needs a clearer dry-run preview, stronger key-readiness checks, richer failure labels, and a more explicit execution dependency model.

## Target Users

- Data engineers refreshing QA, UAT, or staging environments from production-like sources
- Developers reviewing migration plans before execution
- Platform teams evaluating safe AI-assisted database operations
- Hiring managers or technical reviewers inspecting architecture depth
- AI assistants using the repo through CLI, Streamlit, or MCP review tools

## Non-Goals

- Do not add arbitrary SQL execution.
- Do not let an LLM directly choose destructive behavior without deterministic validation and human approval.
- Do not remove the existing approval-gated workflow.
- Do not make migrations fully autonomous.
- Do not require OpenAI, Gemini, or any hosted planner for these features.
- Do not support non-Postgres dialects in this release.
- Do not implement bidirectional sync.

## Product Principles

- Table strategy is an approval decision, not only a config setting.
- Upsert requires a trustworthy conflict key.
- Destructive behavior must be explicit at both global and table levels.
- Dry-run output should be reviewable by humans and machines.
- Retry should reuse state instead of rerunning completed work blindly.
- Execution order should be explainable as a graph of dependencies.
- Failure reports should suggest the next useful operator action.

## Current Behavior

The current approval artifact supports:

- approved migration mode
- global `allow_destructive`
- included tables
- excluded tables
- approved manual-review items

The current executor supports:

- `copy_table`
- optional target `TRUNCATE TABLE ... CASCADE` when destructive execution is allowed
- append-style copy when truncation is disabled
- Spark JDBC append for the scaffolded large-table path

The current model does not support:

- table-level load strategies
- true `upsert`
- key-readiness validation for upsert
- dry-run plan rendering
- retry-only-failed-table execution
- dependency graph visualization or validation
- structured failure classes

## Proposed Capabilities

### 1. Per-Table Load Strategy Selection

Add table-level load strategies to the approval workflow.

Initial strategies:

| Strategy | Meaning | Destructive | Requires Key |
| --- | --- | ---: | ---: |
| `append_only` | Insert source rows into target without deleting or updating existing rows. | No | No |
| `upsert` | Insert new rows and update existing rows using a primary key or approved unique key. | No | Yes |
| `truncate_reload` | Truncate target table, then reload from source. | Yes | No |
| `skip` | Explicitly do not run data movement for this table. | No | No |

Future strategy:

| Strategy | Meaning |
| --- | --- |
| `replace_partition` | Refresh selected partitions instead of a whole table. |

Approval artifact addition:

```json
{
  "table_strategies": {
    "public.users": {
      "strategy": "upsert",
      "conflict_key": ["id"]
    },
    "public.orders": {
      "strategy": "truncate_reload"
    },
    "public.audit_log": {
      "strategy": "append_only"
    }
  }
}
```

CLI approval addition:

```powershell
amo approve `
  --table-strategy public.users=upsert `
  --table-strategy public.orders=truncate_reload `
  --table-strategy public.audit_log=append_only
```

Streamlit addition:

- Add a strategy selector per approved table.
- Show whether each table is key-ready.
- Disable `upsert` when no safe key exists unless a user explicitly approves a unique key.
- Mark `truncate_reload` as destructive and require destructive approval.

### 2. Staging-Table Upsert Engine

Implement upsert through a staging table instead of writing directly into the final target table.

Execution flow:

```text
create staging table
copy source rows into staging table
validate staging rowcount
merge staging into target with INSERT ... ON CONFLICT DO UPDATE
sync sequences
drop staging table
verify target table
```

Postgres upsert shape:

```sql
INSERT INTO target_schema.target_table (col_a, col_b, col_c)
SELECT col_a, col_b, col_c
FROM staging_schema.staging_table
ON CONFLICT (primary_key_col)
DO UPDATE SET
  col_b = EXCLUDED.col_b,
  col_c = EXCLUDED.col_c;
```

Rules:

- Do not update conflict key columns.
- Do not update generated columns.
- Do not update identity columns unless explicitly safe.
- Preserve target-only columns when they are nullable or have defaults.
- Fail fast if required target columns cannot be populated.
- Drop staging tables after success.
- Keep failed staging tables only when debug mode is enabled.

### 3. Primary-Key and Unique-Key Readiness Checks

Add preflight key-readiness analysis for every table.

Readiness statuses:

| Status | Meaning | Upsert Eligible |
| --- | --- | ---: |
| `primary_key` | Table has a primary key in source and target. | Yes |
| `matching_unique_key` | Table has an approved unique constraint available in source and target. | Yes |
| `source_only_key` | Source has a key but target does not. | No |
| `target_only_key` | Target has a key but source does not expose equivalent metadata. | No |
| `no_key` | No safe conflict key found. | No |
| `ambiguous_key` | Multiple possible keys require human selection. | Manual review |

Artifacts:

- Add key readiness to `pre_migration_summary.json`.
- Add warnings to `planner_critique.json` when a selected strategy is not key-ready.
- Add strategy rationale to `planner_rationale.md`.

### 4. Dry-Run Execution Preview

Add a dry-run command that renders the exact approved execution without mutating the target.

CLI:

```powershell
amo dry-run --plan runs\analysis_demo\plan.json --approval runs\analysis_demo\approval.json
```

Output:

- approved tables
- selected strategy per table
- filtered plan steps
- destructive actions
- estimated rows
- dependency ordering
- upsert key choice
- verification depth
- blocked tables and reasons

Artifacts:

- `dry_run_preview.json`
- `dry_run_preview.md`

Dry-run should fail closed when:

- selected upsert table has no safe key
- selected truncate table lacks destructive approval
- approval references a table not present in the plan
- filtered plan has no executable steps outside `plan_only`

### 5. Retry and Resume for Failed Tables

Add a first-class retry path that uses the existing state file to avoid rerunning completed work.

CLI:

```powershell
amo retry --config config.yaml --plan plan.json --approval approval.json --state runs\state_20260523_120000.json
```

Modes:

| Mode | Behavior |
| --- | --- |
| `failed_only` | Retry steps associated with failed tables. |
| `from_failed_step` | Resume from the first failed step and continue onward. |
| `table` | Retry one specified table. |

Rules:

- Completed successful steps remain skipped unless `--force` is supplied.
- Destructive strategy checks still apply during retry.
- Upsert retry should be idempotent when the conflict key is valid.
- Truncate retry requires destructive approval again.

Artifacts:

- `retry_plan.json`
- `retry_summary.json`
- updated state file or new timestamped state file

### 6. Dependency-Aware Execution Graph

Make execution ordering explicit as a dependency graph.

Node types:

- schema
- UDF
- table DDL
- table data movement
- sequence sync
- index creation
- foreign key creation
- grant replay
- materialized view rebuild
- verification
- maintenance

Example ordering:

```text
ensure_schema
create_udfs
ensure_table
copy_or_upsert_table
sync_sequences
create_indexes
add_fks
apply_grants
create_or_refresh_matviews
verify_table
analyze_or_vacuum
```

Benefits:

- explain why a step runs before another
- detect cycles or missing dependencies
- support targeted retry by table or dependency group
- make dry-run output more understandable
- improve planner evals with dependency assertions

Artifacts:

- `execution_graph.json`
- optional `execution_graph.md`

### 7. Structured Failure Classification

Classify execution and verification failures into operator-useful categories.

Initial classes:

| Class | Example |
| --- | --- |
| `connection_error` | Source or target unavailable. |
| `permission_error` | Missing grants for schema/table/function. |
| `schema_mismatch` | Column or type mismatch during copy/upsert. |
| `duplicate_key` | Upsert conflict key is not unique in staging/source. |
| `foreign_key_violation` | Data violates target FK constraints. |
| `destructive_policy_denied` | Truncate requested without destructive approval. |
| `verification_mismatch` | Rowcount or sample hash differs after execution. |
| `timeout_or_resource_error` | Query timeout, disk pressure, memory pressure. |
| `planner_contract_error` | Plan shape violates schema or approval constraints. |
| `unknown_error` | Unclassified exception with captured context. |

Failure analysis output:

```json
{
  "failed_step": "copy_public_users",
  "table": "public.users",
  "failure_class": "duplicate_key",
  "retryable": true,
  "recommended_next_action": "Inspect duplicate conflict keys in staging before retrying upsert.",
  "safe_to_retry": true
}
```

Post-run summary should group failures by class and show the next recommended action.

## User Experience

### CLI Flow

```powershell
amo analyze --config config.yaml --planner heuristic --mode safe_sync --out-dir runs\analysis_demo
amo review --summary runs\analysis_demo\pre_migration_summary.json
amo approve --plan runs\analysis_demo\plan.json --summary runs\analysis_demo\pre_migration_summary.json --table-strategy public.users=upsert --table-strategy public.orders=truncate_reload --allow-destructive --out runs\analysis_demo\approval.json
amo dry-run --plan runs\analysis_demo\plan.json --approval runs\analysis_demo\approval.json
amo run --config config.yaml --plan runs\analysis_demo\plan.json --approval runs\analysis_demo\approval.json
amo retry --config config.yaml --plan runs\analysis_demo\plan.json --approval runs\analysis_demo\approval.json --state runs\state_20260523_120000.json
```

### Streamlit Flow

The Approve tab should show a table with:

- table name
- recommended action
- selected strategy
- key-readiness status
- conflict key
- destructive warning
- risk level
- approval status

The Run tab should show:

- dry-run preview before execution
- execution graph summary
- current step
- completed/failed/skipped tables
- retry options after failure

## Artifact Changes

New or changed artifacts:

| Artifact | Change |
| --- | --- |
| `approval.json` | Add `table_strategies`. |
| `pre_migration_summary.json` | Add key-readiness and strategy recommendations. |
| `planner_critique.json` | Flag invalid strategy/key/destructive combinations. |
| `planner_rationale.md` | Explain recommended strategy per table. |
| `dry_run_preview.json` | New machine-readable preview. |
| `dry_run_preview.md` | New human-readable preview. |
| `execution_graph.json` | New dependency graph artifact. |
| `retry_plan.json` | New filtered retry plan. |
| `retry_summary.json` | New retry explanation artifact. |
| `failure_analysis.json` | Add structured failure classes. |
| `failure_analysis.md` | Add grouped failures and recommended next actions. |

## Data Model Changes

Add strategy model:

```python
LoadStrategy = Literal["append_only", "upsert", "truncate_reload", "skip"]

class TableStrategy(BaseModel):
    strategy: LoadStrategy
    conflict_key: list[str] = Field(default_factory=list)
    staging_schema: str | None = None
    preserve_target_only_columns: bool = True
```

Extend approval model:

```python
class ApprovalDocument(BaseModel):
    ...
    table_strategies: dict[str, TableStrategy] = Field(default_factory=dict)
```

Add key-readiness model:

```python
KeyReadinessStatus = Literal[
    "primary_key",
    "matching_unique_key",
    "source_only_key",
    "target_only_key",
    "no_key",
    "ambiguous_key",
]
```

## Implementation Plan

### Phase 1: Approval and Preflight Model

- Add `TableStrategy` and key-readiness models.
- Extend approval artifact schema.
- Add key-readiness extraction to analysis.
- Add tests for strategy validation.

### Phase 2: Dry-Run Preview

- Add `amo dry-run`.
- Generate `dry_run_preview.json` and `.md`.
- Validate destructive and upsert readiness before execution.
- Add Streamlit dry-run preview.

### Phase 3: Upsert Execution

- Add staging table creation.
- Copy into staging.
- Generate deterministic `INSERT ... ON CONFLICT DO UPDATE`.
- Sync sequences and verify after merge.
- Add tests with a local Postgres fixture or integration smoke test.

### Phase 4: Execution Graph

- Build graph from validated plan and approval.
- Use graph for dry-run explanation.
- Add graph integrity tests.

### Phase 5: Retry and Failure Classification

- Add failure classifier.
- Extend state payloads with failure class and retryability.
- Add `amo retry`.
- Add post-summary grouping by failure class.

## Test Plan

Unit tests:

- approval schema accepts table strategies
- invalid strategy names fail validation
- upsert without key fails preflight
- truncate strategy without destructive approval fails dry-run
- dry-run filters plan correctly
- failure classifier maps common Postgres errors
- dependency graph orders steps correctly

Integration tests:

- append-only table copy
- truncate-reload table copy
- upsert table with primary key
- upsert table with duplicate source keys fails as `duplicate_key`
- retry failed table after fixing data

Eval additions:

- planner recommends upsert for key-ready metadata-match table
- planner routes no-key upsert candidate to manual review
- planner marks destructive strategy as requiring approval
- dependency graph contains expected order for FK-heavy schemas

## Success Criteria

- A reviewer can approve different strategies for different tables.
- Upsert works through a deterministic staging-table path.
- Upsert is blocked when no safe conflict key exists.
- Destructive table refresh requires explicit table strategy and destructive approval.
- Dry-run clearly shows what will happen before mutation.
- Failed runs can be retried without rerunning successful completed steps.
- Execution order can be inspected as a dependency graph.
- Failure analysis identifies the likely class and next operator action.
- Existing `analyze -> review -> approve -> run -> summarize-post` flow still works.

## Open Questions

- Should `upsert` update all mutable columns by default, or allow per-table include/exclude update columns?
- Should staging tables live in a dedicated schema such as `amo_staging` or use temporary tables?
- Should retry write a new state file by default or append to the existing state file?
- Should `replace_partition` be included in v0.2.0 or deferred to v0.3.0?
- Should Streamlit require an explicit confirmation checkbox per destructive table or one global confirmation plus strategy table?

## Recommended Scope For v0.2.0

Ship these in `v0.2.0`:

- per-table `append_only`, `upsert`, `truncate_reload`, and `skip`
- key-readiness checks
- dry-run preview
- staging-table upsert for primary-key tables
- structured failure classification

Defer these if needed:

- `replace_partition`
- full graph-driven executor rewrite
- advanced unique-key selection UI
- column-level upsert update rules


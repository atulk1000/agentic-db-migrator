# Agentic DB Migration Orchestrator

An approval-gated database migration orchestrator for PostgreSQL. It inspects source and target databases, computes deterministic drift, generates a structured migration plan, lets a human review and approve that plan, then executes only the approved actions with checkpointing and verification.

The key design choice is simple:

- the planner recommends
- the executor enforces

That means you can use a heuristic planner today and swap in a real LLM-backed planner later without changing the safety boundary.

## Default Workflow

The primary workflow is now:

1. `analyze`
2. `review`
3. `approve`
4. `run`
5. `summarize-post`

This is the recommended path for both heuristic and LLM planning because it separates read-only analysis from execution.

### What Each Step Does

- `analyze`
  - discovers both source and target
  - writes `source_manifest.json` and `target_manifest.json`
  - computes `manifest_diff.json`
  - generates `plan.json`
  - produces `pre_migration_summary.json` and a readable summary file
- `review`
  - prints the pre-migration summary for human review
- `approve`
  - creates an `approval.json` artifact with the approved migration mode, table scope, and destructive-policy choices
- `run`
  - executes only the approved subset of the plan
- `summarize-post`
  - produces a final post-migration summary from execution state and verification output

## Why This Is Better Than Direct Execute

The older low-level commands are still available, but the new workflow is better for production because it adds:

- source vs target drift analysis before transfer
- pre-migration warnings and manual-review routing
- explicit migration modes
- human approval before mutation
- auditable approval artifacts
- post-migration reporting

## Planner Capabilities

The planning layer can support:

- table ordering beyond simple size sorting
- transfer strategy selection
- chunking-column selection
- chunk count and concurrency hints
- verification depth selection
- risk scoring
- preflight warnings
- manual-review routing
- pre-migration summary generation
- post-migration summary generation

Today, the deterministic analysis layer already computes much of that from manifests and diffs. A real LLM-backed planner can later replace or augment those recommendations while still emitting the same structured plan schema.

## Large-Migration Features

The project now includes first-class support or scaffolding for the features that originally motivated it:

- `spark_jdbc` execution mode for large table movement
- geometry-aware transfer metadata for PostGIS-heavy tables
- grants discovery and replay
- staged materialized view rebuilds for large or geometry-heavy matviews
- partition replication fidelity checks
- post-migration maintenance steps such as `ANALYZE` and `VACUUM ANALYZE`

The heuristic planner emits these as structured plan steps or transfer hints so the same approval flow applies to them.

## Planner Backends

The CLI accepts:

- `heuristic`
- `demo`
- `gemini`
- `openai`
- `ollama`

Current behavior:

- `heuristic` is real and deterministic
- `demo` can call a hosted planner endpoint that you control, and otherwise falls back to the heuristic planner
- `gemini` can call the Gemini API when `GEMINI_API_KEY` is configured, and otherwise falls back to the heuristic planner
- `openai` is kept as a demo adapter and still falls back to local heuristic planning
- `ollama` currently falls back to local heuristic planning

Important notes:

- `demo` is only for low-friction public demos; it assumes you host a planner service and keep the real LLM credentials server-side
- for normal LLM-based planning, each user should provide their own API key or local model runtime
- `openai` requires your own OpenAI API key and OpenAI API billing once a real adapter is wired in
- ChatGPT Plus does not include OpenAI API credits
- `gemini` is the easiest direct hosted planner path when each user brings their own key

All live LLM-backed planners are expected to go through the same safety gate:

- provider-specific API call
- shared normalization and repair of common model mistakes
- strict plan validation
- heuristic fallback if the output is still invalid

## Quickstart

### Prerequisites

- Docker Desktop
- Docker Compose
- Python 3.10+

### Local Setup

```powershell
docker compose up -d

python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -e .

Copy-Item config.example.yaml config.yaml
Copy-Item .env.example .env
```

### Optional Browser UI

A lightweight Streamlit UI is included for users who want to test the migration workflow in a browser instead of using the CLI directly.

Install the UI dependency:

```powershell
pip install -e .[ui]
```

Launch it:

```powershell
streamlit run streamlit_app.py
```

The UI can:

- run `analyze`
- render the pre-migration summary
- create an approval artifact
- execute the approved run
- generate the post-migration summary

The browser UI is a workflow dashboard, not a separate migration engine. It uses the same underlying project functions as the CLI and is intended to make it easier to test the end-to-end flow in a browser.

The main screens map to the same artifacts as the CLI:

- `Migration Plan File` points to the generated `plan.json`
- `Run State File` points to the execution state file written during `run`
- `Pre-Migration Summary File` points to `pre_migration_summary.json`
- `Verification Report File (Optional)` points to a standalone `verify` report if you generated one
- `Post-Migration Summary Output File` is where the final browser-built summary will be written

If a workflow action succeeds but the dashboard looks stale, restart Streamlit so it reloads the latest code and session state:

```powershell
streamlit run streamlit_app.py
```

If you want to try the hosted demo endpoint path, set:

```powershell
$env:DEMO_PLANNER_URL="https://your-demo-planner.example.com/plan"
```

This path is intended only for demos. In normal usage, a user should configure their own provider credentials.

If you want to try the direct Gemini planner path locally with your own key, set:

```powershell
$env:GEMINI_API_KEY="your-key-here"
```

### Recommended Approval-Gated Flow

```powershell
python -m amo.cli analyze --config config.yaml --planner demo --out-dir runs\analysis_demo
python -m amo.cli review --summary runs\analysis_demo\pre_migration_summary.json
python -m amo.cli approve --plan runs\analysis_demo\plan.json --summary runs\analysis_demo\pre_migration_summary.json --mode safe_sync --out runs\analysis_demo\approval.json
python -m amo.cli run --config config.yaml --plan runs\analysis_demo\plan.json --approval runs\analysis_demo\approval.json
python -m amo.cli summarize-post --plan runs\analysis_demo\plan.json --state runs\state_YYYYMMDD_HHMMSS.json --pre-summary runs\analysis_demo\pre_migration_summary.json --out runs\analysis_demo\post_migration_summary.json
```

### Recommended Local LLM Flow

If you want to run the planner directly with your own hosted-model key, use `gemini` instead of `demo`:

```powershell
python -m amo.cli analyze --config config.yaml --planner gemini --out-dir runs\analysis_gemini
python -m amo.cli review --summary runs\analysis_gemini\pre_migration_summary.json
python -m amo.cli approve --plan runs\analysis_gemini\plan.json --summary runs\analysis_gemini\pre_migration_summary.json --mode safe_sync --out runs\analysis_gemini\approval.json
python -m amo.cli run --config config.yaml --plan runs\analysis_gemini\plan.json --approval runs\analysis_gemini\approval.json
python -m amo.cli summarize-post --plan runs\analysis_gemini\plan.json --state runs\state_YYYYMMDD_HHMMSS.json --pre-summary runs\analysis_gemini\pre_migration_summary.json --out runs\analysis_gemini\post_migration_summary.json
```

Expected request shape for a hosted demo planner:

```json
{
  "manifest": { "...": "the full source manifest" },
  "requested_planner": "demo",
  "requested_provider": "gemini"
}
```

The endpoint may return either:

```json
{ "plan": { "...": "full migration plan" } }
```

or the plan object directly. The plan must still satisfy the repo's validated plan schema.

### Low-Level Commands

These still exist for debugging and development:

```powershell
python -m amo.cli discover --config config.yaml --database source --out source_manifest.json
python -m amo.cli discover --config config.yaml --database target --out target_manifest.json
python -m amo.cli plan --manifest source_manifest.json --planner heuristic --out plan.json
python -m amo.cli run --config config.yaml --plan plan.json
python -m amo.cli verify --config config.yaml --plan plan.json --out report.json
```

## Migration Modes

The approval artifact supports these modes:

- `full_refresh`
- `missing_only`
- `metadata_diff_only`
- `data_diff_only`
- `safe_sync`
- `plan_only`

`plan_only` is analysis-only and cannot be executed.

## Artifacts

The new workflow writes a reusable set of artifacts:

- `source_manifest.json`
- `target_manifest.json`
- `manifest_diff.json`
- `plan.json`
- `pre_migration_summary.json`
- `approval.json`
- `state.json` or timestamped state files
- `report.json`
- `post_migration_summary.json`

These make the process auditable and resumable.

## Safety Model

This project is AI-assisted, not AI-autonomous.

- plans are validated against strict schemas
- execution uses allowlisted operations
- destructive actions require explicit approval
- approval can scope execution to specific tables
- manual-review items are surfaced before execution
- the executor never runs arbitrary free-form model SQL

## Current Scope

The current implementation is strongest as a Postgres-to-Postgres migration orchestrator with support for:

- schema discovery
- tables and partition awareness
- indexes and foreign keys
- materialized views
- staged materialized view rebuilds
- schema and relation grants
- UDF recreation
- optional Spark/JDBC transfer mode for large tables
- post-migration maintenance (`ANALYZE` / `VACUUM ANALYZE`)
- resumable execution
- rowcount and optional sample-hash verification

## Testing

Run the local checks with:

```powershell
python -m compileall src tests
python -m pytest -q
```

## Project Structure

```text
src/amo/
  cli.py
  core/
    analysis.py
    config.py
    executor.py
    manifest_builder.py
    verifier.py
    workflow_models.py
    planners/
      gemini.py
      heuristic_planner.py
      llm_common.py
      llm_stub.py
      models.py
      ollama.py
      openai.py
      remote_demo.py
tests/
  test_analysis.py
  test_cli_plan.py
  test_cli_workflow.py
  test_gemini_planner.py
  test_heuristic_planner.py
  test_remote_demo_planner.py
  test_verifier.py
```

## Roadmap

- replace the `openai` fallback with a true structured OpenAI planner
- harden the hosted demo planner with retries, repair, and better telemetry
- expand diffing and verification depth
- deepen the `spark_jdbc` engine with chunked/partition-wise execution policies
- improve PostGIS conversion handling for non-native transfer paths
- add evals for planner quality and fallback rate
- extract dialect adapters for additional databases

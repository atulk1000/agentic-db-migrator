# PRD: Explicit Agent Runtime Coordination

## Goal

Make the agentic coordination in Agentic DB Migration Orchestrator explicit by adding a real agent runtime boundary around the existing observe, plan, critique, approve, dry-run, execute, verify, and recover workflow.

This PRD is not about adding a cosmetic `agent.py` file. The goal is to make the current system-level agent behavior visible, testable, and easier to explain in code reviews, interviews, README architecture diagrams, and future MCP/tool integrations.

The existing safety principle remains unchanged:

> The planner recommends, the human approves, and deterministic code executes only validated operations.

## Release Version

Planned release: `v0.3.0`

Current package baseline: `0.2.0`

This should be a minor-version release because it introduces a clearer orchestration API and optional agent-run entrypoint while preserving the existing CLI, Streamlit, MCP, and artifact-first workflows.

## Problem

The project is already agentic at the workflow level, but the coordination is distributed across CLI commands, Streamlit handlers, artifacts, and core modules.

Today, the implicit agent loop is:

```text
observe   -> manifest_builder + diff_manifests
decide    -> heuristic/OpenAI/Gemini/demo planner
critique  -> agentic.py companion artifacts
approve   -> approval.json + safety checks
dry-run   -> approval validation + execution graph
act       -> deterministic executor
verify    -> verify_table / verification reports
recover   -> failure_analysis + retry plan
```

That is good architecture, but it is not obvious to a fast reviewer because there is no single runtime boundary that says:

```text
This object coordinates the migration agent loop.
```

The result is a storytelling gap:

- The README says the system is agentic.
- The code has planners, agentic companion artifacts, approval policy, executor, verifier, and retry planning.
- But the repo does not yet expose a clear `MigrationAgent` or `agent.py` coordination layer.

This can make the project look more like a CLI workflow with LLM helpers than a bounded operational agent, even though the underlying design is agentic.

## Target Users

- Technical reviewers evaluating whether the repo is truly agentic
- Interviewers asking where the agent loop lives
- Developers extending CLI, Streamlit, or MCP workflows
- AI assistants using the repo through MCP or CLI tools
- Data/platform engineers who want a single coordination API instead of manually chaining commands

## Non-Goals

- Do not make database migration fully autonomous.
- Do not remove human approval before mutation.
- Do not allow arbitrary model-generated SQL execution.
- Do not bypass existing plan validation, approval filtering, destructive guards, or deterministic execution.
- Do not duplicate planner, analyzer, executor, verifier, or policy logic.
- Do not break existing CLI commands or Streamlit tabs.
- Do not require OpenAI, Gemini, or any hosted LLM provider.

## Product Principles

- `agent.py` must coordinate existing capabilities, not hide or rewrite them.
- The agent runtime should be artifact-first and auditable.
- Every phase should have explicit inputs, outputs, status, and blockers.
- Human approval remains a first-class phase.
- Heuristic planning remains a valid deterministic planner backend inside the agent loop.
- LLM planning is optional and bounded by the same schemas, repair path, validation, and fallback behavior.
- Execution remains deterministic and allowlisted.
- Recovery should use state and failure analysis instead of blindly rerunning everything.

## Current Coordination

Current workflow coordination lives in:

| Layer | Current files | Responsibility |
| --- | --- | --- |
| Interface orchestration | `src/amo/cli.py`, `streamlit_app.py` | Chains analyze, approve, dry-run, run, verify, summarize commands |
| Observation | `manifest_builder.py`, `analysis.py` | Builds manifests and source/target drift |
| Planning | `core/planners/` | Generates validated migration plans with heuristic, demo, Gemini, or OpenAI backends |
| Agentic review | `core/agentic.py` | Builds planner critique, clarification questions, rationale, and failure analysis |
| Approval policy | `workflow_models.py`, `analysis.py`, `policy.py` | Captures approval scope, destructive policy, and table strategies |
| Execution | `executor.py` | Runs allowlisted deterministic operations |
| Verification | `verifier.py`, executor `verify_table` | Validates copied data |
| Recovery | `analysis.py` | Builds retry plans and post-migration summaries |

This is a real agentic system, but the coordination is implicit and distributed.

## Proposed Capability

Add a new explicit runtime module:

```text
src/amo/core/agent.py
```

Initial public type:

```python
class MigrationAgent:
    def observe(self) -> AgentPhaseResult: ...
    def plan(self) -> AgentPhaseResult: ...
    def critique(self) -> AgentPhaseResult: ...
    def prepare_approval(self) -> AgentPhaseResult: ...
    def dry_run(self) -> AgentPhaseResult: ...
    def execute_approved(self) -> AgentPhaseResult: ...
    def verify(self) -> AgentPhaseResult: ...
    def summarize(self) -> AgentPhaseResult: ...
    def recover(self) -> AgentPhaseResult: ...
```

The agent should coordinate existing functions and artifacts. It should not directly implement migration internals.

## Agent Loop

The explicit runtime loop should be:

```text
observe database state
    |
    v
generate or load plan
    |
    v
critique plan and ask clarification questions
    |
    v
wait for or prepare human approval
    |
    v
dry-run approved execution
    |
    v
execute allowlisted approved steps
    |
    v
verify results
    |
    v
summarize outcome
    |
    v
recover or retry if needed
```

The loop may stop at phase boundaries when human approval, missing credentials, destructive approval, manual review, or failed validation is required.

## Proposed Data Models

Add lightweight runtime models in `src/amo/core/agent.py` or `src/amo/core/agent_models.py`.

### `AgentPhase`

Initial values:

- `observe`
- `plan`
- `critique`
- `approval`
- `dry_run`
- `execute`
- `verify`
- `summarize`
- `recover`
- `blocked`
- `complete`

### `AgentPhaseResult`

Suggested fields:

```python
class AgentPhaseResult(BaseModel):
    phase: AgentPhase
    ok: bool
    message: str
    artifacts: dict[str, str] = {}
    blockers: list[str] = []
    next_phase: AgentPhase | None = None
```

### `AgentRunState`

Suggested fields:

```python
class AgentRunState(BaseModel):
    run_id: str
    config_path: str
    planner: str
    migration_mode: str
    analysis_dir: str
    current_phase: AgentPhase
    artifacts: dict[str, str] = {}
    approval_required: bool = True
    blocked_reason: str | None = None
```

## Proposed Files

```text
src/amo/core/agent.py
tests/test_migration_agent.py
docs/agent_runtime_coordination_prd.md
```

Optional later split:

```text
src/amo/core/agent_models.py
src/amo/core/agent_runtime.py
```

Keep the first implementation small unless the model layer becomes noisy.

## CLI Integration

Add a new optional command after the runtime class is stable:

```powershell
python -m amo.cli agent-run --config config.yaml --planner heuristic --mode safe_sync --out-dir runs\agent_demo
```

Initial behavior:

- Runs through observe, plan, critique, and dry-run-ready artifact creation.
- Stops before mutation unless an approval artifact is provided.
- If approval is provided, validates approval with dry-run before execution.

Potential command shape:

```powershell
python -m amo.cli agent-run `
  --config config.yaml `
  --planner heuristic `
  --mode safe_sync `
  --approval runs\agent_demo\approval.json `
  --execute
```

The command must not become a bypass around `approve`, `dry-run`, or `run`.

## Streamlit Integration

The existing Streamlit tabs can continue to call the same lower-level functions.

After the agent runtime exists, the UI can optionally show:

- current agent phase
- next recommended phase
- blocking reason
- artifacts produced by each phase
- "agent trace" table that maps phase to artifact and status

This should be additive and should not remove the existing step-by-step tabs.

## MCP Integration

MCP tools can use `MigrationAgent` as a safer coordination layer.

Potential future tools:

- `start_agent_run`
- `get_agent_run_state`
- `advance_agent_phase`
- `render_agent_trace`

Execution-oriented MCP tools must still require approval artifacts and explicit confirmation.

## Artifact Outputs

The agent runtime should collect and expose existing artifacts:

- `source_manifest.json`
- `target_manifest.json`
- `manifest_diff.json`
- `plan.json`
- `planner_critique.json`
- `clarification_questions.json`
- `planner_rationale.md`
- `pre_migration_summary.json`
- `approval.json`
- `dry_run_preview.json`
- `execution_graph.json`
- `state_*.json`
- `verification_report.json`
- `post_migration_summary.json`
- `failure_analysis.json`
- `retry_plan.json`

New optional artifact:

- `agent_trace.json`

Suggested shape:

```json
{
  "run_id": "agent_20260601_120000",
  "status": "blocked_for_approval",
  "phases": [
    {
      "phase": "observe",
      "ok": true,
      "artifacts": {
        "source_manifest": "runs/agent_demo/source_manifest.json",
        "target_manifest": "runs/agent_demo/target_manifest.json",
        "manifest_diff": "runs/agent_demo/manifest_diff.json"
      }
    }
  ]
}
```

## Safety Requirements

- Agent runtime must never execute raw LLM SQL.
- Agent runtime must only execute validated plan operations.
- Agent runtime must refuse mutation without approval unless explicitly running a non-mutating phase.
- Destructive behavior must require both approval artifact permission and executor configuration permission.
- `truncate_reload` must remain blocked without destructive approval.
- `upsert` must remain blocked without a validated conflict key.
- `plan_only` approvals must not execute.
- Failed dry-run validation must block execution.

## Acceptance Criteria

### Functional

- A `MigrationAgent` class exists and coordinates at least observe, plan, critique, dry-run, and summarize phases.
- The agent writes or returns an `AgentPhaseResult` for each phase.
- The agent can stop cleanly when approval is required.
- The agent can consume an approval artifact and execute only the approved filtered plan.
- The agent can produce an `agent_trace.json` artifact.
- Existing CLI commands still work unchanged.
- Existing Streamlit workflow still works unchanged.

### Agent Signal

- README can point to a concrete `MigrationAgent` runtime boundary.
- The code makes the loop obvious without requiring a reviewer to infer it from CLI functions.
- Heuristic, OpenAI, Gemini, and demo planners are all represented as planner backends inside the same agent loop.
- The implementation reinforces that deterministic fallback is a strength, not a non-agentic exception.

### Tests

Add tests for:

- agent observe/plan/critique phases produce expected artifacts
- agent stops before execution when approval is missing
- agent refuses invalid approval or failed dry-run
- agent executes with a valid approval using a mocked executor
- agent trace records phase status and artifact paths
- existing CLI workflow tests continue to pass

## Open Questions

- Should `agent-run` ship in the same release as `MigrationAgent`, or should the class land first and the CLI command follow?
- Should `agent_trace.json` be generated for every existing CLI workflow or only for `agent-run`?
- Should Streamlit show an agent trace in a new tab or inside the existing Artifacts tab?
- Should retry/recovery be included in the first `MigrationAgent` release or deferred to a follow-up?

## Recommended Initial Scope

Ship these first:

1. `src/amo/core/agent.py`
2. `MigrationAgent`
3. `AgentPhaseResult`
4. `AgentRunState`
5. observe, plan, critique, dry-run, and summarize coordination
6. blocking behavior when approval is required
7. `agent_trace.json`
8. README architecture update pointing to the runtime boundary
9. focused unit tests with executor mocked

Defer these until the initial runtime boundary is stable:

- autonomous multi-step `while` loop
- MCP execution tools
- operational memory
- adaptive replanning after live failures
- Streamlit trace visualization beyond the Artifacts tab

## Interview Positioning

After this PRD is implemented, the project can be described as:

> An approval-gated PostgreSQL migration agent with an explicit runtime coordinator. The agent observes source and target database state, plans with a deterministic or LLM-backed planner, critiques the plan, prepares review artifacts, waits for human approval, validates the approved run with a dry-run graph, executes only deterministic allowlisted operations, verifies results, and produces recovery artifacts when needed.

This makes the agent signal explicit while preserving the core safety story.

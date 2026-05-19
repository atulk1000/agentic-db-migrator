# PRD: MCP Server for Agentic DB Migration Orchestrator

## Goal

Add an MCP server that exposes Agentic DB Migration Orchestrator as a safe, standardized tool layer for AI agents. The server should let external MCP clients inspect migration artifacts, analyze source/target drift, generate and critique plans, ask clarification questions, and produce migration reports while preserving the project safety model:

> LLM suggests, deterministic executor enforces, human approves.

## Problem

The project currently exposes a CLI and Streamlit workflow:

```text
analyze -> review -> approve -> run -> summarize-post
```

It also supports deterministic and LLM-backed planners. However, AI access is mostly embedded inside the app. External AI assistants cannot call the migration system through a standardized, governed interface.

MCP solves this by exposing the system as:

- tools: bounded callable operations
- resources: read-only migration context
- prompts: reusable guided workflows

## Target Users

- Developers evaluating the repo locally
- Hiring managers reviewing architecture depth
- AI coding assistants or ops copilots that need safe migration tools
- Platform/data teams experimenting with agent-accessible migration workflows

## Non-Goals

- Do not allow arbitrary model-generated SQL execution.
- Do not expose `.env` or raw database credentials as resources.
- Do not make migrations fully autonomous.
- Do not replace the CLI or Streamlit workflow.
- Do not require LLM provider credentials for basic MCP usage.
- Do not add mutation tools before the read/review control plane is stable.

## Product Principles

- MCP is a control plane, not a bypass.
- Read-only and review tools come first.
- Dangerous execution requires approval artifacts and explicit confirmation in a later phase.
- All migration plans must pass deterministic validation.
- All outputs should remain auditable JSON/Markdown artifacts.
- MCP tools should reuse existing `amo.core` logic instead of duplicating migration behavior.

## Architecture

```text
MCP Client / AI Assistant
        |
        v
Agentic DB Migrator MCP Server
        |
        v
Existing Orchestrator Core
        |
        +-- manifest_builder.py
        +-- analysis.py
        +-- planners/
        +-- agentic.py
        +-- executor.py
        +-- verifier.py
        |
        v
runs/<analysis_id>/ artifacts
```

Initial implementation:

```text
src/amo/mcp_tools.py
src/amo/mcp_server.py
```

Future split if the surface grows:

```text
src/amo/mcp/
  server.py
  tools.py
  resources.py
  prompts.py
  schemas.py
```

## MVP Scope

The MVP exposes a read/review control plane. It does not execute migrations.

### Tool: `analyze_databases`

Runs the existing analysis workflow.

Inputs:

```json
{
  "config_path": "config.yaml",
  "planner": "heuristic",
  "migration_mode": "safe_sync",
  "out_dir": "runs/mcp_analysis_demo"
}
```

Outputs:

```json
{
  "out_dir": "runs/mcp_analysis_demo",
  "source_manifest": "...",
  "target_manifest": "...",
  "manifest_diff": "...",
  "plan": "...",
  "pre_migration_summary": "...",
  "planner_critique": "...",
  "clarification_questions": "...",
  "planner_rationale": "..."
}
```

### Tool: `list_artifacts`

Lists known artifacts in a run directory.

### Tool: `read_artifact`

Reads safe `.json` or `.md` artifacts. It must deny `.env`, config files, files outside the workspace, and unsupported extensions.

### Tool: `validate_plan`

Validates a plan against the strict migration plan schema.

### Tool: `critique_plan`

Runs the planner critic against existing plan, summary, and drift artifacts.

### Tool: `generate_clarification_questions`

Generates operator questions from summary and drift artifacts.

### Tool: `generate_plan_rationale`

Creates a human-readable plan rationale markdown artifact.

### Tool: `build_post_migration_summary`

Builds a post-migration summary and post-run failure analysis from existing plan/state/report artifacts.

## Future Guarded Mutation Tools

These should be implemented after the read/review MCP surface is stable.

- `create_approval`
- `execute_approved_migration`
- `verify_post_migration`

Safety requirements:

- `execute_approved_migration` must require an approval artifact.
- `plan_only` approvals must be rejected.
- destructive execution must require both approval and explicit confirmation.
- arbitrary SQL execution must never be exposed.

## Resources

Initial read-only resources:

```text
policy://allowed-operations
manifest://source/latest
manifest://target/latest
drift://latest
plan://latest
summary://pre/latest
summary://post/latest
critique://latest
questions://latest
rationale://latest
```

Resources are artifact-backed and must not expose credentials.

## Prompts

Initial reusable prompts:

- `analyze_drift`
- `review_safe_migration_plan`
- `debug_failed_verification`
- `prepare_approval`

These prompts should guide agents to use tools/resources in safe workflows and explicitly avoid unapproved execution.

## Safety Model

| Capability | MCP MVP | Requires Approval |
| --- | ---: | ---: |
| Read artifacts | Yes | No |
| Discover schemas | Yes | No |
| Analyze drift | Yes | No |
| Generate plan | Yes | No |
| Critique plan | Yes | No |
| Ask clarification questions | Yes | No |
| Create approval artifact | Future | Human decision |
| Execute migration | Future | Yes |
| Run arbitrary SQL | Never | Never |
| Expose `.env` or credentials | Never | Never |

## Success Criteria

- MCP server starts locally.
- MCP tools can run analysis and produce artifacts.
- MCP tools can validate, critique, and explain a plan.
- MCP resources expose latest drift/plan/summary context.
- Prompt templates are available to MCP clients.
- No MCP MVP tool executes migration or arbitrary SQL.
- Tests cover artifact reading, plan validation, critic generation, and unsafe path rejection.

## Final Positioning

Agentic DB Migration Orchestrator is an approval-gated PostgreSQL migration control plane exposed through CLI, Streamlit, and MCP. It lets AI agents inspect schemas, analyze drift, generate and critique plans, ask clarification questions, and produce reports while deterministic code enforces execution safety.

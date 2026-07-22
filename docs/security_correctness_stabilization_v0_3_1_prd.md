# PRD: Security and Correctness Stabilization v0.3.1

## Goal

Harden Agentic DB Migration Orchestrator so its implemented safety boundary matches its documented safety boundary.

This release closes security, correctness, packaging, test-evidence, and public-release gaps without adding new migration features.

The release must make the core project principle true at every supported execution entrypoint:

> The planner recommends, the human approves an exact artifact set, and deterministic code executes only validated, approved operations.

## Release Version

Planned release: `v0.3.1`

Current package baseline: `0.3.0`

Current code baseline: `7ef897e433764ec70e56e04dc959a310c8aaf8cd`

This is a patch release because it corrects behavior that is already promised by the README and existing workflow. It does not add a new database dialect, transfer engine, planner, UI, or migration mode.

The approval artifact will receive a new schema version. Legacy approval artifacts may require regeneration because silently accepting an approval that is not bound to exact inputs would preserve the defect this release is intended to fix.

## Release Thesis

`v0.3.1` is successful when a reviewer can trust four claims:

1. No supported execution path can bypass approval.
2. Approval is bound to the exact plan, summary, and trusted source manifest that were reviewed.
3. Planner output cannot introduce executable SQL that was not derived from trusted source metadata.
4. The repository proves its migration behavior against real PostgreSQL in CI and works when installed from a built wheel.

## Confirmed Problems

| Severity | Problem | Current risk | Required outcome |
| --- | --- | --- | --- |
| P0 | A provider API key exists in public Git history and the corresponding secret-scanning alert remains open. | Credential misuse and an unresolved public security incident. | Revoke or rotate the credential, resolve the alert, and add a regression guard. |
| P0 | `amo run` can execute without an approval artifact. | The approval-gated safety claim is bypassable. | Approval becomes mandatory for every supported mutation entrypoint. |
| P0 | Approval stores artifact paths but not content digests. | A plan or summary can change after approval without invalidating the approval. | Bind approval to SHA-256 digests of the reviewed artifacts. |
| P0 | The CLI can receive a plan different from the path recorded in the approval. | A benign plan can be approved and another plan executed. | Validate artifact identity by digest before database connection or mutation. |
| P0 | SQL-bearing plan fields are schema-valid but are not cross-checked against trusted source metadata. | Untrusted planner output can influence SQL executed for UDFs, indexes, foreign keys, grants, and materialized views. | Hydrate or verify executable metadata deterministically from the trusted source manifest. |
| P1 | The executor accepts raw plan dictionaries without validating them at its boundary. | Internal or future callers can bypass CLI-level validation. | Require a validated, approved execution bundle at the public executor boundary. |
| P1 | Checkpoint resume skips any recorded step, including a failed step. | A resumed migration can silently leave failed work unresolved. | Skip only successful steps and retain explicit retry history for failures. |
| P1 | The built wheel omits the versioned planner prompt. | OpenAI and Gemini planning can fail outside an editable source checkout. | Include package data and verify the installed-wheel path in CI. |
| P1 | Core tests mock database execution. | CI does not prove that migration, approval, destructive guards, retry, or verification work against PostgreSQL. | Add a real source-to-target PostgreSQL integration job. |
| P2 | The public approval example advertises `plan.json` and `state.json`, but `.gitignore` prevents them from being published. | The primary reviewer path is incomplete on GitHub. | Publish a complete, internally consistent artifact chain. |
| P2 | The package version is `0.3.0`, but the repository has no corresponding tags or releases and no license. | Public release state and reuse terms are unclear. | Add release metadata and an owner-selected license before `v0.3.1`. |

## Target Users

- Data engineers evaluating the tool for PostgreSQL environment refreshes
- Operators who need to review and approve an exact migration plan before mutation
- Developers extending the CLI, Streamlit, agent runtime, or future MCP mutation tools
- Security reviewers checking whether planner output is isolated from execution authority
- Hiring managers and technical reviewers evaluating engineering depth and evidence

## Threat Model

This release treats the following inputs as untrusted until validated:

- OpenAI, Gemini, demo, or other planner output
- manually edited `plan.json`
- manually edited summary or manifest artifacts
- approval files whose referenced artifacts have changed
- retry plans or state files provided through the CLI

This release treats the local operator and repository code as trusted. It does not attempt to protect against an operator who can modify and run arbitrary Python code on the migration host.

Database credentials remain local configuration and must never be written into plans, approvals, state files, traces, logs, examples, or test artifacts.

## Non-Goals

- Do not add MySQL, SQL Server, Oracle, or other dialect adapters.
- Do not expand Spark JDBC or PostGIS capabilities.
- Do not add a new planner backend.
- Do not redesign the Streamlit interface.
- Do not add autonomous execution or remove human approval.
- Do not implement RBAC, cryptographic user signatures, or an external approval service.
- Do not build a full schema migration language or accept arbitrary user SQL.
- Do not rewrite the executor or analysis modules solely to reduce file size.
- Do not add execution-oriented MCP tools in this release.
- Do not treat Git-history rewriting as a substitute for revoking an exposed credential.

## Product Principles

- Safety checks belong at the execution boundary, not only in UI or CLI orchestration.
- Configuration may restrict behavior but must not grant approval.
- An approval applies to exact artifact contents, not only filenames.
- Planner output is a proposal, not executable metadata.
- SQL-bearing metadata must come from a trusted source manifest or live deterministic introspection.
- Rejected execution must fail before opening a target mutation transaction whenever possible.
- Successful checkpointed work should be reusable; failed work should remain visible and retryable.
- Installable package behavior must match editable-checkout behavior.
- CI evidence should exercise the database behavior that the project claims to support.
- Documentation must describe current enforcement, not intended enforcement.

## Selected Fixes

This PRD covers seven stabilization workstreams:

1. Credential incident remediation and regression prevention
2. Mandatory approval and artifact integrity
3. Trusted executable-metadata boundary
4. Checkpoint and retry correctness
5. Wheel and packaging correctness
6. PostgreSQL end-to-end verification
7. Public reviewer path and release hygiene

## 1. Credential Incident Remediation

### Required Actions

- Revoke or rotate the exposed provider credential outside the repository.
- Confirm the revoked credential can no longer authenticate; do not test it through this repository or log its value.
- Mark the GitHub secret-scanning alert as resolved with the accurate resolution reason.
- Scan the current tree and Git history without printing secret values.
- Add a repository secret-scanning regression check, such as Gitleaks, to pull-request and push CI.
- Add `SECURITY.md` with a private vulnerability-reporting path and credential-response guidance.
- Review whether Git-history rewriting is worthwhile after revocation. If history is rewritten, coordinate the force-push and clone-reset impact separately.

### Acceptance Criteria

- GitHub reports zero open secret-scanning alerts for the repository.
- The exposed credential has been revoked or rotated.
- CI fails when a known test-secret fixture is introduced outside the scanner's allowlisted test path.
- CI and documentation never print the historical credential.
- `.env`, generated configs containing secrets, and provider credentials remain ignored.

## 2. Mandatory Approval and Artifact Integrity

### Approval Schema v2

Extend `ApprovalDocument` with explicit artifact identity:

```python
class ArtifactDigest(BaseModel):
    path: str
    sha256: str


class ApprovalDocument(BaseModel):
    schema_version: Literal["2"] = "2"
    approved_mode: MigrationMode
    approved_at: str
    approved_by: str
    allow_destructive: bool = False
    plan: ArtifactDigest
    summary: ArtifactDigest
    source_manifest: ArtifactDigest
    included_tables: list[str]
    excluded_tables: list[str]
    approved_manual_review_items: list[str]
    table_strategies: dict[str, TableStrategy]
    notes: str | None = None
```

SHA-256 values must be calculated from exact file bytes at approval creation time. Paths remain useful for operator ergonomics, but digest equality is the security decision.

Moving an unchanged artifact is allowed if the caller explicitly provides its new path and the digest still matches. Editing or reserializing an approved artifact requires a new approval.

### Execution Bundle

Add one shared validation path used by CLI, Streamlit, and `MigrationAgent`:

```python
class ApprovedExecutionBundle(BaseModel):
    approval: ApprovalDocument
    validated_plan: MigrationPlan
    filtered_plan: MigrationPlan
    source_manifest: MigrationManifest
    pre_migration_summary: PreMigrationSummary
```

Only the approval-validation layer should create this bundle. The public execution function must require the bundle instead of accepting an arbitrary plan dictionary.

Suggested API shape:

```python
def build_approved_execution_bundle(
    *,
    approval_path: str,
    plan_path: str | None = None,
    summary_path: str | None = None,
    source_manifest_path: str | None = None,
) -> ApprovedExecutionBundle: ...


def execute_approved(
    *,
    cfg: dict[str, Any],
    bundle: ApprovedExecutionBundle,
    state_path: str,
) -> None: ...
```

The current raw plan-driven executor may remain as a private implementation detail for tests and internal delegation, but it must not be exposed as a supported mutation entrypoint.

### Validation Order

Before creating database pools or opening a target connection:

1. Parse the approval using the v2 schema.
2. Load the plan, summary, and source manifest.
3. Calculate and compare all approved SHA-256 digests.
4. Validate all three artifact schemas.
5. Confirm summary and approval table scopes agree with the plan and source manifest.
6. Validate or hydrate SQL-bearing metadata against the source manifest.
7. Apply approval filtering and per-table strategies.
8. Revalidate the filtered executable plan.
9. Enforce destructive policy from the approval.
10. Only then initialize the executor and database connections.

### CLI Behavior

`amo run` must require:

```text
--approval <approval.json>
```

The CLI may continue accepting `--plan` as an ergonomic override, but the supplied bytes must match the plan digest recorded in the approval.

Remove or reject configuration-only authorization:

- `engine.allow_destructive=true` is not sufficient to authorize destructive work.
- `--allow-destructive` cannot elevate beyond the approval artifact.
- The default example configuration must set destructive execution and truncate-first behavior to `false`.
- `truncate_reload` requires both the approved table strategy and `approval.allow_destructive=true`.

The same approved-bundle builder must be used by:

- `amo run`
- `amo retry`
- `MigrationAgent.execute_approved`
- Streamlit execution
- any future mutation-capable MCP tool

### Legacy Approval Behavior

- Approval files without `schema_version: "2"` must fail closed at execution.
- The error must explain that the approval predates artifact integrity binding and must be regenerated.
- Read-only rendering of legacy example artifacts may remain supported if useful.
- Do not silently upgrade a legacy approval because the original approver did not approve the newly calculated digests.

## 3. Trusted Executable-Metadata Boundary

### Problem

Plan validation currently constrains operation names and basic shapes, but SQL-bearing nested fields can still contain arbitrary strings. The executor uses several of those strings for:

- UDF creation statements
- index definitions and cluster statements
- foreign-key definitions
- materialized-view definitions
- grant privileges
- partition expressions and bounds
- column types and defaults used during DDL recreation

Schema validation alone does not prove that these strings came from source discovery rather than a planner response or manual edit.

### Required Design

Treat planner output as a proposal containing:

- operation type
- schema and object identity
- ordering
- transfer hints
- validation depth
- risk or rationale metadata

Before execution, deterministically hydrate executable metadata from the approved source manifest.

For v0.3.1, use one of these safe implementations:

1. Preferred: strip SQL-bearing nested payloads from planner proposals and repopulate them from the source manifest.
2. Acceptable: compare every SQL-bearing value with the corresponding source-manifest value and reject any mismatch.

The implementation must cover UDFs, indexes, foreign keys, materialized views, grants, partitions, column types, and defaults. Partial enforcement is not sufficient for this release.

Unknown schemas, tables, functions, views, indexes, grants, or constraints must be rejected before execution.

### Allowed SQL Sources

- deterministic SQL constructed by code using `psycopg2.sql.Identifier` and allowlisted operations
- metadata captured by deterministic source introspection and bound to the approved source-manifest digest
- static, repository-controlled SQL used only for test fixtures

### Disallowed SQL Sources

- raw planner-generated SQL
- unbound plan edits made after approval
- free-form SQL supplied through CLI, Streamlit, MCP, or approval notes
- arbitrary SQL embedded in retry or state artifacts

### Acceptance Criteria

- A schema-valid plan containing a modified UDF statement is rejected before database connection.
- Modified index, FK, matview, grant, partition, column-type, and default payloads are rejected or replaced from the approved manifest.
- Valid heuristic, OpenAI, Gemini, and demo planner proposals still produce the same deterministic executable plan shape after hydration.
- The README no longer overstates the boundary; its claims match the implemented validation path.

## 4. Checkpoint and Retry Correctness

### State Model

Replace ambiguous membership-based completion checks with explicit status:

```python
StepStatus = Literal["running", "succeeded", "failed", "skipped"]


class StepAttempt(BaseModel):
    attempt: int
    started_at: float
    finished_at: float | None = None
    status: StepStatus
    elapsed_s: float | None = None
    error: str | None = None
    failure_class: str | None = None
    retryable: bool | None = None


class StepState(BaseModel):
    status: StepStatus
    attempts: list[StepAttempt]
```

### Required Behavior

- Resume skips only steps whose latest status is `succeeded`.
- A failed step remains visible and eligible for explicit retry.
- Retrying appends an attempt instead of overwriting the prior failure.
- Step IDs must be unique within a validated plan.
- A state file must be bound to the approved plan digest.
- Loading a state file from another plan must fail closed.
- Retry plans must pass the same approval and trusted-metadata validation as initial execution.
- A retry must not broaden the originally approved table scope or destructive permissions.

### Backward Compatibility

Existing state files may be normalized into the new structure only when their plan identity can be established safely. Otherwise, require a fresh state file and preserve the legacy file for audit history.

## 5. Wheel and Packaging Correctness

### Required Changes

- Include `src/amo/core/planners/prompts/*.md` as package data.
- Load prompt resources with `importlib.resources` instead of assuming a source-tree path.
- Build both source distribution and wheel in CI.
- Install the wheel into a clean environment without editable mode.
- Run CLI import, prompt loading, and planner fallback smoke tests from the installed wheel.
- Confirm the `amo` and `amo-mcp` console entrypoints are present.
- Confirm the wheel contains no `.env`, local config, run state, secrets, screenshots, or unrelated generated files.

### Acceptance Criteria

- `python -m build` succeeds.
- `twine check dist/*` or an equivalent metadata check succeeds.
- The versioned prompt is present in the wheel.
- OpenAI and Gemini planners can build their prompt from an installed wheel.
- Missing provider keys still produce the documented deterministic fallback rather than a packaging exception.

## 6. PostgreSQL End-to-End Verification

### CI Topology

Add a dedicated integration job with isolated source and target PostgreSQL instances. GitHub Actions service containers or Docker Compose are both acceptable.

The job must initialize representative source objects:

- multiple schemas
- a table with a primary key and sequence
- an append-only table
- an upsert-ready table
- a parent/child foreign-key pair
- a partitioned table with at least two partitions
- a secondary index
- a simple UDF
- a materialized view
- schema and relation grants when supported by the fixture roles

### Required Scenarios

1. Analyze source and target and generate deterministic artifacts.
2. Create a v2 non-destructive approval.
3. Execute append-only and upsert strategies.
4. Verify row counts and a deterministic sample check.
5. Confirm sequence synchronization after data movement.
6. Confirm partition structure, indexes, FK, UDF, and matview behavior.
7. Attempt `truncate_reload` without destructive approval and prove that no target rows are changed.
8. Approve `truncate_reload`, execute it, and verify the expected target state.
9. Modify one byte in the approved plan and prove execution fails before target mutation.
10. Inject a mismatched SQL-bearing metadata field and prove execution fails before target mutation.
11. Force one retryable failure, preserve the failed attempt, retry it, and skip previously successful steps.

### CI Expectations

- Unit and integration jobs are separate so failures are easy to diagnose.
- Integration tests run on pull requests and `main` pushes.
- The integration job should target a predictable runtime, initially under five minutes.
- Test logs must not include database passwords, provider keys, or full DSNs.
- Failure diagnostics should retain safe state and report artifacts as workflow artifacts when useful.

## 7. Public Reviewer Path and Release Hygiene

### Example Artifacts

- Change `.gitignore` patterns so root runtime files remain ignored without excluding curated nested examples.
- Check in `examples/approval_workflow/plan.json` and `state.json`.
- Regenerate the example as an approval-schema-v2 chain.
- Ensure all referenced paths and SHA-256 values are internally consistent.
- Add an automated example-consistency test that loads the manifest, plan, summary, approval, state, and verification report.
- Verify every relative README link in CI.

### Documentation

- Update the README safety section to describe mandatory approval and digest binding.
- State that `approved_by` is an audit label, not authenticated identity.
- Document legacy approval rejection and regeneration.
- Add a short three-minute reviewer path before detailed architecture sections.
- Keep the deeper technical material but remove duplicated agentic and safety claims.
- Document how to run unit tests, integration tests, wheel smoke tests, and secret scans.
- Add `SECURITY.md`.

### Release Metadata

- Add an owner-selected open-source license before release.
- Add a changelog or release-notes section covering security and compatibility changes.
- Update package version to `0.3.1` only when release criteria pass.
- Create Git tag `v0.3.1` from the verified release commit.
- Create a GitHub release describing the approval-schema change and required approval regeneration.
- Do not claim that a release, tag, or push exists until the corresponding GitHub operation succeeds.

## Security Invariants

The following invariants are release blockers:

- No supported mutation command runs without an approval artifact.
- No approved artifact can change without invalidating approval.
- Configuration cannot elevate destructive authority beyond approval.
- Retry cannot broaden approved scope.
- The executor receives only a validated, approved, filtered plan.
- Planner output cannot contribute unverified executable SQL.
- Unknown operations and duplicate step IDs fail validation.
- Artifact validation completes before target mutation begins.
- Credentials never appear in generated artifacts or logs.
- The repository has no open secret-scanning alerts at release time.

## Artifact Changes

| Artifact | Change |
| --- | --- |
| `approval.json` | Add schema version and plan, summary, and source-manifest SHA-256 bindings. |
| `plan.json` | Add or validate source-manifest identity and unique step IDs. |
| `state.json` | Add plan identity and per-step attempt history with explicit statuses. |
| `retry_plan.json` | Bind to the original approval and prevent scope expansion. |
| `dry_run_preview.json` | Include integrity-validation results and approved artifact digests. |
| `agent_trace.json` | Record integrity checks and blockers without logging secrets. |
| `examples/approval_workflow/*` | Publish a complete v2 approval chain, including plan and state. |
| `SECURITY.md` | Add security reporting and credential-response guidance. |

## Proposed Code Organization

The exact module split may vary, but safety responsibilities should be discoverable:

```text
src/amo/core/
  approval.py or policy.py
    approval schema validation
    destructive-policy enforcement
    approval filtering
  integrity.py
    SHA-256 calculation
    artifact identity validation
    plan-to-manifest validation or hydration
  executor.py
    private raw operation runner
    approved execution entrypoint
  state.py
    state schema
    attempt recording
    resume and retry selection
```

Avoid a broad refactor unrelated to the release criteria. Extract only enough code to create a single, testable enforcement path.

## Interface Changes

### CLI

Expected mutation flow:

```powershell
python -m amo.cli analyze --config config.yaml --planner heuristic --out-dir runs\analysis
python -m amo.cli approve --plan runs\analysis\plan.json --summary runs\analysis\pre_migration_summary.json --source-manifest runs\analysis\source_manifest.json --out runs\analysis\approval.json
python -m amo.cli dry-run --approval runs\analysis\approval.json
python -m amo.cli run --config config.yaml --approval runs\analysis\approval.json
```

Expected tamper failure:

```text
Execution blocked: plan SHA-256 does not match the approved artifact.
Regenerate approval after reviewing the updated plan.
```

### Streamlit

- Approval must display shortened plan, summary, and source-manifest digests.
- Run must use the shared approved-bundle builder.
- If an artifact changes, the UI must invalidate the current approval and route the operator back to review.
- Destructive execution must continue requiring explicit typed confirmation in addition to the approval artifact.

### MigrationAgent

- `prepare_approval` writes approval schema v2.
- `dry_run` validates artifact digests.
- `execute_approved` accepts no unapproved fallback path.
- The trace records integrity validation as a distinct status or phase result.
- The CLI agent flow must call verification before final summarization when execution succeeds.

### MCP

- Existing read/review tools remain read-only.
- Plan validation should report manifest-binding failures when the required manifest is provided.
- Mutation tools remain deferred.

## Implementation Plan

### Phase 0: Credential Containment

- Revoke or rotate the exposed credential.
- Resolve the GitHub alert accurately.
- Add secret scanning and `SECURITY.md`.
- Verify that no current tracked file contains a live credential.

This phase must complete before publishing additional release work.

### Phase 1: Artifact Integrity and Mandatory Approval

- Add digest utilities and approval schema v2.
- Add source-manifest identity to approval.
- Implement the shared approved-execution-bundle builder.
- Require approval in CLI, Streamlit, and agent execution.
- Make example destructive defaults false.
- Add legacy approval rejection with an actionable error.

### Phase 2: Trusted Metadata and Executor Boundary

- Cross-check or hydrate SQL-bearing metadata from the approved source manifest.
- Reject unknown objects, mismatched metadata, and duplicate step IDs.
- Convert the public executor boundary to accept only an approved bundle.
- Add tamper and planner-payload security tests.

### Phase 3: State, Retry, and Packaging

- Add explicit step status and attempt history.
- Bind state and retry artifacts to the approved plan.
- Fix resume and retry selection.
- Package prompt resources correctly.
- Add built-wheel smoke tests.

### Phase 4: PostgreSQL Integration Proof

- Build representative source and target fixtures.
- Add safe, destructive-blocked, destructive-approved, tamper, metadata-injection, verification, and retry scenarios.
- Add the integration job to CI.
- Store safe diagnostic artifacts for failed CI runs.

### Phase 5: Reviewer Path and Release

- Regenerate and publish complete example artifacts.
- Update and shorten README safety and reviewer sections.
- Add the owner-selected license.
- Run the full release checklist.
- Tag and publish `v0.3.1` only after all gates pass.

## Test Plan

### Unit Tests

- v2 approval records correct SHA-256 digests
- modified plan fails integrity validation
- modified summary fails integrity validation
- modified source manifest fails integrity validation
- legacy approval fails closed with a regeneration message
- destructive config cannot override non-destructive approval
- CLI run without approval fails before loading database configuration
- duplicate step IDs fail plan validation
- unknown table or object references fail manifest validation
- each SQL-bearing metadata category is hydrated or mismatch-rejected
- successful state entries are skipped on resume
- failed state entries remain retryable
- retry appends an attempt and preserves prior failures
- retry scope cannot exceed original approval
- state from another plan fails identity validation
- installed prompt resource can be loaded through `importlib.resources`
- public example artifact digests and references are consistent

### Integration Tests

- analyze, approve, run, verify, and summarize against source and target PostgreSQL
- append-only execution preserves existing target rows
- upsert updates matching rows and inserts new rows
- truncate reload is blocked without destructive approval and leaves target unchanged
- approved truncate reload produces the expected target state
- plan tampering is blocked before target mutation
- SQL-bearing metadata tampering is blocked before target mutation
- partition, FK, index, sequence, UDF, matview, and grant outcomes are verified
- retry records a second attempt and does not rerun successful prerequisites unnecessarily

### Planner Evals

- Existing 10 heuristic cases remain 10/10
- Planner proposals containing SQL-bearing values not present in the manifest are rejected or safely hydrated
- OpenAI and Gemini normalization tests continue using mocked provider responses
- Missing API keys continue to produce deterministic fallback plans

### Packaging Tests

- build sdist and wheel
- inspect wheel contents for the versioned prompt
- install wheel into a clean environment
- run `amo --help` and `amo-mcp --help` or import smoke equivalents
- load planner prompt from the installed wheel
- run a no-key heuristic/fallback planner smoke test

### Security Tests

- secret scanner runs on the current tree and pull-request diff
- approval, plan, state, trace, and logs contain no configured password values
- a test credential outside an allowlisted fixture makes the scanner fail
- plan, summary, manifest, and state tampering fail before target mutation

## CI Gates

The release branch must require all of these checks:

- Black formatting
- Ruff linting
- unit tests
- existing planner eval suite
- PostgreSQL integration tests
- wheel/sdist build and installed-wheel smoke tests
- secret scan
- README relative-link and curated-example consistency check

Recommended follow-up checks, if low effort:

- dependency vulnerability scan
- Python version matrix for the minimum supported version and one current stable version
- coverage report with focus on executor, approval, integrity, and retry modules

## Success Criteria

- Every supported mutation path requires a valid v2 approval.
- Approval rejects any changed plan, summary, or source manifest.
- SQL-bearing executable metadata is derived from or exactly matched to approved source metadata.
- Destructive behavior cannot be enabled by config or CLI flags alone.
- Failed steps remain retryable and preserve attempt history.
- The built wheel includes and loads the planner prompt.
- CI proves an end-to-end PostgreSQL migration and all critical negative safety scenarios.
- The public approval example is complete and machine-validated.
- GitHub reports no open secret-scanning alerts.
- The README accurately describes enforcement and limitations.
- All existing unit tests and planner evals continue to pass.
- `v0.3.1` is tagged and released only after the above criteria pass.

## Release Checklist

- [ ] Exposed credential revoked or rotated
- [ ] GitHub secret-scanning alert resolved
- [ ] Secret-scan CI green
- [ ] Approval schema v2 implemented
- [ ] Plan, summary, and source-manifest digests enforced
- [ ] Approval mandatory for CLI, Streamlit, and agent execution
- [ ] Config-only destructive authorization removed
- [ ] SQL-bearing metadata bound to source manifest
- [ ] Raw executor no longer exposed as a supported public mutation path
- [ ] Retry and resume semantics fixed
- [ ] State bound to approved plan
- [ ] Prompt included in wheel
- [ ] Installed-wheel smoke test green
- [ ] PostgreSQL integration suite green
- [ ] Existing unit suite green
- [ ] Existing 10-case heuristic eval remains 10/10
- [ ] Example plan and state published and consistent
- [ ] README and `SECURITY.md` updated
- [ ] License selected and added
- [ ] Package version updated to `0.3.1`
- [ ] Git tag `v0.3.1` created
- [ ] GitHub release published

## Open Questions

1. Should the project use MIT or Apache-2.0 licensing? This requires an owner decision before release; Apache-2.0 is the safer default when explicit patent terms are desirable.
2. Should revoked credentials be purged from Git history after alert resolution? Revocation is mandatory; history rewriting is optional and operationally disruptive.
3. Should artifact digests use exact bytes or canonical JSON? This PRD recommends exact bytes because any post-approval edit should require reapproval.
4. Should a relocated artifact with the same digest be accepted? This PRD recommends yes, with an explicit caller-provided path and an audit message.
5. Should failed steps rerun automatically during `amo run --state`, or only through `amo retry`? The safer default is explicit retry; in either case, failed steps must never be treated as successful.
6. Should approval schema v1 remain renderable after execution support is removed? Read-only compatibility is acceptable, but execution must fail closed.

## Recommended Decisions

- Use exact-byte SHA-256 artifact digests.
- Accept relocated artifacts only when the digest matches and the operator supplies the new path explicitly.
- Require `amo retry` for failed-step re-execution; ordinary resume should skip succeeded steps and stop with a clear failed-step status.
- Keep legacy approvals read-only and require regeneration for execution.
- Prefer deterministic metadata hydration from the source manifest over string comparison when implementation cost is reasonable.
- Use `policy.py` for approval/destructive enforcement and a separate `integrity.py` for digests and manifest binding.
- Defer broad executor and Streamlit refactors until after the stabilization release.

## Definition of Done

`v0.3.1` is done only when the security incident is contained, all supported mutation paths enforce approval and artifact integrity, planner output cannot introduce untrusted executable SQL, retry behavior is correct, the installed package works outside an editable checkout, real PostgreSQL integration tests pass in CI, the public reviewer path is complete, and the release is tagged and published with accurate documentation.

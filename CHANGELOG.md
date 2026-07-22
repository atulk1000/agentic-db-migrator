# Changelog

## Unreleased

### Security

- Require approval-schema-v2 artifacts for every supported mutation entrypoint.
- Bind approvals to exact plan, pre-migration summary, and source-manifest bytes with SHA-256.
- Validate and hydrate executable metadata from the approved source manifest before database connection.
- Derive destructive execution authority only from the approval artifact.
- Add current-tree Gitleaks scanning to CI.

### Correctness

- Bind checkpoint state to the approved plan digest.
- Preserve failed and successful attempt history and rerun failed steps without replaying successful steps.
- Reject duplicate plan step IDs and retry plans that broaden, change, or reorder approved steps.
- Include approved grant and staged-materialized-view operations in filtered plans.

### Packaging and verification

- Package the versioned planner prompt in wheels.
- Build and install-test wheel and source distributions in CI.
- Add approval-chain consistency tests, README link validation, and a two-PostgreSQL integration workflow.
- License the project under the MIT License.

### Compatibility

- Legacy approval artifacts without `schema_version: "2"` and artifact digests cannot execute. Regenerate them after reviewing the current plan, summary, and source manifest.
- The package remains at `0.3.0` until the external credential incident is closed and all release gates pass.

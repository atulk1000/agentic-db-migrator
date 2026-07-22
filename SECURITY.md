# Security Policy

## Supported Code

Security fixes are applied to the current `main` branch. This project has not yet published a supported stable release line.

## Reporting a Vulnerability

Do not include credentials, exploit details, database contents, or other sensitive material in a public issue.

Use **Security > Report a vulnerability** in this repository if GitHub private vulnerability reporting is available. If it is unavailable, contact the maintainer through a private method listed on the maintainer's GitHub profile and provide only enough initial detail to establish a secure follow-up channel.

Include:

- the affected command, module, or artifact type
- the security boundary that can be bypassed
- minimal reproduction steps using synthetic data
- expected and observed behavior
- any known target mutation or credential exposure

Please allow a reasonable period for triage and remediation before public disclosure.

## Credential Exposure Response

Treat a committed credential as compromised even after it is removed from the current branch.

1. Revoke or rotate it at the provider immediately.
2. Remove it from the current tree and generated artifacts.
3. Review provider audit logs and repository secret-scanning alerts.
4. Resolve the alert only after revocation or rotation is confirmed.
5. Consider a coordinated history rewrite if reducing historical exposure is worth the disruption to existing clones and forks.
6. Add or strengthen an automated regression scan.

Secret scanning prevents recurrence; it does not invalidate a credential that has already been exposed.

## Security Boundaries

- Every supported mutation entrypoint requires an approval-schema-v2 artifact.
- Approval binds the exact plan, pre-migration summary, and source manifest bytes by SHA-256.
- Legacy approvals are intentionally rejected and must be regenerated after review.
- Destructive authority comes from the approval artifact, not from config or a later CLI flag.
- Planner-provided SQL-bearing metadata is replaced with or checked against the approved source manifest before execution.
- `approved_by` is an audit label supplied by the operator; it is not authenticated identity.

Never commit production credentials or place them in approval artifacts, examples, screenshots, state files, logs, or issue reports. Use environment-variable substitution in local runtime configuration.

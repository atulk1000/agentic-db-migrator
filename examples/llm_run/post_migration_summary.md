# Post-Migration Summary

Success: true

Steps completed: 5/5

Failed steps: 0

Tables verified: 1

Verification ok: true

## AI Planner Contribution

- Recommended chunking on `event_ts` for a large event table.
- Selected rowcount verification for a high-volume table to keep validation cost bounded.
- Recommended post-load `VACUUM ANALYZE` before cutover.
- Flagged a large materialized view as a staged rebuild candidate.

# LLM Planner Example

This folder shows the AI-facing artifact chain for a small migration planning case:

- `source_manifest.json`
- `target_manifest.json`
- `manifest_diff.json`
- `llm_raw_response.json`
- `repaired_plan.json`
- `validated_plan.json`
- `approval.json`
- `post_migration_summary.md`

The raw response is intentionally close to executor-ready JSON but still carries planner metadata explaining why the model chose stronger verification and staged materialized view handling. In a live run, the planner adapter normalizes common model mistakes, validates the result against the strict plan schema, and falls back to the heuristic planner if validation fails.

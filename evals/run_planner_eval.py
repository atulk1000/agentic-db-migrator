from __future__ import annotations

import argparse
import importlib
import inspect
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from amo.core.planners.models import validate_plan_document  # noqa: E402

PLANNER_MODULES = {
    "heuristic": "amo.core.planners.heuristic_planner",
    "gemini": "amo.core.planners.gemini",
    "openai": "amo.core.planners.openai",
    "demo": "amo.core.planners.remote_demo",
}


def _table_key(step: dict[str, Any]) -> str | None:
    schema = step.get("schema")
    table = step.get("table")
    if not schema or not table:
        return None
    return f"{schema}.{table}"


def _run_case(planner_name: str, case_path: Path) -> dict[str, Any]:
    case = json.loads(case_path.read_text(encoding="utf-8"))
    planner = importlib.import_module(PLANNER_MODULES[planner_name])
    expected = case.get("expected", {})

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        manifest_path = tmp_path / "source_manifest.json"
        context_path = tmp_path / "manifest_diff.json"
        manifest_path.write_text(json.dumps(case["manifest"]), encoding="utf-8")
        context_path.write_text(json.dumps(case.get("manifest_diff", {})), encoding="utf-8")

        start = time.perf_counter()
        try:
            kwargs = {"manifest_path": str(manifest_path)}
            signature = inspect.signature(planner.generate_plan)
            if "context_path" in signature.parameters:
                kwargs["context_path"] = str(context_path)
            if "objective" in signature.parameters:
                kwargs["objective"] = "planner_eval"
            plan = planner.generate_plan(**kwargs)
            plan = validate_plan_document(plan)
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            validation_passed = True
            error = None
        except Exception as exc:
            plan = {"steps": [], "planner": planner_name, "planner_metadata": {}}
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            validation_passed = False
            error = str(exc)

    steps = plan.get("steps", [])
    ops = {step.get("op") for step in steps}
    tables = {_table_key(step) for step in steps if _table_key(step)}
    forbidden_ops = sorted(op for op in expected.get("must_not_use_ops", []) if op in ops)
    missing_required_ops = sorted(
        op for op in expected.get("must_include_ops", []) if op not in ops
    )
    missing_required_tables = sorted(
        table for table in expected.get("must_include_tables", []) if table not in tables
    )

    missing_table_ops: list[str] = []
    for table, required_ops in expected.get("must_include_table_ops", {}).items():
        table_ops = {step.get("op") for step in steps if _table_key(step) == table}
        for op in required_ops:
            if op not in table_ops:
                missing_table_ops.append(f"{table}:{op}")

    sample_hash_missing: list[str] = []
    for table in expected.get("requires_sample_hash_for", []):
        matching = [
            step for step in steps if _table_key(step) == table and step.get("op") == "verify_table"
        ]
        if not any((step.get("validate") or {}).get("sample_hash") for step in matching):
            sample_hash_missing.append(table)

    partition_fidelity_missing: list[str] = []
    for table in expected.get("requires_partition_fidelity_for", []):
        matching = [
            step for step in steps if _table_key(step) == table and step.get("op") == "verify_table"
        ]
        if not any((step.get("validate") or {}).get("partition_fidelity") for step in matching):
            partition_fidelity_missing.append(table)

    transfer_expectation_failures: list[str] = []
    for rule in expected.get("requires_transfer_for", []):
        table = rule["table"]
        matching = [
            step for step in steps if _table_key(step) == table and step.get("op") == "copy_table"
        ]
        if not matching:
            transfer_expectation_failures.append(f"{table}:copy_table")
            continue
        transfer = matching[0].get("transfer") or {}
        for field, expected_value in rule.items():
            if field == "table":
                continue
            if transfer.get(field) != expected_value:
                transfer_expectation_failures.append(f"{table}:{field}")

    matview_strategy_missing: list[str] = []
    for rule in expected.get("requires_matview_strategy_for", []):
        expected_schema = rule["schema"]
        expected_name = rule["name"]
        expected_strategy = rule["strategy"]
        found = False
        for step in steps:
            if step.get("schema") != expected_schema:
                continue
            for matview in step.get("matviews", []):
                if (
                    matview.get("name") == expected_name
                    and matview.get("strategy") == expected_strategy
                ):
                    found = True
                    break
            if found:
                break
        if not found:
            matview_strategy_missing.append(
                f"{expected_schema}.{expected_name}:{expected_strategy}"
            )

    passed = (
        validation_passed
        and not forbidden_ops
        and not missing_required_ops
        and not missing_required_tables
        and not missing_table_ops
        and not sample_hash_missing
        and not partition_fidelity_missing
        and not transfer_expectation_failures
        and not matview_strategy_missing
    )

    return {
        "case": case_path.stem,
        "description": case.get("description", ""),
        "planner": plan.get("planner", planner_name),
        "passed": passed,
        "validation_passed": validation_passed,
        "fallback_reason": (plan.get("planner_metadata") or {}).get("reason"),
        "elapsed_ms": elapsed_ms,
        "step_count": len(steps),
        "forbidden_ops": forbidden_ops,
        "missing_required_ops": missing_required_ops,
        "missing_required_tables": missing_required_tables,
        "missing_table_ops": missing_table_ops,
        "sample_hash_missing": sample_hash_missing,
        "partition_fidelity_missing": partition_fidelity_missing,
        "transfer_expectation_failures": transfer_expectation_failures,
        "matview_strategy_missing": matview_strategy_missing,
        "error": error,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run planner quality checks against saved cases.")
    parser.add_argument("--planner", choices=sorted(PLANNER_MODULES), default="heuristic")
    parser.add_argument("--cases-dir", default=str(Path(__file__).with_name("cases")))
    parser.add_argument("--out", default=str(Path(__file__).with_name("latest_eval_report.json")))
    args = parser.parse_args()

    cases_dir = Path(args.cases_dir)
    results = [_run_case(args.planner, path) for path in sorted(cases_dir.glob("*.json"))]
    report = {
        "planner": args.planner,
        "case_count": len(results),
        "passed_count": sum(1 for item in results if item["passed"]),
        "schema_validation_pass_rate": (
            sum(1 for item in results if item["validation_passed"]) / len(results) if results else 0
        ),
        "fallback_count": sum(1 for item in results if item.get("fallback_reason")),
        "results": results,
    }
    Path(args.out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

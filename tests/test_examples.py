from __future__ import annotations

import json
from pathlib import Path

import pytest

from amo.core.policy import build_approved_execution_bundle


@pytest.mark.parametrize(
    "approval_path",
    [
        "examples/approval_workflow/approval.json",
        "examples/llm_run/approval.json",
    ],
)
def test_checked_in_approval_examples_are_complete_and_integrity_bound(approval_path):
    bundle = build_approved_execution_bundle(approval_path=approval_path)

    assert bundle.filtered_plan["steps"]
    assert bundle.approval.schema_version == "2"
    assert bundle.plan_path.exists()
    assert bundle.summary_path.exists()
    assert bundle.source_manifest_path.exists()


def test_checked_in_state_is_bound_to_the_approved_plan():
    approval = json.loads(Path("examples/approval_workflow/approval.json").read_text())
    state = json.loads(Path("examples/approval_workflow/state.json").read_text())

    assert state["schema_version"] == "2"
    assert state["plan_sha256"] == approval["plan"]["sha256"]
    assert all(item["status"] == "succeeded" for item in state["completed"].values())

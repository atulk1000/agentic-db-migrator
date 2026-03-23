from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import error, request

from amo.core.planners.llm_common import parse_normalize_and_validate_llm_plan
from amo.core.planners.llm_stub import generate_fallback_plan
from amo.core.planners.models import MigrationManifest


DEFAULT_TIMEOUT_SECONDS = 30


def _post_json(url: str, payload: Dict[str, Any], bearer_token: Optional[str], timeout_seconds: int) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    req = request.Request(url=url, data=body, headers=headers, method="POST")
    with request.urlopen(req, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def generate_plan(manifest_path: str) -> dict:
    endpoint = os.environ.get("DEMO_PLANNER_URL")
    token = os.environ.get("DEMO_PLANNER_TOKEN")
    timeout_seconds = int(os.environ.get("DEMO_PLANNER_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))

    if not endpoint:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="demo_stub",
            reason="DEMO_PLANNER_URL is not set.",
        )

    manifest = MigrationManifest.model_validate(json.loads(Path(manifest_path).read_text()))
    payload = {
        "manifest": manifest.model_dump(mode="python", by_alias=True),
        "requested_planner": "demo",
        "requested_provider": "gemini",
    }

    try:
        response = _post_json(endpoint, payload, bearer_token=token, timeout_seconds=timeout_seconds)
        return parse_normalize_and_validate_llm_plan(
            raw_plan=response.get("plan", response),
            manifest=manifest,
            planner_name="demo",
            planner_metadata={
                "mode": "remote_demo",
                "endpoint": endpoint,
            },
        )
    except error.HTTPError as exc:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="demo_stub",
            reason=f"Demo planner HTTP error: {exc.code}",
        )
    except Exception as exc:
        return generate_fallback_plan(
            manifest_path=manifest_path,
            planner_name="demo_stub",
            reason=f"Demo planner request failed: {exc}",
        )

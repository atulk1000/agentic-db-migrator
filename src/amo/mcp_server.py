from __future__ import annotations

import argparse
import json

from mcp.server.fastmcp import FastMCP

from amo import mcp_tools

mcp = FastMCP("Agentic DB Migration Orchestrator")


@mcp.tool()
def analyze_databases(
    config_path: str = "config.yaml",
    planner: str = "heuristic",
    migration_mode: str = "safe_sync",
    out_dir: str | None = None,
) -> dict:
    """Analyze source/target databases and create safe review artifacts."""
    return mcp_tools.analyze_databases(
        config_path=config_path,
        planner=planner,
        migration_mode=migration_mode,  # type: ignore[arg-type]
        out_dir=out_dir,
    )


@mcp.tool()
def list_artifacts(run_dir: str) -> dict:
    """List known migration artifacts in a run directory."""
    return mcp_tools.list_artifacts(run_dir)


@mcp.tool()
def read_artifact(path: str) -> dict:
    """Read a safe JSON or Markdown migration artifact."""
    return mcp_tools.read_artifact(path)


@mcp.tool()
def validate_plan(plan_path: str) -> dict:
    """Validate a migration plan artifact."""
    return mcp_tools.validate_plan(plan_path)


@mcp.tool()
def critique_plan(
    plan_path: str,
    summary_path: str,
    diff_path: str,
    out_path: str | None = None,
) -> dict:
    """Run planner critic against existing analysis artifacts."""
    return mcp_tools.critique_plan(plan_path, summary_path, diff_path, out_path)


@mcp.tool()
def generate_clarification_questions(
    summary_path: str,
    diff_path: str,
    out_path: str | None = None,
) -> dict:
    """Generate operator clarification questions."""
    return mcp_tools.generate_clarification_questions(summary_path, diff_path, out_path)


@mcp.tool()
def generate_plan_rationale(
    plan_path: str,
    summary_path: str,
    critique_path: str,
    out_path: str | None = None,
) -> dict:
    """Generate a Markdown rationale for a migration plan."""
    return mcp_tools.generate_plan_rationale(plan_path, summary_path, critique_path, out_path)


@mcp.tool()
def build_post_migration_summary(
    plan_path: str,
    state_path: str,
    pre_summary_path: str,
    report_path: str | None = None,
    out_path: str | None = None,
) -> dict:
    """Build post-migration summary and failure analysis from artifacts."""
    return mcp_tools.build_post_summary(
        plan_path=plan_path,
        state_path=state_path,
        pre_summary_path=pre_summary_path,
        report_path=report_path,
        out_path=out_path,
    )


@mcp.resource("policy://allowed-operations", mime_type="application/json")
def allowed_operations() -> str:
    """Return the MCP server safety policy."""
    return json.dumps(mcp_tools.allowed_operations_policy(), indent=2, sort_keys=True)


@mcp.resource("manifest://source/latest", mime_type="application/json")
def latest_source_manifest() -> str:
    """Return latest source manifest artifact."""
    return mcp_tools.latest_artifact_text("source_manifest.json")


@mcp.resource("manifest://target/latest", mime_type="application/json")
def latest_target_manifest() -> str:
    """Return latest target manifest artifact."""
    return mcp_tools.latest_artifact_text("target_manifest.json")


@mcp.resource("drift://latest", mime_type="application/json")
def latest_drift() -> str:
    """Return latest manifest drift artifact."""
    return mcp_tools.latest_artifact_text("manifest_diff.json")


@mcp.resource("plan://latest", mime_type="application/json")
def latest_plan() -> str:
    """Return latest migration plan artifact."""
    return mcp_tools.latest_artifact_text("plan.json")


@mcp.resource("summary://pre/latest", mime_type="application/json")
def latest_pre_summary() -> str:
    """Return latest pre-migration summary artifact."""
    return mcp_tools.latest_artifact_text("pre_migration_summary.json")


@mcp.resource("summary://post/latest", mime_type="application/json")
def latest_post_summary() -> str:
    """Return latest post-migration summary artifact."""
    return mcp_tools.latest_artifact_text("post_migration_summary.json")


@mcp.resource("critique://latest", mime_type="application/json")
def latest_critique() -> str:
    """Return latest planner critique artifact."""
    return mcp_tools.latest_artifact_text("planner_critique.json")


@mcp.resource("questions://latest", mime_type="application/json")
def latest_questions() -> str:
    """Return latest clarification questions artifact."""
    return mcp_tools.latest_artifact_text("clarification_questions.json")


@mcp.resource("rationale://latest", mime_type="text/markdown")
def latest_rationale() -> str:
    """Return latest planner rationale artifact."""
    return mcp_tools.latest_artifact_text("planner_rationale.md")


@mcp.prompt()
def analyze_drift() -> str:
    return (
        "Use analyze_databases, then read manifest_diff and pre_migration_summary. "
        "Summarize missing tables, metadata drift, manual-review items, and recommended next action. "
        "Do not execute migration."
    )


@mcp.prompt()
def review_safe_migration_plan() -> str:
    return (
        "Read plan, pre-summary, planner critique, clarification questions, and rationale. "
        "Explain whether the plan is safe to approve. List manual-review blockers. "
        "Do not execute migration."
    )


@mcp.prompt()
def debug_failed_verification() -> str:
    return (
        "Read state, verification report, post summary, and failure analysis. "
        "Explain likely failure cause and safest next command. Do not rerun migration automatically."
    )


@mcp.prompt()
def prepare_approval() -> str:
    return (
        "Review summary and clarification questions. Suggest included/excluded tables and whether "
        "destructive actions should remain disabled. Ask the user for final confirmation before any "
        "approval or execution step."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Agentic DB Migrator MCP server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http", "sse"],
        default="stdio",
        help="MCP transport to use.",
    )
    parser.add_argument("--host", default=None, help="Host for HTTP/SSE transports.")
    parser.add_argument("--port", type=int, default=None, help="Port for HTTP/SSE transports.")
    args = parser.parse_args()

    if args.host:
        mcp.settings.host = args.host
    if args.port:
        mcp.settings.port = args.port

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()

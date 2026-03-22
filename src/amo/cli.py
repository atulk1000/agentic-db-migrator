import typer
from amo.core.config import load_env, load_config
from amo.core.manifest_builder import build_manifest, write_manifest
from amo.core.planners.models import validate_plan_document
from typing import Optional
from datetime import datetime
from pathlib import Path

app = typer.Typer()

@app.command()
def discover(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    out: str = typer.Option("manifest.json", help="Output manifest path"),
):
    load_env(".env")
    cfg = load_config(config)
    manifest = build_manifest(cfg)
    write_manifest(manifest, out)
    print(f"Wrote manifest to {out} ({len(manifest.get('tables', []))} tables, {len(manifest.get('errors', []))} errors)")

@app.command()
def plan(
    manifest: str = typer.Option("manifest.json", help="Input manifest path"),
    planner: str = typer.Option("heuristic", help="Planner to use: heuristic | openai | ollama"),
    out: str = typer.Option("plan.json", help="Output plan path"),
):
    planner_modules = {
        "heuristic": "amo.core.planners.heuristic_planner",
        "openai": "amo.core.planners.openai",
        "ollama": "amo.core.planners.ollama",
    }

    mod_path = planner_modules.get(planner)
    if not mod_path:
        raise typer.BadParameter(f"Unknown planner: {planner}. Use heuristic|openai|ollama")

    # Each planner module should expose: generate_plan(manifest_path: str) -> dict
    mod = __import__(mod_path, fromlist=["generate_plan"])
    plan_obj = validate_plan_document(mod.generate_plan(manifest_path=manifest))

    # Keep write_plan in one place (I assume heuristic owns the JSON shape)
    from amo.core.planners.heuristic_planner import write_plan
    write_plan(plan_obj, out)

    print(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")


@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: Optional[str] = typer.Option(
        None,
        "--state",
        help="Checkpoint state file. If omitted, a timestamped file is created under runs/.",
    ),
    fresh: bool = typer.Option(
        False,
        "--fresh",
        help="Start a fresh run (ignore existing state file if provided).",
    ),
    truncate_first: Optional[bool] = typer.Option(
        None,
        "--truncate/--no-truncate",
        help="Truncate target tables before COPY (overrides engine.copy.truncate_first)",
    ),
    allow_destructive: Optional[bool] = typer.Option(
        None,
        "--allow-destructive/--no-allow-destructive",
        help="Allow destructive ops like TRUNCATE (overrides engine.allow_destructive)",
    ),
):
    from amo.core.executor import execute

    load_env(".env")
    cfg = load_config(config)

    # ✅ apply CLI overrides
    if truncate_first is not None:
        cfg.setdefault("engine", {}).setdefault("copy", {})["truncate_first"] = bool(truncate_first)

    if allow_destructive is not None:
        cfg.setdefault("engine", {})["allow_destructive"] = bool(allow_destructive)

    # ✅ default state path = timestamped per run
    if state is None:
        Path("runs").mkdir(exist_ok=True)
        state = f"runs/state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # ✅ fresh run: delete existing state file if any
    if fresh and Path(state).exists():
        Path(state).unlink()

    execute(cfg=cfg, plan_path=plan, state_path=state)
    print(f"Run complete. State saved to {state}")

@app.command()
def verify(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    out: str = typer.Option("report.json", help="Output report path"),
):
    from amo.core.verifier import verify_plan, write_report

    load_env(".env")
    cfg = load_config(config)

    report = verify_plan(cfg=cfg, plan_path=plan)
    write_report(report, out)
    print(f"Wrote report to {out}")

if __name__ == "__main__":
    app()

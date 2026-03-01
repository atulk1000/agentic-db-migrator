import typer
from amo.core.config import load_env, load_config
from amo.core.manifest_builder import build_manifest, write_manifest
from typing import Optional

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
    if planner == "heuristic":
        from amo.core.heuristic_planner import generate_plan, write_plan
        plan_obj = generate_plan(manifest_path=manifest)
        write_plan(plan_obj, out)
        print(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")
        return

    if planner == "openai":
        from amo.planners.openai_stub import generate_plan as llm_generate
        plan_obj = llm_generate(manifest_path=manifest)
        from amo.core.heuristic_planner import write_plan
        write_plan(plan_obj, out)
        print(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")
        return

    if planner == "ollama":
        from amo.planners.ollama_stub import generate_plan as llm_generate
        plan_obj = llm_generate(manifest_path=manifest)
        from amo.core.heuristic_planner import write_plan
        write_plan(plan_obj, out)
        print(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")
        return

    raise typer.BadParameter(f"Unknown planner: {planner}. Use heuristic|openai|ollama")


@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: str = typer.Option("state.json", help="Checkpoint state file"),
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

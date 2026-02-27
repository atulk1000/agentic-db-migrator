import typer
from amo.core.config import load_env, load_config
from amo.core.manifest_builder import build_manifest, write_manifest

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
    out: str = typer.Option("plan.json", help="Output plan path"),
):
    from amo.core.heuristic_planner import generate_plan, write_plan

    plan_obj = generate_plan(manifest_path=manifest)
    write_plan(plan_obj, out)
    print(f"Wrote plan to {out} ({len(plan_obj.get('steps', []))} steps)")


@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: str = typer.Option("state.json", help="Checkpoint state file"),
):
    from amo.core.executor import execute

    load_env(".env")
    cfg = load_config(config)

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

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

    print(f"Wrote manifest to {out} ({len(manifest.get('tables', []))} tables, "
          f"{len(manifest.get('errors', []))} errors)")

@app.command()
def plan(
    manifest: str = typer.Option("manifest.json", help="Input manifest path"),
    out: str = typer.Option("plan.json", help="Output plan path"),
):
    print("Planning...")
    # TODO: from amo.core.heuristic_planner import generate_plan, write_plan
    # TODO: plan_obj = generate_plan(manifest)
    # TODO: write_plan(plan_obj, out)

@app.command()
def run(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    state: str = typer.Option("state.json", help="Checkpoint state file"),
):
    load_env(".env")
    cfg = load_config(config)
    print("Running migration with engine:", cfg["engine"]["type"])
    # TODO: from amo.core.executor import execute
    # TODO: execute(cfg, plan_path=plan, state_path=state)

@app.command()
def verify(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    plan: str = typer.Option("plan.json", help="Path to plan JSON"),
    out: str = typer.Option("report.json", help="Output report path"),
):
    load_env(".env")
    cfg = load_config(config)
    print("Verifying...")
    # TODO: from amo.core.verifier import verify_plan
    # TODO: report = verify_plan(cfg, plan_path=plan)
    # TODO: write_report(report, out)

if __name__ == "__main__":
    app()

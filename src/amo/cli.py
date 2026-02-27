import typer
from amo.core.config import load_env, load_config

app = typer.Typer()

@app.command()
def discover(config: str = typer.Option("config.yaml", help="Path to config YAML")):
    load_env(".env")
    cfg = load_config(config)
    print("Loaded config OK")
    from amo.core.manifest_builder import build_manifest, write_manifest

@app.command()
def plan(manifest: str = typer.Option("manifest.json"), out: str = typer.Option("plan.json")):
    # TODO: planner.generate_plan(...)
    print("Planning...")

@app.command()
def run(config: str = typer.Option("config.yaml"), plan: str = typer.Option("plan.json")):
    load_env(".env")
    cfg = load_config(config)
    print("Running migration with engine:", cfg["engine"]["type"])
    # TODO: executor.execute(cfg, plan)

@app.command()
def verify(config: str = typer.Option("config.yaml"), plan: str = typer.Option("plan.json")):
    load_env(".env")
    cfg = load_config(config)
    print("Verifying...")
    # TODO: verifier.verify(cfg, plan)

if __name__ == "__main__":
    app()

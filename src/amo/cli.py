import typer

app = typer.Typer()

@app.command()
def discover():
    print("Discovering schema...")

@app.command()
def plan():
    print("Generating execution plan...")

@app.command()
def run():
    print("Executing migration...")

@app.command()
def verify():
    print("Verifying migration...")

if __name__ == "__main__":
    app()

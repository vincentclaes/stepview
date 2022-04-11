import typer

from stepview.tui import StepViewTUI

app = typer.Typer()


def run():
    """entrypoint for stepview."""
    app()


def parse_string_to_list(profiles: str) -> list:
    return profiles.split(" ")


@app.command()
def stepview(profiles: str = typer.Option(..., callback=parse_string_to_list)):
    # StepViewTUI(aws_profiles=profiles).run(title="STEPVIEW")
    StepViewTUI().run(title="STEPVIEW")

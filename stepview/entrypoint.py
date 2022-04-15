from typing import List

import typer

from stepview.tui import StepViewTUI

app = typer.Typer()


def run():
    """entrypoint for stepview."""
    app()


def parse_string_to_list(profiles: str) -> list:
    """Watch out for some dirty code. afaik, typer cannot parse a list of
    strings out of the box. If you know how to do this, please let me know by
    creating a github issue.

    A list of Paths does work:
    https://typer.tiangolo.com/tutorial/multiple-values/arguments-with-multiple-values/
    """
    return profiles[0].split(",")


@app.command()
def stepview(
    profiles: List[str] = typer.Option(
        ...,
        callback=parse_string_to_list,
        help="specify the aws profiles you want to use as a comma seperated string. "
        "For example '--profiles profile1,profile2,profile3,...'",
    ),
    period: str = typer.Option(
        "day",
        help="specify the time period for which you wish to look back."
        "you can use 'day', 'week', 'month', 'year'",
    ),
):
    StepViewTUI.run(title="STEPVIEW", aws_profiles=profiles, period=period)


if __name__ == "__main__":
    run()

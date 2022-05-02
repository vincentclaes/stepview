from typing import List

import typer

from stepview.data import PERIODS_LIST, DAY
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
    profile: List[str] = typer.Option(
        default=["default"],
        callback=parse_string_to_list,
        help="specify the aws profile you want to use as a comma seperated string. "
        "For example '--profile profile1,profile2,profile3,...'",
    ),
    period: str = typer.Option(
        default="day",
        help="specify the time period for which you wish to look back. "
        f"""You can choose from the values: {', '.join(PERIODS_LIST.keys())}""",
    ),
):
    StepViewTUI.run(
        title=f"STEPVIEW (period: {period})", aws_profiles=profile, period=period
    )


if __name__ == "__main__":
    run()

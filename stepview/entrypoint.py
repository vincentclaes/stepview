import logging
import warnings
from typing import List

import typer
from rich import traceback
from rich.console import Console

from stepview import logger
from stepview import set_logger_3rd_party_lib
from stepview.data import main
from stepview.data import Time
from stepview.tui import StepViewTUI

warnings.simplefilter("ignore", ResourceWarning)

app = typer.Typer()
console = Console()


def run():
    """entrypoint for stepview."""
    app()


def parse_string_to_list(arguments: str) -> list:
    """Watch out for some dirty code. afaik, typer cannot parse a list of
    strings out of the box. If you know how to do this, please let me know by
    creating a github issue.

    A list of Paths does work:
    https://typer.tiangolo.com/tutorial/multiple-values/arguments-with-multiple-values/
    """
    if arguments:
        return arguments[0].split(",")
    return []


@app.command()
def stepview(
    profile: List[str] = typer.Option(
        default=["default"],
        callback=parse_string_to_list,
        help="Specify the aws profile you want to use as a comma seperated string. "
        "For example '--profile profile1,profile2,profile3,...'",
    ),
    period: str = typer.Option(
        default=Time.DAY,
        help="Specify the time period for which you wish to look back. "
        f"""You can choose from the values: {', '.join(Time.get_time_variables())}""",
    ),
    tags: List[str] = typer.Option(
        default=[],
        callback=parse_string_to_list,
        help="Specify the tags you want to filter your stepfunctions statemachine. "
        "Provide your tags as comma seperated key words: --tags foo=bar,baz=qux",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Use --verbose to set verbose logging."
    ),
):
    _tags = []
    if tags is not None:
        for tag in tags:
            if "=" in tag:
                key, value = tag.split("=")
                _tags.append((key, value))
    if verbose:
        set_logger_3rd_party_lib(logging_level=logging.DEBUG)
    try:
        table, _ = main(aws_profiles=profile, period=period, tags=_tags)
    except Exception as e:
        console.print_exception()
        console.log("Woops something went wrong.")
        console.log(
            "Remember that if you need  to fetch a lot (hundreds to thousands) of statemachines \n"
            "make sure to add --tags to filter or reduce the number of --profile"
        )
        console.log("")
        console.log(
            "Let us help you, create a github issue here: https://bit.ly/3wHh70g"
        )
        console.log("")
    else:
        StepViewTUI.run(
            title=f"STEPVIEW (period: {period}, tags: {', '.join(tags)})", table=table
        )


if __name__ == "__main__":
    run()

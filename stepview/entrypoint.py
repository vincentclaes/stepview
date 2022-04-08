from collections import defaultdict

import boto3
from rich.table import Table

from stepview import logger


def _list_executions_for_state_machine(sfn_client: object, state_machine_arn: str):
    return sfn_client.list_executions(stateMachineArn=state_machine_arn)


def parse_aws_arn(arn):
    """parse arn into its logical pieces https://gist.github.com/gene1wood/5299
    969edc4ef21d8efcfea52158dd40?permalink_comment_id=2351697#gistcomment-23516
    97.

    :param arn: full aws arn
    :return:
    """
    elements = arn.split(":", 5)
    result = {
        "arn": elements[0],
        "partition": elements[1],
        "service": elements[2],
        "region": elements[3],
        "account": elements[4],
        "resource": elements[5],
        "resource_type": None,
    }
    if "/" in result["resource"]:
        result["resource_type"], result["resource"] = result["resource"].split("/", 1)
    elif ":" in result["resource"]:
        result["resource_type"], result["resource"] = result["resource"].split(":", 1)
    return result


def get_statemachine_url(state_machine_arn: str, region: str) -> str:
    return f"https://console.aws.amazon.com/states/home?region={region}#/statemachines/view/{state_machine_arn}"


def main(aws_profiles: list):
    table = Table()
    table.add_column("State Machine", justify="right")
    table.add_column("Profile")
    table.add_column("Account")
    table.add_column("Region")
    table.add_column("Succeed (%)")
    table.add_column("Failure (absolute)")
    table.add_column("Running (absolute)")
    for profile_name in aws_profiles:
        sfn_client = boto3.Session(profile_name=profile_name).client("stepfunctions")
        state_machines = sfn_client.list_state_machines().get("stateMachines")
        if state_machines:
            for state_machine in state_machines:
                states = defaultdict(int)
                state_machine_arn = state_machine.get("stateMachineArn")
                executions = _list_executions_for_state_machine(
                    sfn_client=sfn_client, state_machine_arn=state_machine_arn
                )
                for execution in executions.get("executions"):
                    states[execution["status"]] += 1
                total_executions = sum(states.values())
                succeeded = states["SUCCEEDED"]
                succeeded_perc = (
                    (succeeded / total_executions) * 100 if total_executions > 0 else 0
                )
                failed = states["FAILED"]
                running = states["RUNNING"]
                arn_parsed = parse_aws_arn(state_machine_arn)
                account = arn_parsed.get("account")
                region = arn_parsed.get("region")
                state_machine_name = state_machine.get("name")
                state_machine_url = get_statemachine_url(
                    state_machine_arn=state_machine_arn, region=region
                )
                state_machine_name_url = (
                    f"[link={state_machine_url}]{state_machine_name}[/link]"
                )
                table.add_row(
                    state_machine_name_url,
                    profile_name,
                    account,
                    region,
                    f"{succeeded_perc}",
                    f"{failed}",
                    f"{running}",
                )
        else:
            logger.info(f"no statemachines found for profile {profile_name}")
    return table


from textual import events
from textual.app import App
from textual.widgets import Header, Footer, ScrollView


class StepViewTUI(App):
    """StepView shows a table with stepfunction statemachine summaries."""

    async def on_load(self, event: events.Load) -> None:
        """Bind keys with the app loads (but before entering application
        mode)"""
        await self.bind("q", "quit", "Quit")
        await self.bind("escape", "quit", "Quit")

    async def on_mount(self, event: events.Mount) -> None:
        """Create and dock the widgets."""

        body = ScrollView(gutter=1)

        await self.view.dock(Header(), edge="top")
        await self.view.dock(Footer(), edge="bottom")
        await self.view.dock(body, edge="right")

        async def get_stepfunction_data():
            table = main(aws_profiles=["datajob", "default"])
            await body.update(table)

        await self.call_later(get_stepfunction_data)


StepViewTUI.run(title="STEPVIEW")

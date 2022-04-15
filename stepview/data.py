from collections import defaultdict
from dataclasses import dataclass

import boto3
import pendulum
from rich.table import Table

from stepview import logger

__now = pendulum.now()
__yesterday = __now.subtract(days=1)


@dataclass
class States:
    """States that we pass to the TUI table."""

    total_executions: int
    succeeded: str
    succeeded_perc: str
    failed: str
    running: str


def main(aws_profiles: list):
    table = Table()
    table.add_column("State Machine", justify="right")
    table.add_column("Profile")
    table.add_column("Account")
    table.add_column("Region")
    table.add_column("Total")
    table.add_column("Succeed (%)")
    table.add_column("Failure (absolute)")
    table.add_column("Running (absolute)")
    for profile_name in aws_profiles:
        sfn_client = boto3.Session(profile_name=profile_name).client("stepfunctions")
        state_machines = sfn_client.list_state_machines().get("stateMachines")
        if state_machines:
            for state_machine in state_machines:
                state_machine_arn = state_machine.get("stateMachineArn")
                states = get_all_states_of_executions(
                    sfn_client=sfn_client, state_machine_arn=state_machine_arn
                )
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
                    f"{states.total_executions}" f"{states.succeeded_perc}",
                    f"{states.failed}",
                    f"{states.running}",
                )
        else:
            logger.info(f"no statemachines found for profile {profile_name}")
    return table


def get_all_states_of_executions(sfn_client: object, state_machine_arn: str) -> States:

    states = defaultdict(int)
    states = get_executions_for_statemachine(sfn_client, state_machine_arn, states)
    total_executions = sum(states.values())
    succeeded = states["SUCCEEDED"]
    succeeded_perc = (succeeded / total_executions) * 100 if total_executions > 0 else 0
    failed = states["FAILED"]
    running = states["RUNNING"]

    return States(
        total_executions=total_executions,
        succeeded=succeeded,
        succeeded_perc=succeeded_perc,
        failed=failed,
        running=running,
    )


def get_executions_for_statemachine(
    sfn_client: object,
    state_machine_arn: str,
    states: defaultdict,
    nextToken: str = None,
) -> defaultdict:
    executions = list_executions_for_state_machine(
        sfn_client=sfn_client, state_machine_arn=state_machine_arn, nextToken=nextToken
    )
    for execution in executions.get("executions"):
        start_date = execution.get("startDate")
        period = pendulum.period(__now, pendulum.instance(start_date)).days
        if period >= 0:
            states[execution["status"]] += 1
        else:
            logger.debug("we only want executions of the last day.")
            continue
    else:
        logger.debug("for loop ended normally, checking if we have a next token.")
        if executions.get("nextToken"):
            states = get_executions_for_statemachine(
                sfn_client=sfn_client,
                state_machine_arn=state_machine_arn,
                states=states,
                nextToken=executions.get("nextToken"),
            )
    return states


def get_statemachine_url(state_machine_arn: str, region: str) -> str:
    return f"https://console.aws.amazon.com/states/home?region={region}#/statemachines/view/{state_machine_arn}"


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


def list_executions_for_state_machine(
    sfn_client: object, state_machine_arn: str, nextToken: str
):
    if nextToken is None:
        executions = sfn_client.list_executions(stateMachineArn=state_machine_arn)
    else:
        executions = sfn_client.list_executions(
            stateMachineArn=state_machine_arn, nextToken=nextToken
        )
    return executions

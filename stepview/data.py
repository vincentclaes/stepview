import concurrent.futures
import boto3
import pendulum

from rich.table import Table
from dataclasses import dataclass
from stepview import logger


@dataclass
class States:
    """States that we pass to the TUI table."""

    total_executions: int
    succeeded: str
    succeeded_perc: str
    failed: str
    running: str
    aborted: str
    timed_out: str
    throttled: str


@dataclass
class Periods:
    """We use Periods class to get the datetime range."""

    start_date_of_period: pendulum.DateTime
    now: pendulum.DateTime
    granularity: str

    def get_difference_in_seconds(self):
        return (self.now - self.start_date_of_period).in_seconds()


class MetricNames:
    """
    Metric names we can fetch from cloudwatch.
    more info here:
    https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html#cloudwatch-step-functions-execution-metrics
    """

    EXECUTIONS_STARTED = "ExecutionsStarted"
    EXECUTIONS_SUCCEEDED = "ExecutionsSucceeded"
    EXECUTIONS_FAILED = "ExecutionsFailed"
    EXECUTIONS_ABORTED = "ExecutionsAborted"
    EXECUTIONS_TIMED_OUT = "ExecutionsTimedOut"
    EXECUTION_THROTTLED = "ExecutionThrottled"


class Time:
    MINUTE = "minute"
    HOUR = "hour"
    TODAY = "today"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


NOW = pendulum.now()

PERIODS_MAPPING = {
    Time.MINUTE: Periods(NOW.subtract(minutes=1), NOW, "microseconds"),
    Time.HOUR: Periods(NOW.subtract(hours=1), NOW, "seconds"),
    Time.TODAY: Periods(NOW.start_of("day"), NOW, "seconds"),
    Time.DAY: Periods(NOW.subtract(days=1), NOW, "seconds"),
    Time.WEEK: Periods(NOW.subtract(weeks=1), NOW, "hours"),
    Time.MONTH: Periods(NOW.subtract(months=1), NOW, "hours"),
    Time.YEAR: Periods(NOW.subtract(years=1), NOW, "hours"),
}


def main(aws_profiles: list, period: str):

    profile_generator = run_all_profiles(aws_profiles=aws_profiles, period=period)

    table = Table()
    table.add_column("StateMachine", justify="left", overflow="fold")
    table.add_column("Profile")
    table.add_column("Account")
    table.add_column("Region")
    table.add_column("Total")
    table.add_column("Succeed (%)")
    table.add_column("Running")
    table.add_column("Failed")
    table.add_column("Aborted")
    table.add_column("TimedOut")
    table.add_column("Throttled")

    for profile in profile_generator:
        for row in profile:
            if row:
                table.add_row(*row)

    return table


def run_all_profiles(aws_profiles: list, period: str):
    def _run_for_profile(aws_profile: str):
        return run_for_profile(profile_name=aws_profile, period=period)

    with concurrent.futures.ThreadPoolExecutor(len(aws_profiles)) as thread:
        profile_generator = thread.map(_run_for_profile, aws_profiles)

    return profile_generator


def run_for_state_machine(
    state_machine: object, cloudwatch_resource: object, profile_name: str, period: str
):
    state_machine_arn = state_machine.get("stateMachineArn")
    states = get_data_from_cloudwatch(
        cloudwatch_resource=cloudwatch_resource,
        state_machine_arn=state_machine_arn,
        period=period,
    )
    arn_parsed = parse_aws_arn(state_machine_arn)
    account = arn_parsed.get("account")
    region = arn_parsed.get("region")
    state_machine_name = state_machine.get("name")
    state_machine_url = get_statemachine_url(
        state_machine_arn=state_machine_arn, region=region
    )
    state_machine_name_url = f"[link={state_machine_url}]{state_machine_name}[/link]"
    return_object = (
        state_machine_name_url,
        profile_name,
        account,
        region,
        f"{states.total_executions:,.0f}",
        f"{states.succeeded_perc:,.2f}",
        f"{states.running:,.0f}",
        f"{states.failed:,.0f}",
        f"{states.aborted:,.0f}",
        f"{states.timed_out:,.0f}",
        f"{states.throttled:,.0f}",
    )
    return return_object


def run_for_profile(profile_name: str, period: str) -> Table:
    sfn_client = boto3.Session(profile_name=profile_name).client("stepfunctions")
    cloudwatch_resource = boto3.Session(profile_name=profile_name).resource(
        "cloudwatch"
    )
    state_machines = sfn_client.list_state_machines().get("stateMachines")
    if state_machines:

        def _run_for_state_machine(state_machine):
            return run_for_state_machine(
                state_machine=state_machine,
                cloudwatch_resource=cloudwatch_resource,
                profile_name=profile_name,
                period=period,
            )

        with concurrent.futures.ThreadPoolExecutor(
            min(len(state_machines), 100)
        ) as thread:
            state_machine_generator = thread.map(_run_for_state_machine, state_machines)
        return state_machine_generator
    else:
        logger.info(f"no statemachines found for profile {profile_name}")
        return ()


def call_metric_endpoint(
    metric_name, cloudwatch_resource, state_machine_arn, period_object
):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#metric
    """
    metric = cloudwatch_resource.Metric("AWS/States", metric_name).get_statistics(
        Dimensions=[
            {
                "Name": "StateMachineArn",
                "Value": state_machine_arn,
            },
        ],
        StartTime=period_object.start_date_of_period,
        EndTime=period_object.now,
        Period=period_object.get_difference_in_seconds(),
        Statistics=[
            "Sum",
        ],
    )
    sum_of_datapoints = sum([datapoint["Sum"] for datapoint in metric["Datapoints"]])
    return sum_of_datapoints


def get_data_from_cloudwatch(
    cloudwatch_resource: object, state_machine_arn: str, period: str
) -> States:
    """
    check the docs for more info
    https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html

    Parameters
    ----------
    cloudwatch_resource
    state_machine_arn
    period

    Returns
    -------

    """

    period_object = get_period_objects(period=period)

    def _call_metric_endpoint(metric_name):
        return call_metric_endpoint(
            metric_name=metric_name,
            cloudwatch_resource=cloudwatch_resource,
            state_machine_arn=state_machine_arn,
            period_object=period_object,
        )

    metrics = [
        "ExecutionsStarted",
        "ExecutionsSucceeded",
        "ExecutionsFailed",
        "ExecutionsAborted",
        "ExecutionsTimedOut",
        "ExecutionThrottled",
    ]

    with concurrent.futures.ThreadPoolExecutor(len(metrics)) as thread:
        started, succeeded, failed, aborted, timed_out, throttled = list(
            thread.map(_call_metric_endpoint, metrics)
        )

    running = started - succeeded - failed - aborted - timed_out - throttled

    succeeded_perc = (succeeded / started) * 100 if started > 0 else 0

    return States(
        total_executions=started,
        succeeded=succeeded,
        succeeded_perc=succeeded_perc,
        failed=failed,
        running=running,
        aborted=aborted,
        timed_out=timed_out,
        throttled=throttled,
    )


def get_period_objects(period: str):
    try:
        period_object = PERIODS_MAPPING[period]
    except KeyError as e:
        raise NameError(
            f"We did not recognize the value {period}. Please choose from {PERIODS_MAPPING.keys()}"
        )
    return period_object


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

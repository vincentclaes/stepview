from collections import defaultdict
from typing import List, Optional

import boto3
import botocore
import pendulum

from rich.progress import Progress, TextColumn, BarColumn
from rich.table import Table
from dataclasses import dataclass
from stepview import logger
from concurrent.futures import ThreadPoolExecutor


@dataclass
class State:
    """State that we pass to the TUI table."""

    total_executions: int
    succeeded: str
    succeeded_perc: str
    failed: str
    running: str
    aborted: str
    timed_out: str
    throttled: str


@dataclass
class Row:
    state_machine_name: str
    state_machine_name_with_url: str
    profile_name: str
    account: str
    region: str
    state: State

    def get_values(self):
        return (
            self.state_machine_name_with_url,
            self.profile_name,
            self.account,
            self.region,
            f"{self.state.total_executions:,.0f}",
            f"{self.state.succeeded_perc:,.2f}",
            f"{self.state.running}",
            f"{self.state.failed:,.0f}/"
            f"{self.state.failed:,.0f}/"
            f"{self.state.aborted:,.0f}/"
            f"{self.state.timed_out:,.0f}/"
            f"{self.state.throttled:,.0f}",
        )


@dataclass
class Periods:
    """We use Periods class to get the datetime range
    for collecting the metrics."""

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

    @classmethod
    def get_time_variables(cls):
        return [
            v
            for k, v in cls.__dict__.items()
            if not k.startswith("__")
            and not k.endswith("__")
            and not "method" in str(v)
            and not "function" in str(v)
        ]


NOW = pendulum.now()
MAX_POOL_CONNECTIONS = 10
MAX_RUNNING_RESULTS = 10
MAX_RETRIES = 10


def main(aws_profiles: List[str], period: str, tags: Optional[List[tuple]]):
    period = get_period_objects(period=period)
    progress_viz = (TextColumn("[progress.description]{task.description}"), BarColumn())

    with Progress(*progress_viz) as progress:
        progress.add_task("[green]Getting Data...", start=False)
        try:
            profile_generator = run_all_profiles(
                aws_profiles=aws_profiles, period=period, tags=tags
            )
        except botocore.client.ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                logger.info(
                    "Throttling exception. Please reduce the number of AWS profiles "
                    "or filter on tags."
                )
            else:
                raise e

    table = Table()
    table.add_column("StateMachine", justify="left", overflow="fold")
    table.add_column("Profile", overflow="fold")
    table.add_column("Account", overflow="fold")
    table.add_column("Region", overflow="fold")
    table.add_column("Total", overflow="fold")
    table.add_column("Succeed (%)", overflow="fold")
    table.add_column("Running", overflow="fold")
    table.add_column("Failed/Aborted/TimedOut/Throttled", overflow="fold")

    all_rows = []
    for profile in profile_generator:
        for row in profile:
            if row:
                table.add_row(*row.get_values())
            all_rows.append(row)

    # return table for viz, return all_rows for tests
    return table, all_rows


def run_all_profiles(
    aws_profiles: List[str], period: Periods, tags=Optional[List[tuple]]
):
    def _run_for_profile(aws_profile: str):
        return run_for_profile(profile_name=aws_profile, period=period, tags=tags)

    with ThreadPoolExecutor(len(aws_profiles)) as thread:
        profile_generator = thread.map(_run_for_profile, aws_profiles)

    return profile_generator


def run_for_state_machine(
    state_machine_arn: str,
    cloudwatch_resource: object,
    sfn_client: object,
    profile_name: str,
    period: Periods,
):
    state = get_sfn_data(
        cloudwatch_resource=cloudwatch_resource,
        sfn_client=sfn_client,
        state_machine_arn=state_machine_arn,
        period=period,
    )
    arn_parsed = parse_aws_arn(state_machine_arn)
    account = arn_parsed.get("account")
    region = arn_parsed.get("region")
    state_machine_name = arn_parsed.get("resource")
    state_machine_url = get_statemachine_url(
        state_machine_arn=state_machine_arn, region=region
    )
    state_machine_name_with_url = (
        f"[link={state_machine_url}]{state_machine_name}[/link]"
    )
    row = Row(
        state_machine_name=state_machine_name,
        state_machine_name_with_url=state_machine_name_with_url,
        profile_name=profile_name,
        account=account,
        region=region,
        state=state,
    )
    return row


def get_statemachines(sfn_client: object, tags_client:object, tags: Optional[List[tuple]]) -> list:

    # parse tags
    tags_ = defaultdict(list)
    for key, value in tags:
        tags_[key].append(value)
    tag_filters = [{"Key": key, "Values": value}for key, value in tags_.items()]

    tagging_paginator = tags_client.get_paginator('get_resources')
    for page_for_tags in tagging_paginator.paginate(
            ResourceTypeFilters=['states:stateMachine'],
            TagFilters=tag_filters
    ):
        statemachines = page_for_tags.get("ResourceTagMappingList")
        if statemachines:
            yield_ = [statemachine.get("ResourceARN") for statemachine in statemachines]
            yield yield_
        else:
            logger.info(f"No statemachines were found for tags: {tags}")


def run_for_profile(
    profile_name: str, period: Periods, tags: Optional[List[tuple]]
) -> list[Row]:
    config = botocore.client.Config(
        retries={"max_attempts": MAX_RETRIES, "mode": "standard"},
        max_pool_connections=MAX_POOL_CONNECTIONS,
    )
    sfn_client = boto3.Session(profile_name=profile_name).client(
        "stepfunctions", config=config
    )
    cloudwatch_resource = boto3.Session(profile_name=profile_name).resource(
        "cloudwatch", config=config
    )
    tags_client = boto3.Session(profile_name=profile_name).client(
        "resourcegroupstaggingapi", config=config
    )
    all_statemachine_results = []
    for state_machines in get_statemachines(sfn_client=sfn_client, tags_client=tags_client, tags=tags):
        if state_machines:
            def _run_for_state_machine(state_machine_arn):
                return run_for_state_machine(
                    state_machine_arn=state_machine_arn,
                    cloudwatch_resource=cloudwatch_resource,
                    sfn_client=sfn_client,
                    profile_name=profile_name,
                    period=period,
                )

            with ThreadPoolExecutor(
                min(len(state_machines), MAX_POOL_CONNECTIONS)
            ) as thread:
                state_machine_results = thread.map(
                    _run_for_state_machine, state_machines
                )
            all_statemachine_results.extend(state_machine_results)
        else:
            logger.info(f"no statemachines found for profile {profile_name}")
    all_statemachine_results_ = {row.state_machine_name: row for row in all_statemachine_results}
    all_statemachine_results_sorted = list(dict(sorted(all_statemachine_results_.items())).values())
    return all_statemachine_results_sorted


def call_metric_endpoint(
    metric_name: str,
    cloudwatch_resource: object,
    state_machine_arn: str,
    period_object: Periods,
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


def get_sfn_data(
    cloudwatch_resource: object,
    sfn_client: object,
    state_machine_arn: str,
    period: Periods,
) -> State:
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

    def _call_metric_endpoint(metric_name):
        return call_metric_endpoint(
            metric_name=metric_name,
            cloudwatch_resource=cloudwatch_resource,
            state_machine_arn=state_machine_arn,
            period_object=period,
        )

    metrics = [
        "ExecutionsStarted",
        "ExecutionsSucceeded",
        "ExecutionsFailed",
        "ExecutionsAborted",
        "ExecutionsTimedOut",
        "ExecutionThrottled",
    ]

    with ThreadPoolExecutor(len(metrics)) as thread:
        started, succeeded, failed, aborted, timed_out, throttled = list(
            thread.map(_call_metric_endpoint, metrics)
        )

    running_ = started - succeeded - failed - aborted - timed_out - throttled
    logger.debug(f"running calculated: {running_}")
    running = running_ if running_ >= 0 else 0

    succeeded_perc = (succeeded / started) * 100 if started > 0 else 0

    return State(
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
    periods_mapping = {
        Time.MINUTE: Periods(NOW.subtract(minutes=1), NOW, "microseconds"),
        Time.HOUR: Periods(NOW.subtract(hours=1), NOW, "seconds"),
        Time.TODAY: Periods(NOW.start_of("day"), NOW, "seconds"),
        Time.DAY: Periods(NOW.subtract(days=1), NOW, "seconds"),
        Time.WEEK: Periods(NOW.subtract(weeks=1), NOW, "hours"),
        Time.MONTH: Periods(NOW.subtract(months=1), NOW, "hours"),
        Time.YEAR: Periods(NOW.subtract(years=1), NOW, "hours"),
    }

    try:
        period_object = periods_mapping[period]
    except KeyError as e:
        raise NameError(
            f"We did not recognize the value {period}. Please choose from {periods_mapping.keys()}"
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


def get_running_executions_for_state_machine(
    sfn_client: object, state_machine_arn: str
):
    executions = sfn_client.list_executions(
        stateMachineArn=state_machine_arn,
        statusFilter="RUNNING",
        maxResults=MAX_RUNNING_RESULTS,
    )
    no_running = str(len(executions.get("executions")))
    if no_running >= MAX_RUNNING_RESULTS:
        no_running = f">={no_running}"
    return no_running

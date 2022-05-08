import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import boto3
import pendulum
from dateutil.tz import tzutc
from moto import mock_stepfunctions
from moto import mock_cloudwatch
from textual.app import App
from typer.testing import CliRunner
from freezegun import freeze_time

import stepview.data
from stepview.data import MetricNames, NOW, Time
from stepview import entrypoint

current_dir = Path(__file__).resolve().parent
#
#
# def list_executions(status: list, start_date=datetime.datetime.now(tz=tzutc())):
#     """example.
#
#         [{
#              'executionArn': 'arn:aws:states:eu-west-1:123456789012:execution:sm1:586ae65c-05fa-47ff-a45e-f3efea9f551a',
#              'stateMachineArn': 'arn:aws:states:eu-west-1:123456789012:stateMachine:sm1',
#              'name': '586ae65c-05fa-47ff-a45e-f3efea9f551a', 'status': 'RUNNING',
#              'startDate': datetime.datetime(2022, 4, 6, 10, 49, 51, 278000, tzinfo=tzutc())}, {
#              'executionArn': 'arn:aws:states:eu-west-1:123456789012:execution:sm1:5a89bab9-7276-4df0-ab70-ac3e4375f6d6',
#              'stateMachineArn': 'arn:aws:states:eu-west-1:123456789012:stateMachine:sm1',
#              'name': '5a89bab9-7276-4df0-ab70-ac3e4375f6d6', 'status': 'RUNNING',
#              'startDate': datetime.datetime(2022, 4, 6, 10, 49, 41, 459000, tzinfo=tzutc())
#         }]
#
#
#
#     :param status: 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED'
#     :return:
#     """
#     return {
#         "executions": [
#             {
#                 "executionArn": "arn:aws:states:eu-west-1:123456789012:execution:sm1:586ae65c-05fa-47ff-a45e-f3efea9f551a",
#                 "stateMachineArn": "arn:aws:states:eu-west-1:123456789012:stateMachine:sm1",
#                 "name": "586ae65c-05fa-47ff-a45e-f3efea9f551a",
#                 "status": s,
#                 "startDate": start_date,
#             }
#             for s in status
#         ]
#     }
#


def create_statemachine(name, profile):

    # point boto3 to our local credentials file
    # using an env var.
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(
        Path(current_dir, "resources", "mock_credentials")
    )

    # read our dummy statemachine definition
    with open(Path(current_dir, "resources", "sfn_definition.json")) as f:
        sfn_definition = f.read()

    # some dummy role
    role = "arn:aws:iam::012345678901:role/service-role/AmazonSageMaker-ExecutionRole-20191008T190827"

    client = boto3.Session(profile_name=profile).client("stepfunctions")
    state_machine = client.create_state_machine(
        name=name, definition=sfn_definition, roleArn=role
    )

    return client, role, state_machine


def create_metric(metric_name, profile, state_machine, timestamp=NOW.subtract(minutes=1)):
    """
    Add a metric to cloudwatch
    check how to add MetricData from the documentation
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data

    """

    # point boto3 to our local credentials file
    # using an env var.
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(
        Path(current_dir, "resources", "mock_credentials")
    )

    client = boto3.Session(profile_name=profile).client("cloudwatch")
    client.put_metric_data(
        Namespace="AWS/State",
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [
                    {
                        "Name": "StateMachineArn",
                        "Value": state_machine.get("stateMachineArn"),
                    }
                ],
                "StatisticValues": {
                    "SampleCount": 1,
                    "Sum": 1,
                    "Minimum": 1,
                    "Maximum": 1,
                },
                # we substract 1 minute so that we are sure we are
                # before the init of NOW in data module.
                "Timestamp": timestamp,
                "Values": [1],
                "Value": 1,
            }
        ],
    )


class TestStepView(unittest.TestCase):
    @mock_cloudwatch
    @mock_stepfunctions
    def test_get_stepfunctions_status_happy_flow(self):

        client, role, state_machine = create_statemachine("sm1", "profile1")
        create_metric(
            MetricNames.EXECUTIONS_SUCCEEDED,
            profile="profile1",
            state_machine=state_machine,
        )

        self.exception_ = None
        try:
            stepview.data.main(aws_profiles=["profile1"], period="day")
        except Exception as e:
            self.exception_ = e

        self.assertIsNone(self.exception_)

    @freeze_time("2022-05-08 12:05:05")
    @mock_cloudwatch
    @mock_stepfunctions
    def test_stepview_on_time_period_minute(self):
        stepview.data.NOW = pendulum.now()
        sfn_client, role, state_machine = create_statemachine("sm1", "profile1")

        time_started = datetime.datetime.fromisoformat(
            pendulum.now().subtract(seconds=40).to_iso8601_string()
        )
        time_succeeded = datetime.datetime.fromisoformat(
            pendulum.now().subtract(seconds=5).to_iso8601_string()
        )
        time_too_early = datetime.datetime.fromisoformat(
            pendulum.now().subtract(minutes=1, seconds=1).to_iso8601_string()
        )

        create_metric(
            MetricNames.EXECUTIONS_STARTED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_started
        )
        create_metric(
            MetricNames.EXECUTIONS_SUCCEEDED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_succeeded
        )
        create_metric(
            MetricNames.EXECUTIONS_STARTED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_too_early
        )

        _, result = stepview.data.main(aws_profiles=["profile1"], period="minute")

        # self.assertEqual(result[0].state.running, 1)
        self.assertEqual(result[0].state.succeeded, 1)
        self.assertEqual(result[0].state.succeeded_perc, 100.0)
        self.assertEqual(result[0].state.failed, 0)
        self.assertEqual(result[0].state.throttled, 0)
        self.assertEqual(result[0].state.timed_out, 0)
        self.assertEqual(result[0].state.total_executions, 1)

    @mock_cloudwatch
    @mock_stepfunctions
    def test_stepview_on_time_period_hour(self):

        sfn_client, role, state_machine = create_statemachine("sm1", "profile1")

        time_started = datetime.datetime.fromisoformat(
            pendulum.now().subtract(minutes=59).to_iso8601_string()
        )
        time_succeeded = datetime.datetime.fromisoformat(
            pendulum.now().subtract(seconds=5).to_iso8601_string()
        )
        time_too_early = datetime.datetime.fromisoformat(
            pendulum.now().subtract(hours=1, minutes=1, seconds=1).to_iso8601_string()
        )

        create_metric(
            MetricNames.EXECUTIONS_STARTED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_started
        )
        create_metric(
            MetricNames.EXECUTIONS_SUCCEEDED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_succeeded
        )
        create_metric(
            MetricNames.EXECUTIONS_STARTED,
            profile="profile1",
            state_machine=state_machine,
            timestamp=time_too_early
        )

        _, result = stepview.data.main(aws_profiles=["profile1"], period=Time.HOUR)

        # self.assertEqual(result[0].state.running, 0)
        self.assertEqual(result[0].state.succeeded, 1)
        self.assertEqual(result[0].state.succeeded_perc, 100.0)
        self.assertEqual(result[0].state.failed, 0)
        self.assertEqual(result[0].state.throttled, 0)
        self.assertEqual(result[0].state.timed_out, 0)
        self.assertEqual(result[0].state.total_executions, 1)

    @unittest.skip("first get performance straight before we continue tests.")
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_today(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_MAPPING.
        # for today the granularity is set to hour
        # but in this case we set to seconds because only at midnight
        # days=1 will be seen as part of today.
        not_today = datetime.datetime.fromisoformat(
            pendulum.now().subtract(days=1, seconds=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=not_today),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.TODAY,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @unittest.skip("first get performance straight before we continue tests.")
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_day(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_MAPPING.
        # for day the granularity is set to hour.
        yesterday = datetime.datetime.fromisoformat(
            pendulum.now().subtract(days=1, hours=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=yesterday),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.DAY,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @unittest.skip("first get performance straight before we continue tests.")
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_week(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_MAPPING.
        # for week the granularity is set to hour,
        # therefore we need to go back 1 month and 1 day.
        last_week = datetime.datetime.fromisoformat(
            pendulum.now().subtract(weeks=1, days=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=last_week),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.WEEK,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @unittest.skip("first get performance straight before we continue tests.")
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_month(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_MAPPING.
        # for month the granularity is set to hour,
        # therefore we need to go back 1 month and 1 day.
        last_month = datetime.datetime.fromisoformat(
            pendulum.now().subtract(months=1, days=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=last_month),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.MONTH,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @unittest.skip("first get performance straight before we continue tests.")
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_year(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_MAPPING.
        # for year the granularity is set to hour,
        # therefore we need to go back 1 year and 1 day.
        last_year = datetime.datetime.fromisoformat(
            pendulum.now().subtract(years=1, days=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=last_year),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.YEAR,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)


class TestStepViewCli(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = CliRunner()

    @patch.object(App, "run")
    def test_cli(self, m_textual_run):
        # for some reason i cannot call the run function when instantiating
        # StepViewTui (subclass of textual.app.App) in this test.
        result = self.runner.invoke(
            stepview.entrypoint.app, ["--profile", "profile1 profile2 profile3"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_verbose(self):
        pass

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

import stepview.data
from stepview import entrypoint

current_dir = Path(__file__).resolve().parent


def list_executions(status: list, start_date=datetime.datetime.now(tz=tzutc())):
    """example.

        [{
             'executionArn': 'arn:aws:states:eu-west-1:123456789012:execution:sm1:586ae65c-05fa-47ff-a45e-f3efea9f551a',
             'stateMachineArn': 'arn:aws:states:eu-west-1:123456789012:stateMachine:sm1',
             'name': '586ae65c-05fa-47ff-a45e-f3efea9f551a', 'status': 'RUNNING',
             'startDate': datetime.datetime(2022, 4, 6, 10, 49, 51, 278000, tzinfo=tzutc())}, {
             'executionArn': 'arn:aws:states:eu-west-1:123456789012:execution:sm1:5a89bab9-7276-4df0-ab70-ac3e4375f6d6',
             'stateMachineArn': 'arn:aws:states:eu-west-1:123456789012:stateMachine:sm1',
             'name': '5a89bab9-7276-4df0-ab70-ac3e4375f6d6', 'status': 'RUNNING',
             'startDate': datetime.datetime(2022, 4, 6, 10, 49, 41, 459000, tzinfo=tzutc())
        }]



    :param status: 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED'
    :return:
    """
    return {
        "executions": [
            {
                "executionArn": "arn:aws:states:eu-west-1:123456789012:execution:sm1:586ae65c-05fa-47ff-a45e-f3efea9f551a",
                "stateMachineArn": "arn:aws:states:eu-west-1:123456789012:stateMachine:sm1",
                "name": "586ae65c-05fa-47ff-a45e-f3efea9f551a",
                "status": s,
                "startDate": start_date,
            }
            for s in status
        ]
    }


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

def create_metric(name, profile, state_machine):

    # point boto3 to our local credentials file
    # using an env var.
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(
        Path(current_dir, "resources", "mock_credentials")
    )

    # # read our dummy statemachine definition
    # with open(Path(current_dir, "resources", "sfn_definition.json")) as f:
    #     sfn_definition = f.read()

    # some dummy role
    # role = "arn:aws:iam::012345678901:role/service-role/AmazonSageMaker-ExecutionRole-20191008T19082"

    # check how to add MetricData from the documentation
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data
    # cloudwatch_resource = boto3.Session(profile_name=profile).resource("cloudwatch")
    # cloudwatch_resource.Metric('AWS/States', 'ExecutionsSucceeded').put_data(
    client = boto3.Session(profile_name=profile).client("cloudwatch")
    client.put_metric_data(
            Namespace='AWS/States',
            MetricData=[
                {
                    'MetricName': 'ExecutionsSucceeded',
                    'Dimensions': [{
                        'Name': 'StateMachineArn',
                        # 'Value': 'arn:aws:states:eu-central-1:077590795309:stateMachine:data-pipeline-simple-workflow'
                        'Value': state_machine.get("stateMachineArn")
                    }], 'StatisticValues': {
                        'SampleCount': 1,
                        'Sum': 1,
                        'Minimum': 1,
                        'Maximum': 1
                    },
                    'Timestamp': pendulum.now().subtract(hours=6),
                    'Values': [1],
                    'Value': 1
                }
            ]
    )

    # client.list_metrics(
    #     Namespace='AWS/States',
    #     MetricName='ExecutionsSucceeded',
    #     Dimensions=[
    #         {
    #             'Name': 'StateMachineArn',
    #             'Value': state_machine.get("stateMachineArn")
    #         },
    #     ],
    #
    # )
    #
    # state_machine = client.create_state_machine(
    #     name=name, definition=sfn_definition, roleArn=role
    # )

    # return client, role, state_machine

class TestStepView(unittest.TestCase):
    @mock_cloudwatch
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_get_stepfunctions_status_happy_flow(self, m_list_executions):

        client, role, state_machine = create_statemachine("sm1", "profile1")
        create_metric("", profile="profile1", state_machine=state_machine)

        # create_statemachine("sm2", "profile1")
        # create_statemachine("sm3", "profile1")
        # create_statemachine("sm1", "profile2")

        m_list_executions.side_effect = [
            list_executions(["RUNNING", "FAILED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["SUCCEEDED", "SUCCEEDED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["FAILED", "FAILED", "FAILED", "FAILED"]),
            list_executions(["RUNNING"]),
        ]
        self.exception_ = None
        try:
            # stepview.data.main(aws_profiles=["profile1", "profile2"], period="day")
            stepview.data.main(aws_profiles=["profile1",], period="day")
        except Exception as e:
            self.exception_ = e

        self.assertIsNone(self.exception_)

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_get_stepfunctions_with_next_token(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        m_list_executions.side_effect = [
            {
                # we add a token so that we call the function
                # list_executions_for_state_machine two times.
                "nextToken": "some-token",
                **list_executions(["RUNNING", "FAILED", "SUCCEEDED", "SUCCEEDED"]),
            },
            list_executions(["RUNNING", "FAILED", "SUCCEEDED", "SUCCEEDED"]),
        ]
        self.exception_ = None
        try:
            states = stepview.data.get_all_states_of_executions(
                sfn_client=sfn_client,
                state_machine_arn=statemachine.get("stateMachineArn"),
                period="day",
            )

        except Exception as e:
            self.exception_ = e

        self.assertIsNone(self.exception_)
        self.assertEqual(m_list_executions.call_count, 2)
        self.assertEqual(states.failed, 2)
        self.assertEqual(states.running, 2)
        self.assertEqual(states.succeeded, 4)
        self.assertEqual(states.succeeded_perc, 50.0)
        self.assertEqual(states.total_executions, 8)

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_minute(self, m_list_executions):
        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        last_minute = datetime.datetime.fromisoformat(
            pendulum.now().subtract(minutes=1, seconds=2).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=last_minute),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.MINUTE,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_hour(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
        # for hour the granularity is set to minute.
        last_hour = datetime.datetime.fromisoformat(
            pendulum.now().subtract(hours=1, minutes=1).to_iso8601_string()
        )
        m_list_executions.side_effect = [
            {
                "nextToken": "some-token",
                **list_executions(["SUCCEEDED"]),
            },
            list_executions(["FAILED"], start_date=last_hour),
        ]

        states = stepview.data.get_all_states_of_executions(
            sfn_client=sfn_client,
            state_machine_arn=statemachine.get("stateMachineArn"),
            period=stepview.data.HOUR,
        )

        self.assertEqual(states.succeeded, 1)
        self.assertEqual(states.succeeded_perc, 100.0)
        self.assertEqual(states.failed, 0)

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_today(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
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

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_day(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
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

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_week(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
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

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_month(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
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

    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_stepview_on_time_period_year(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

        # we substract the granularity of the PERIODS_LIST.
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

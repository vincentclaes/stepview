import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pendulum
from dateutil.tz import tzutc
from moto import mock_stepfunctions
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
        Path(current_dir, "resources", "credentials")
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


class TestStepView(unittest.TestCase):
    @mock_stepfunctions
    @patch("stepview.data.list_executions_for_state_machine")
    def test_get_stepfunctions_status_happy_flow(self, m_list_executions):

        create_statemachine("sm1", "profile1")
        create_statemachine("sm2", "profile1")
        create_statemachine("sm3", "profile1")
        create_statemachine("sm1", "profile2")

        m_list_executions.side_effect = [
            list_executions(["RUNNING", "FAILED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["SUCCEEDED", "SUCCEEDED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["FAILED", "FAILED", "FAILED", "FAILED"]),
            list_executions(["RUNNING"]),
        ]
        self.exception_ = None
        try:
            stepview.data.main(aws_profiles=["profile1", "profile2"], period="day")
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
    def test_stepview_on_time_period_day(self, m_list_executions):

        sfn_client, role, statemachine = create_statemachine("sm1", "profile1")

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
            period="day",
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
            stepview.entrypoint.app, ["--profiles", "profile1 profile2 profile3"]
        )
        self.assertEqual(result.exit_code, 0)

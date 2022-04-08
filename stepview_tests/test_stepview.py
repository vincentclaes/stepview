import datetime
import os
import unittest
from unittest.mock import patch

import boto3
from dateutil.tz import tzutc
from moto import mock_stepfunctions

from stepview import entrypoint


def list_executions(status: list):
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
                "startDate": datetime.datetime.now(tz=tzutc()),
            }
            for s in status
        ]
    }


class TestStepView(unittest.TestCase):
    @patch("stepview.entrypoint._list_executions_for_state_machine")
    @mock_stepfunctions
    def test_get_stepfunctions_status_happy_flow(self, m_list_executions):
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "./resources/credentials"
        with open("./resources/sfn_definition.json") as f:
            sfn_definition = f.read()

        roleArn = "arn:aws:iam::012345678901:role/service-role/AmazonSageMaker-ExecutionRole-20191008T190827"

        client = boto3.Session(profile_name="profile1").client("stepfunctions")
        client.create_state_machine(
            name="sm1", definition=sfn_definition, roleArn=roleArn
        )
        client.create_state_machine(
            name="sm2", definition=sfn_definition, roleArn=roleArn
        )
        client.create_state_machine(
            name="sm3", definition=sfn_definition, roleArn=roleArn
        )

        client = boto3.Session(profile_name="profile2").client("stepfunctions")
        client.create_state_machine(
            name="sm1", definition=sfn_definition, roleArn=roleArn
        )

        m_list_executions.side_effect = [
            list_executions(["RUNNING", "FAILED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["SUCCEEDED", "SUCCEEDED", "SUCCEEDED", "SUCCEEDED"]),
            list_executions(["FAILED", "FAILED", "FAILED", "FAILED"]),
            list_executions(["RUNNING"]),
        ]
        self.exception_ = None
        try:
            entrypoint.main(aws_profiles=["profile1", "profile2"])
        except Exception as e:
            self.assertIsNone(self.exception_)

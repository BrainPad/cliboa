#
# Copyright BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
import json
from unittest import TestCase
from unittest.mock import MagicMock

from cliboa.core.executor import _StepExecutor
from cliboa.core.model import StepModel
from cliboa.listener.step import StepStatusListener
from cliboa.scenario.sample_step import SampleCustomStep


class TestStepStatusListener(TestCase):
    def test_logging_properties(self):
        """
        Test basic property logging functionality.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        model = StepModel.model_validate(
            {"step": "test_step", "class": "SampleCustomStep", "arguments": {"retry_count": 5}}
        )
        executor = _StepExecutor(instance, model)
        listener = StepStatusListener()
        listener._logger = mock_logger
        executor.register_listener(listener)
        executor.execute()

        mock_logger.info.assert_called()

        all_calls = mock_logger.info.call_args_list
        logged_messages = [call[0][0] for call in all_calls]

        # Find Step properties JSON and verify property values
        step_props_found = False
        for msg in logged_messages:
            if msg.startswith("Step properties: "):
                json_str = msg.replace("Step properties: ", "", 1)
                try:
                    props = json.loads(json_str)
                    self.assertEqual(props.get("_retry_count"), 5)
                    step_props_found = True
                    break
                except json.JSONDecodeError:
                    pass
        self.assertTrue(step_props_found, "Step properties JSON should be found and valid")

    def test_logging_mask_password(self):
        """
        In log file, 'password' is masked.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        model = StepModel.model_validate(
            {"step": "test_step", "class": "SampleCustomStep", "arguments": {"password": "test"}}
        )
        executor = _StepExecutor(instance, model)
        listener = StepStatusListener()
        listener._logger = mock_logger
        executor.register_listener(listener)
        executor.execute()

        mock_logger.info.assert_called()

        all_calls = mock_logger.info.call_args_list
        logged_messages = [call[0][0] for call in all_calls]

        # Find Step properties JSON and verify password masking
        step_props_found = False
        for msg in logged_messages:
            if msg.startswith("Step properties: "):
                json_str = msg.replace("Step properties: ", "", 1)
                try:
                    props = json.loads(json_str)
                    self.assertEqual(
                        props.get("_password"), "****", "Password should be masked as ****"
                    )
                    # assert None parameter
                    self.assertEqual(
                        props.get("_access_key"),
                        None,
                        "Access key should NOT be masked when it is None.",
                    )
                    step_props_found = True
                    break
                except json.JSONDecodeError:
                    pass
        self.assertTrue(step_props_found, "Step properties JSON should be found and valid")

    def test_logging_mask_aws_keys(self):
        """
        In log file, 'access_key' and 'secret_key' of AWS are masked.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        model = StepModel.model_validate(
            {
                "step": "test_step",
                "class": "SampleCustomStep",
                "arguments": {
                    "access_key": "test_access",
                    "secret_key": "test_secret",
                    "access_token": "token",
                },
            }
        )
        executor = _StepExecutor(instance, model)
        listener = StepStatusListener()
        listener._logger = mock_logger
        executor.register_listener(listener)
        executor.execute()

        mock_logger.info.assert_called()

        all_calls = mock_logger.info.call_args_list
        logged_messages = [call[0][0] for call in all_calls]

        # Find Step properties JSON and verify AWS key masking
        step_props_found = False
        for msg in logged_messages:
            if msg.startswith("Step properties: "):
                json_str = msg.replace("Step properties: ", "", 1)
                try:
                    props = json.loads(json_str)
                    self.assertEqual(
                        props.get("_access_key"),
                        "tes****ess",
                        "Access key should be partial masked as ???****???",
                    )
                    self.assertEqual(
                        props.get("_secret_key"), "****", "Secret key should be full masked as ****"
                    )
                    self.assertEqual(
                        props.get("_access_token"),
                        "****ken",
                        "Short access token should be partial masked as ****???",
                    )
                    step_props_found = True
                    break
                except json.JSONDecodeError:
                    pass
        self.assertTrue(step_props_found, "Step properties JSON should be found and valid")

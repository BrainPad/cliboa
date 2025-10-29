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
import os
import shutil
from unittest import TestCase
from unittest.mock import MagicMock

from cliboa.conf import env
from cliboa.core.executor import _StepExecutor
from cliboa.core.listener import StepStatusListener
from cliboa.core.model import StepModel
from cliboa.scenario.sample_step import SampleCustomStep
from cliboa.util.helper import Helper
from cliboa.util.log import _get_logger


class TestBase(TestCase):
    def setup_method(self, method):
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")
        self._data_dir = os.path.join(env.BASE_DIR, "data")

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
                    self.assertEqual(props.get("_step"), "test_step")
                    self.assertEqual(props.get("_retry_count"), 5)
                    self.assertEqual(props.get("_symbol"), None)
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
                "arguments": {"access_key": "test", "secret_key": "test"},
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
                        props.get("_access_key"), "****", "Access key should be masked as ****"
                    )
                    self.assertEqual(
                        props.get("_secret_key"), "****", "Secret key should be masked as ****"
                    )
                    step_props_found = True
                    break
                except json.JSONDecodeError:
                    pass
        self.assertTrue(step_props_found, "Step properties JSON should be found and valid")

    def test_source_path_reader_with_none(self):
        instance = SampleCustomStep()
        Helper.set_property(instance, "logger", _get_logger(instance.__class__.__name__))
        ret = instance._source_path_reader(None)

        assert ret is None

    def test_source_path_reader_with_path(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            dummy_pass = os.path.join(self._data_dir, "id_rsa")
            with open(dummy_pass, "w") as f:
                f.write("test")

            instance = SampleCustomStep()

            ret = instance._source_path_reader({"file": dummy_pass})
            assert ret == dummy_pass
            with open(ret, "r") as fp:
                actual = fp.read()
                assert "test" == actual
        finally:
            shutil.rmtree(self._data_dir)

    def test_source_path_reader_with_content(self):
        instance = SampleCustomStep()
        ret = instance._source_path_reader({"content": "test"})
        with open(ret, "r") as fp:
            actual = fp.read()
            assert "test" == actual

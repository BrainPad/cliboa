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
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

from cliboa.conf import env
from cliboa.scenario.sample_step import SampleCustomStep
from cliboa.util.helper import Helper
from cliboa.util.log import _get_logger


class TestBase(TestCase):
    def setup_method(self, method):
        cmd_args = {"project_name": "spam", "format": "yaml"}
        self._cmd_args = SimpleNamespace(**cmd_args)
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_logging_properties(self):
        """
        Test basic property logging functionality.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        original_logger = instance._logger
        instance._logger = mock_logger

        try:
            # Set normal properties (not sensitive)
            Helper.set_property(instance, "step", "test_step")
            Helper.set_property(instance, "retry_count", 5)
            instance.trigger()

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
                        self.assertEqual(props.get("_parallel"), None)
                        step_props_found = True
                        break
                    except json.JSONDecodeError:
                        pass
            self.assertTrue(step_props_found, "Step properties JSON should be found and valid")
        finally:
            instance._logger = original_logger

    def test_logging_mask_password(self):
        """
        In log file, 'password' is masked.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        original_logger = instance._logger
        instance._logger = mock_logger

        try:
            Helper.set_property(instance, "password", "test")
            instance.trigger()

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
        finally:
            instance._logger = original_logger

    def test_logging_mask_aws_keys(self):
        """
        In log file, 'access_key' and 'secret_key' of AWS are masked.
        """
        instance = SampleCustomStep()

        mock_logger = MagicMock()
        original_logger = instance._logger
        instance._logger = mock_logger

        try:
            Helper.set_property(instance, "access_key", "test")
            Helper.set_property(instance, "secret_key", "test")
            instance.trigger()

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
        finally:
            instance._logger = original_logger

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
            Helper.set_property(instance, "logger", _get_logger(instance.__class__.__name__))

            ret = instance._source_path_reader({"file": dummy_pass})
            assert ret == dummy_pass
            with open(ret, "r") as fp:
                actual = fp.read()
                assert "test" == actual
        finally:
            shutil.rmtree(self._data_dir)

    def test_source_path_reader_with_content(self):
        instance = SampleCustomStep()
        Helper.set_property(instance, "logger", _get_logger(instance.__class__.__name__))
        ret = instance._source_path_reader({"content": "test"})
        with open(ret, "r") as fp:
            actual = fp.read()
            assert "test" == actual

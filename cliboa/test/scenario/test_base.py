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
import os
import shutil
import sys
from unittest import TestCase

from cliboa.conf import env
from cliboa.interface import CommandArgumentParser, ScenarioRunner
from cliboa.scenario.sample_step import SampleCustomStep
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestBase(TestCase):
    def setup_method(self, method):
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        cmd_parser = CommandArgumentParser()
        self._cmd_args = cmd_parser.parse()
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        runner = ScenarioRunner(self._cmd_args)
        runner.add_system_path()

    def test_logging_mask_password(self):
        """
        In log file, 'password' is masked.
        """
        instance = SampleCustomStep()
        instance._logger = LisboaLog.get_logger(__name__)
        Helper.set_property(instance, "logger", LisboaLog.get_logger(instance.__class__.__name__))
        Helper.set_property(instance, "password", "test")
        instance.trigger()
        ret = False
        with open(self._log_file, mode="r", encoding="utf-8") as f:
            for line in f:
                if "password : ****" in line:
                    ret = True
                    break
        self.assertTrue(ret)

    def test_logging_mask_aws_keys(self):
        """
        In log file, 'access_key' and 'secret_key' of AWS are masked.
        """
        instance = SampleCustomStep()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(instance.__class__.__name__))
        Helper.set_property(instance, "access_key", "test")
        Helper.set_property(instance, "secret_key", "test")
        instance.trigger()
        masked_access_key = False
        masked_secret_key = False
        with open(self._log_file, mode="r", encoding="utf-8") as f:
            for line in f:
                if "access_key : ****" in line:
                    masked_access_key = True
                elif "secret_key : ****" in line:
                    masked_secret_key = True
        self.assertTrue(masked_access_key)
        self.assertTrue(masked_secret_key)

    def test_source_path_reader_with_none(self):
        instance = SampleCustomStep()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(instance.__class__.__name__))
        ret = instance._source_path_reader(None)

        assert ret is None

    def test_source_path_reader_with_path(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            dummy_pass = os.path.join(self._data_dir, "id_rsa")
            with open(dummy_pass, "w") as f:
                f.write("test")

            instance = SampleCustomStep()
            Helper.set_property(
                instance, "logger", LisboaLog.get_logger(instance.__class__.__name__)
            )

            ret = instance._source_path_reader({"file": dummy_pass})
            assert ret == dummy_pass
            with open(ret, "r") as fp:
                actual = fp.read()
                assert "test" == actual
        finally:
            shutil.rmtree(self._data_dir)

    def test_source_path_reader_with_content(self):
        instance = SampleCustomStep()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(instance.__class__.__name__))
        ret = instance._source_path_reader({"content": "test"})
        with open(ret, "r") as fp:
            actual = fp.read()
            assert "test" == actual

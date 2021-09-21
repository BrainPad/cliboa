#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
import sys
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.listener import (
    ScenarioStatusListener,
    StepStatusListener,
    StepListener,
)
from cliboa.core.strategy import SingleProcExecutor, StepExecutor
from cliboa.core.worker import ScenarioWorker
from cliboa.scenario.sample_step import SampleCustomStep
from cliboa.util.exception import CliboaException
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestScenarioStatusListener(object):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._listener = ScenarioStatusListener()
        self._worker = ScenarioWorker(cmd_parser.parse())
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")

    def test_after_scenario(self):
        self._listener.after_scenario(self._worker)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Finish" in last_line

    def test_before_scenario(self):
        self._listener.before_scenario(self._worker)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Start" in last_line


class TestStepStatusListener(object):
    def setup_method(self, method):
        CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._listener = StepStatusListener()
        self._executor = StepExecutor(["1"])
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")

    def test_after_step(self):
        self._listener.after_step(self._executor)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Finish" in last_line

    def test_before_step(self):
        self._listener.before_step(self._executor)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Start" in last_line


class TestAppropriateListnerCall(unittest.TestCase):
    def test_end_with_noerror(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_before_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.before_step")
            )
            mock_error_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.error_step")
            )
            mock_after_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.after_step")
            )
            mock_post_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.after_completion")
            )

            step = SampleCustomStep()
            Helper.set_property(
                step, "logger", LisboaLog.get_logger(step.__class__.__name__)
            )
            Helper.set_property(step, "listeners", [StepStatusListener()])
            executor = SingleProcExecutor([step])
            executor.execute_steps(None)

            mock_before_step.assert_called_once()
            mock_error_step.assert_not_called()
            mock_after_step.assert_called_once()
            mock_post_step.assert_called_once()

    def test_end_with_error(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6 (mock#assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_before_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.before_step")
            )
            mock_error_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.error_step")
            )
            mock_after_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.after_step")
            )
            mock_post_step = stack.enter_context(
                patch("cliboa.core.listener.StepStatusListener.after_completion")
            )

            step = ErrorSampleCustomStep()
            Helper.set_property(
                step, "logger", LisboaLog.get_logger(step.__class__.__name__)
            )
            Helper.set_property(step, "listeners", [StepStatusListener()])
            executor = SingleProcExecutor([step])

            with self.assertRaises(CliboaException):
                executor.execute_steps(None)

            mock_before_step.assert_called_once()
            mock_error_step.assert_called_once()
            mock_after_step.assert_not_called()
            mock_post_step.assert_called_once()


class TestListenerArguments(unittest.TestCase):
    def test_ok(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        step = SampleCustomStep()
        Helper.set_property(
            step, "logger", LisboaLog.get_logger(step.__class__.__name__)
        )
        clz = CustomStepListener()
        values = {"test_key": "test_value"}
        clz.__dict__.update(values)
        Helper.set_property(step, "listeners", [clz])
        executor = SingleProcExecutor([step])
        executor.execute_steps(None)


class ErrorSampleCustomStep(SampleCustomStep):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        raise CliboaException("Something wrong")


class CustomStepListener(StepListener):
    def before_step(self, *args, **kwargs):
        dict = self.__dict__
        assert "test_value" == dict.get("test_key")

    def error_step(self, *args, **kwargs):
        dict = self.__dict__
        assert "test_value" == dict.get("test_key")

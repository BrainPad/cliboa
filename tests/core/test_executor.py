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
from contextlib import ExitStack
from unittest import TestCase
from unittest.mock import Mock, patch

from cliboa.core.executor import _ScenarioExecutor, _StepExecutor
from cliboa.core.model import CommandArgument, StepModel
from cliboa.listener.step import StepStatusListener
from cliboa.scenario.sample_step import SampleCustomStep, SampleStep, SampleStepSub
from cliboa.util.constant import StepStatus
from cliboa.util.exception import CliboaException


class TestScenarioExecutor:
    def test_execute_scenario_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStep",
                "arguments": {"retry_count": 1, "memo": "aaa"},
            }
        )
        instance = SampleStep(di_logger=mock_logger)
        executor = _StepExecutor(instance, model)
        worker = _ScenarioExecutor([executor])
        worker.execute()
        mock_logger.warning.assert_not_called()

    def test_execute_scenario_sub_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStepSub",
                "arguments": {"retry_count": 1, "memo": "aaa", "name": "Alice"},
            }
        )
        instance = SampleStepSub(di_logger=mock_logger)
        executor = _StepExecutor(instance, model)
        worker = _ScenarioExecutor([executor])
        worker.execute()
        mock_logger.info.assert_any_call("my name is Alice")
        mock_logger.info.assert_any_call("my memo is aaa")


class TestStepExecutor:
    def test_execute_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStep",
                "arguments": {"retry_count": 1, "memo": "aaa"},
            }
        )
        instance = SampleStep(di_logger=mock_logger)
        executor = _StepExecutor(instance, model)
        executor.execute()
        mock_logger.warning.assert_not_called()

    def test_execute_with_ignored_both_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStep",
                "arguments": {"retry_count": 1, "memo": "aaa"},
            }
        )
        instance = SampleStep(di_logger=mock_logger)
        cmd_args = CommandArgument.model_validate(
            {"args": [1, 2, "hoge"], "kwargs": {"key_aaa": 12345}}
        )
        executor = _StepExecutor(instance, model, cmd_args)
        executor.execute()
        mock_logger.warning.assert_not_called()

    def test_execute_with_kwargs_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStepSub",
                "arguments": {"retry_count": 1, "memo": "aaa", "name": "Alice"},
            }
        )
        instance = SampleStepSub(di_logger=mock_logger)
        cmd_args = CommandArgument.model_validate({"kwargs": {"key_aaa": 12345}})
        executor = _StepExecutor(instance, model, cmd_args)
        executor.execute()
        mock_logger.info.assert_any_call("kwargs is {'key_aaa': 12345}")

    def test_execute_with_ignored_args_ok(self):
        mock_logger = Mock()
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStepSub",
                "arguments": {"retry_count": 1, "memo": "aaa", "name": "Alice"},
            }
        )
        instance = SampleStepSub(di_logger=mock_logger)
        cmd_args = CommandArgument.model_validate({"args": [1, 2, "hoge"]})
        executor = _StepExecutor(instance, model, cmd_args)
        executor.execute()
        mock_logger.info.assert_any_call("kwargs is {}")


class TestAppropriateListnerCall(TestCase):
    def test_end_with_noerror(self):
        with ExitStack() as stack:
            mock_before = stack.enter_context(
                patch("cliboa.listener.step.StepStatusListener.before")
            )
            mock_error = stack.enter_context(patch("cliboa.listener.step.StepStatusListener.error"))
            mock_after = stack.enter_context(patch("cliboa.listener.step.StepStatusListener.after"))
            mock_post_step = stack.enter_context(
                patch("cliboa.listener.step.StepStatusListener.completion")
            )

            step = SampleCustomStep()
            model = StepModel.model_validate({"step": "hoge", "class": "SampleCustomStep"})
            executor = _StepExecutor(step, model, CommandArgument())
            executor.register_listener(StepStatusListener())
            executor.execute()

            mock_before.assert_called_once()
            mock_error.assert_not_called()
            mock_after.assert_called_once()
            mock_post_step.assert_called_once()

    def test_end_with_error(self):
        with ExitStack() as stack:
            mock_before = stack.enter_context(
                patch("cliboa.listener.step.StepStatusListener.before")
            )
            mock_error = stack.enter_context(patch("cliboa.listener.step.StepStatusListener.error"))
            mock_after = stack.enter_context(patch("cliboa.listener.step.StepStatusListener.after"))
            mock_post_step = stack.enter_context(
                patch("cliboa.listener.step.StepStatusListener.completion")
            )

            step = ErrorSampleCustomStep()
            model = StepModel.model_validate({"step": "hoge", "class": "ErrorSampleCustomStep"})
            executor = _StepExecutor(step, model, CommandArgument())
            executor.register_listener(StepStatusListener())
            res = executor.execute()

            assert res == StepStatus.ABNORMAL_TERMINATION
            mock_before.assert_called_once()
            mock_error.assert_called_once()
            mock_after.assert_not_called()
            mock_post_step.assert_called_once()


class ErrorSampleCustomStep(SampleCustomStep):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        raise CliboaException("Something wrong")

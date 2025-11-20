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
from cliboa.core.executor import _StepExecutor
from cliboa.core.model import ParallelConfigModel, StepModel
from cliboa.core.processor import _ParallelProcessor
from cliboa.scenario.sample_step import SampleStep
from cliboa.util.constant import StepStatus
from cliboa.util.exception import CliboaException
from tests import BaseCliboaTest


class TestParallelProcessor(BaseCliboaTest):
    """
    Test class for processor.py
    """

    def _get_multi_process_executor(self, force_continue: bool, has_error: bool = True):
        model = StepModel.model_validate(
            {
                "step": "sample",
                "class": "SampleStep",
            }
        )
        step1 = _StepExecutor(SampleStep(), model)
        if has_error:
            step2 = _StepExecutor(ErrorSampleStep(), model)
        else:
            step2 = _StepExecutor(SampleStep(), model)
        return _ParallelProcessor(
            [step1, step2], ParallelConfigModel(force_continue=force_continue)
        )

    def test_multi_process_success(self):
        """
        Return None when error is not occurred.
        """
        executor = self._get_multi_process_executor(False, False)
        res = executor.execute()
        assert res is None

    def test_multi_process_error_stop(self):
        """
        Return int - ABNORMAL_TERMINATION - when force_continue is False and error is occurred.
        """
        executor = self._get_multi_process_executor(False)
        res = executor.execute()
        assert res == StepStatus.ABNORMAL_TERMINATION

    def test_multi_process_error_continue(self):
        """
        Return None when force_continue is True and error is occurred.
        """
        executor = self._get_multi_process_executor(True)
        res = executor.execute()
        assert res is None


class ErrorSampleStep(SampleStep):
    def execute(self):
        raise CliboaException("Something wrong")

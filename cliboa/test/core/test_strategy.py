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
import sys

from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import StepQueue
from cliboa.core.strategy import MultiProcExecutor, MultiProcWithConfigExecutor, SingleProcExecutor
from cliboa.interface import CommandArgumentParser
from cliboa.scenario.sample_step import SampleStep
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import CliboaException, StepExecutionFailed
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.parallel_with_config import ParallelWithConfig


class TestStrategy(BaseCliboaTest):
    """
    Test class for strategy.py
    """

    MULTI_PROC_SUPPORT_VER = 35

    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()

    def test_single_process_executor_ok(self):
        """
        Test SingleProcExecutor::execute_steps
        """
        instance = SampleStep()
        strategy = SingleProcExecutor([instance])
        strategy.execute_steps(self._cmd_args)

    def test_multi_process_error_stop(self):
        py_info = sys.version_info
        major_ver = py_info[0]
        minor_ver = py_info[1]
        py_ver = int(str(major_ver) + str(minor_ver))

        log = LisboaLog.get_logger(self.__class__.__name__)
        log.info(minor_ver)
        if py_ver >= self.MULTI_PROC_SUPPORT_VER:
            step1 = SampleStep()
            Helper.set_property(step1, "logger", LisboaLog.get_logger(step1.__class__.__name__))
            step2 = ErrorSampleStep()
            Helper.set_property(step2, "logger", LisboaLog.get_logger(step2.__class__.__name__))

            q = StepQueue()
            q.force_continue = False
            setattr(ScenarioQueue, "step_queue", q)

            executor = MultiProcExecutor([step1, step2])
            try:
                executor.execute_steps(None)
                self.fail("Error must be occured")
            except StepExecutionFailed:
                pass

    def test_multi_process_error_continue(self):
        py_info = sys.version_info
        major_ver = py_info[0]
        minor_ver = py_info[1]
        py_ver = int(str(major_ver) + str(minor_ver))

        if py_ver >= self.MULTI_PROC_SUPPORT_VER:
            step1 = SampleStep()
            Helper.set_property(step1, "logger", LisboaLog.get_logger(step1.__class__.__name__))
            step2 = ErrorSampleStep()
            Helper.set_property(step2, "logger", LisboaLog.get_logger(step2.__class__.__name__))

            q = StepQueue()
            q.force_continue = True
            setattr(ScenarioQueue, "step_queue", q)

            executor = MultiProcExecutor([step1, step2])
            executor.execute_steps(None)

    def test_multi_with_config_process_error_stop(self):
        py_info = sys.version_info
        major_ver = py_info[0]
        minor_ver = py_info[1]
        py_ver = int(str(major_ver) + str(minor_ver))

        log = LisboaLog.get_logger(self.__class__.__name__)
        log.info(minor_ver)
        if py_ver >= self.MULTI_PROC_SUPPORT_VER:
            step1 = SampleStep()
            Helper.set_property(step1, "logger", LisboaLog.get_logger(step1.__class__.__name__))
            step2 = ErrorSampleStep()
            Helper.set_property(step2, "logger", LisboaLog.get_logger(step2.__class__.__name__))
            config = {"multi_process_count": 3}

            q = StepQueue()
            q.force_continue = False
            setattr(ScenarioQueue, "step_queue", q)

            executor = MultiProcWithConfigExecutor([ParallelWithConfig([step1, step2], config)])
            try:
                executor.execute_steps(None)
                self.fail("Error must be occured")
            except StepExecutionFailed:
                pass

    def test_multi_with_config_process_error_continue(self):
        py_info = sys.version_info
        major_ver = py_info[0]
        minor_ver = py_info[1]
        py_ver = int(str(major_ver) + str(minor_ver))

        if py_ver >= self.MULTI_PROC_SUPPORT_VER:
            step1 = SampleStep()
            Helper.set_property(step1, "logger", LisboaLog.get_logger(step1.__class__.__name__))
            step2 = ErrorSampleStep()
            Helper.set_property(step2, "logger", LisboaLog.get_logger(step2.__class__.__name__))
            config = {"multi_process_count": 3}

            q = StepQueue()
            q.force_continue = True
            setattr(ScenarioQueue, "step_queue", q)

            executor = MultiProcWithConfigExecutor([ParallelWithConfig([step1, step2], config)])
            executor.execute_steps(None)


class ErrorSampleStep(SampleStep):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        raise CliboaException("Something wrong")

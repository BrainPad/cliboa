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
import shutil
import os
import sys
import pytest
import yaml
from pprint import pprint

from cliboa.client import CommandArgumentParser
from cliboa.core.listener import StepStatusListener
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import *
from cliboa.core.strategy import *
from cliboa.scenario.sample_step import SampleStep


class TestStrategy(object):
    """
    Test class for strategy.py
    """

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
        strategy.regist_listeners(StepStatusListener())
        strategy.execute_steps(self._cmd_args)

    def test_multi_process_executor_ok(self):
        """
        Test MultiProcExecutor::execute_steps
        """
        instance1 = SampleStep()
        instance2 = SampleStep()
        strategy = MultiProcExecutor([instance1, instance2])
        strategy.regist_listeners(StepStatusListener())
        strategy.execute_steps(self._cmd_args)

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
import pytest
from pprint import pprint

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.listener import *
from cliboa.core.worker import *
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import *
from cliboa.core.strategy import StepExecutor


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
        cmd_parser = CommandArgumentParser()
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

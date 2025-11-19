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
from unittest import TestCase

from cliboa.conf import env
from cliboa.core.executor import _ScenarioExecutor
from cliboa.core.listener import ScenarioStatusListener, StepStatusListener
from cliboa.scenario.sample_step import SampleCustomStep


class TestScenarioStatusListener(TestCase):
    def setup_method(self, method):
        self._listener = ScenarioStatusListener()
        self._executor = _ScenarioExecutor([])
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")

    def test_completion(self):
        self._listener.completion(self._executor)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Complete" in last_line

    def test_before(self):
        self._listener.before(self._executor)
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        assert "Start" in last_line


class TestStepStatusListener:
    def setup_method(self, method):
        self._listener = StepStatusListener()
        self._step = SampleCustomStep()
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")

    def _get_last_log(self):
        with open(self._log_file) as f:
            for line in f:
                pass
            last_line = line
        return last_line

    def test_after(self):
        self._listener.after(self._step)
        last_line = self._get_last_log()
        assert "Finish" in last_line

    def test_before(self):
        self._listener.before(self._step)
        last_line = self._get_last_log()
        assert "Start" in last_line

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
from types import SimpleNamespace

from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.worker import ScenarioWorker
from cliboa.scenario.sample_step import SampleStep


class TestWorker(object):
    def setup_method(self, method):
        cmd_args = {"project_name": "spam", "format": "yaml"}
        self._cmd_args = SimpleNamespace(**cmd_args)

    def test_execute_scenario_ok(self):
        """
        Test ScenarioWorker::execute_scenario
        """
        q = ScenarioQueue.step_queue
        instance = SampleStep()
        q.push([instance])
        worker = ScenarioWorker(self._cmd_args)
        worker.execute_scenario()

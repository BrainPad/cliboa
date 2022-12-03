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

from cliboa.interface import CommandArgumentParser
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.worker import ScenarioWorker
from cliboa.scenario.sample_step import SampleStep


class TestWorker(object):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()

    def test_execute_scenario_ok(self):
        """
        Test ScenarioWorker::execute_scenario
        """
        q = ScenarioQueue.step_queue
        instance = SampleStep()
        q.push([instance])
        worker = ScenarioWorker(self._cmd_args)
        worker.execute_scenario()

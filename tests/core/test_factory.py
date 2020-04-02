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

import pytest

from cliboa.client import CommandArgumentParser
from cliboa.core.factory import ScenarioManagerFactory, StepExecutorFactory
from cliboa.core.manager import YamlScenarioManager
from cliboa.core.strategy import MultiProcExecutor, SingleProcExecutor


class TestFactory(object):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()

    def test_scenario_manager_factory_ok_yml(self):
        """
        Succeeded to create instance with yml
        """
        manager = ScenarioManagerFactory.create(self._cmd_args)
        assert isinstance(manager, type(YamlScenarioManager(self._cmd_args)))

    def test_scenario_manager_factory_ng(self):
        """
        Failed to create instance
        """
        with pytest.raises(AttributeError) as excinfo:
            ScenarioManagerFactory.create("")
        assert "object has no attribute" in str(excinfo.value)

    def test_step_executor_strategy_factory_ok_single(self):
        """
        Succeeded to create SingleProcess instance
        """

        s = StepExecutorFactory.create(["1"])
        assert isinstance(s, type(SingleProcExecutor(None)))

    def test_step_executor_strategy_factory_ok_multi(self):
        """
        Succeeded to create MultiProcess instance
        """
        s = StepExecutorFactory.create(["1", "2"])
        assert isinstance(s, type(MultiProcExecutor(None)))

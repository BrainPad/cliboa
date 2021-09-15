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
from cliboa.core.factory import CustomInstanceFactory, ScenarioManagerFactory, StepExecutorFactory
from cliboa.core.manager import YamlScenarioManager
from cliboa.core.strategy import MultiProcExecutor, MultiProcWithConfigExecutor, SingleProcExecutor
from cliboa.test import BaseCliboaTest
from cliboa.util.parallel_with_config import ParallelWithConfig


class TestFactory(BaseCliboaTest):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        self._cmd_args = cmd_parser.parse()


class TestScenarioManagerFactory(TestFactory):
    def test_create_ok(self):
        """
        Succeeded to create instance with yml
        """
        manager = ScenarioManagerFactory.create(self._cmd_args)
        self.assertTrue(isinstance(manager, type(YamlScenarioManager(self._cmd_args))))

    def test_create_ng(self):
        """
        Failed to create instance
        """
        with pytest.raises(AttributeError) as excinfo:
            ScenarioManagerFactory.create("")
        assert "object has no attribute" in str(excinfo.value)


class TestStepExecutorFactory(TestFactory):
    def test_create_single(self):
        """
        Succeeded to create SingleProcess instance
        """

        s = StepExecutorFactory.create(["1"])
        self.assertTrue(isinstance(s, type(SingleProcExecutor(None))))

    def test_create_multi(self):
        """
        Succeeded to create MultiProcess instance
        """
        s = StepExecutorFactory.create(["1", "2"])
        self.assertTrue(isinstance(s, type(MultiProcExecutor(None))))

    def test_create_multi_with_config(self):
        """
        Succeeded to create MultiProcess instance with config
        """
        instance = [ParallelWithConfig(["1", "2"], {"multi_process_count": 2})]
        s = StepExecutorFactory.create(instance)
        print(s)
        self.assertTrue(isinstance(s, type(MultiProcWithConfigExecutor(instance))))


class TestCustomInstanceFactory(TestFactory):
    def test_execute_no_candidates(self):
        custom_instance = CustomInstanceFactory.create("NotCustomClass")
        assert custom_instance is None

    def test_execute_with_candidates(self):
        sys.path.append("cliboa/scenario")
        custom_instance = CustomInstanceFactory.create("SampleStep")
        assert custom_instance is not None

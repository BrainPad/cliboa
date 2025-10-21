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

import pytest

from cliboa.core.factory import _create_custom_instance, _create_step_executor

# from cliboa.core.manager import ScenarioManager
from cliboa.core.strategy import MultiProcExecutor, SingleProcExecutor
from cliboa.util.parallel_with_config import ParallelWithConfig
from tests import BaseCliboaTest


class TestFactory(BaseCliboaTest):
    def setup_method(self, method):
        pass


# @pytest.mark.skip(reason="ScenarioManager is scheduled for a redesign for v3.")
# class TestScenarioManagerFactory(TestFactory):
#     def test_create_ok(self):
#         """
#         Succeeded to create instance with yml and json
#         """
#         manager = ScenarioManagerFactory.create("spam", "yaml")
#         self.assertTrue(isinstance(manager, YamlScenarioManager))
#
#         manager = ScenarioManagerFactory.create("spam", "json")
#         self.assertTrue(isinstance(manager, JsonScenarioManager))
#
#     def test_create_ng(self):
#         """
#         Failed to create instance
#         """
#         with pytest.raises(AttributeError) as excinfo:
#             ScenarioManagerFactory.create("")
#         assert "object has no attribute" in str(excinfo.value)


class Test_create_step_executor(TestFactory):
    def test_create_single(self):
        """
        Succeeded to create SingleProcess instance
        """

        s = _create_step_executor("1")
        self.assertTrue(isinstance(s, SingleProcExecutor))

    def test_create_multi_with_config(self):
        """
        Succeeded to create MultiProcess instance with config
        """
        instance = ParallelWithConfig(["1", "2"], {"multi_process_count": 2})
        s = _create_step_executor(instance)
        self.assertTrue(isinstance(s, MultiProcExecutor))


class Test_create_custom_instance(TestFactory):
    def test_execute_no_candidates(self):
        custom_instance = _create_custom_instance("NotCustomClass")
        assert custom_instance is None

    @pytest.mark.skip(
        "The factory is scheduled for a redesign for v3"
        " which will eliminate the sys.path.append dependency."
    )
    def test_execute_with_candidates(self):
        # sys.path.append("cliboa/scenario")
        custom_instance = _create_custom_instance("SampleStep")
        assert custom_instance is not None

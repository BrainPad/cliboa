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
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cliboa.core.manager import ScenarioManager
from cliboa.core.model import CommandArgument


class TestScenarioManager:
    """
    Unit test cases for ScenarioManager
    """

    def test_init_ok(self):
        """
        Test __init__: Verify that 'scenario_builder' is set correctly
        using Dependency Injection (DI).
        """

        # A mock builder class to be injected via DI
        class MockBuilder:
            def __init__(self, file, cmn_file, fmt, cmd, *args, **kwargs):
                self.file = file
                self.cmn_file = cmn_file
                self.fmt = fmt
                self.cmd = cmd
                self.args = args  # Inherited from _BaseObject
                self.kwargs = kwargs  # Inherited from _BaseObject

        dummy_cmd = CommandArgument()
        dummy_args = ("pos_arg1",)
        mock_logger = MagicMock()
        dummy_kwargs = {
            "kw_arg1": "val1",
            "di_scenario_builder": MockBuilder,
            "di_logger": mock_logger,
        }

        # Instantiate ScenarioManager using DI
        # (di_scenario_builder and di_logger)
        manager = ScenarioManager("test.yml", None, "yaml", dummy_cmd, *dummy_args, **dummy_kwargs)

        # Verify that _builder is an instance of MockBuilder
        assert isinstance(manager._builder, MockBuilder)

        # Verify that the injected logger was assigned
        assert manager._logger is mock_logger

        # Verify that the arguments passed to ScenarioManager._resolve
        # were correctly passed to MockBuilder.__init__
        assert manager._builder.file == "test.yml"
        assert manager._builder.fmt == "yaml"
        assert manager._builder.cmd is dummy_cmd

        assert len(manager._builder.args) == 0
        assert manager._builder.kwargs == dummy_kwargs

    def test_execute_ok(self):
        """
        Test execute: Verify that the execution flow is correctly controlled
        using the DI classes.
        """
        mock_steps = ["step_a", "step_b"]
        mock_builder_execute = MagicMock(return_value=mock_steps)
        mock_executor_register = MagicMock()
        mock_executor_execute = MagicMock(return_value=0)
        mock_logger_instance = MagicMock()

        created_executor_instances = []

        dummy_kwargs = {"kw_arg_exec": "val_exec"}

        # --- Define Mock Classes ---
        class MockBuilder:
            def __init__(self, *args, **kwargs):
                self.execute = mock_builder_execute

        class MockExecutor:
            def __init__(self, steps, *args, **kwargs):
                # Checkpoint 2: Assert constructor argument
                assert steps == mock_steps, "Executor did not receive correct steps"

                assert "kw_arg_exec" in kwargs
                assert kwargs["di_logger"] is mock_logger_instance

                self.register_listener = mock_executor_register
                self.execute = mock_executor_execute
                created_executor_instances.append(self)

        class MockListener:
            def __init__(self, *args, **kwargs):
                assert "kw_arg_exec" in kwargs
                assert kwargs["di_logger"] is mock_logger_instance

        sm_kwargs = dummy_kwargs | {
            "di_scenario_builder": MockBuilder,
            "di_scenario_executor": MockExecutor,
            "di_scenario_status_listener": MockListener,
            "di_logger": mock_logger_instance,
        }

        # Mock state (outside test scope)
        with patch("cliboa.core.manager.state", MagicMock()):
            manager = ScenarioManager("scenario.yml", "yaml", None, **sm_kwargs)

            return_code = manager.execute()

        assert len(created_executor_instances) == 1

        # Checkpoint 1: builder.execute() was called once
        mock_builder_execute.assert_called_once_with()

        # Checkpoint 3: executor.register_listener() was called
        mock_executor_register.assert_called_once()
        assert isinstance(mock_executor_register.call_args[0][0], MockListener)

        # Checkpoint 4: executor.execute() was called once
        mock_executor_execute.assert_called_once_with()

        # Verify return code
        assert return_code == 0


@pytest.fixture
def scenario_environment(tmp_path):
    pj_dir = tmp_path / "project" / "spam"
    scenario_yaml_file = pj_dir / "scenario.yml"

    os.makedirs(pj_dir)

    yield pj_dir, scenario_yaml_file


class TestScenarioManagerIntegrated:
    def test_create_scenario_queue_ok(self, scenario_environment):
        pj_dir, scenario_yaml_file = scenario_environment

        test_data = {
            "scenario": [
                {
                    "arguments": {"src_dir": str(pj_dir), "src_pattern": r"(.*)\.csv"},
                    "class": "FileRename",
                    "step": "",
                }
            ]
        }

        with open(scenario_yaml_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))

        manager = ScenarioManager(str(scenario_yaml_file))
        manager.execute()

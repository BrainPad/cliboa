import logging
from typing import Any, Dict, Type
from unittest.mock import MagicMock, Mock

import pytest

from cliboa.core.builder import _ScenarioBuilder
from cliboa.core.model import StepModel
from cliboa.scenario.sample_step import SampleStepSub


class MockStepExecutor:
    """Mock implementation of _StepExecutor."""

    def __init__(self, instance: Any, step: StepModel, cmd_arg: Any, *args, **kwargs):
        self.instance = instance
        self.step = step
        self.cmd_arg = cmd_arg
        self.register_listener = MagicMock()


class MockParallelProcessor:
    """Mock implementation of _ParallelProcessor."""

    def __init__(self, instances: list, parallel_config: Any, *args, **kwargs):
        self.instances = instances
        self.parallel_config = parallel_config


class MockStepStatusListener:
    """Mock implementation of StepStatusListener."""

    pass


class MockFactory:
    """
    Mock factory that asserts 'create' is called with a string.
    """

    def create(self, class_name: Any, **kwargs) -> Mock:
        """
        Mock create method.
        Requirement: Fail if argument is not a string.
        """
        if not isinstance(class_name, str):
            raise TypeError(f"Factory received non-string argument: {type(class_name)}")

        mock_instance = Mock()
        mock_instance.class_name = class_name
        return mock_instance


# class MockFactorySub:
#     def create(self, class_name: Any, **kwargs) -> SampleStepSub:
#         return SampleStepSub(**kwargs)


class TestScenarioBuilderExecute:
    """
    Tests for _ScenarioBuilder.execute method.
    """

    def _create_dummy_loader_cls(
        self, main_scenario_dict: Dict[str, Any], common_scenario_dict: Dict[str, Any] | None = None
    ) -> Type:
        """
        Helper to create a dummy loader CLASS as required by the DI.
        """

        class DummyLoader:
            """
            A mock ScenarioLoader class that satisfies the DI requirements.
            """

            def __init__(self, scenario_file: str, is_main: bool):
                self._is_main = is_main
                self._scenario_file = scenario_file

            def __call__(self) -> Dict[str, Any] | None:
                if self._is_main:
                    return main_scenario_dict
                else:
                    return common_scenario_dict

        return DummyLoader

    def _assert_mock_called_at_least_once(self, mock_func: MagicMock, *args) -> None:
        calls = mock_func.mock_calls
        found = False
        for c in calls:
            if len(args) > len(c.args):
                continue
            args_match = True
            for i, expected_arg in enumerate(args):
                if expected_arg != c.args[i]:
                    args_match = False
                    break
            if args_match:
                found = True
                break
        assert found, f"Mock(*{args}) call not found"

    @pytest.fixture
    def mock_factory(self) -> MagicMock:
        """
        Fixture for the custom MockFactory.
        """
        factory_instance = MockFactory()
        factory_instance.create = MagicMock(side_effect=factory_instance.create)
        return factory_instance

    def test_execute_simple_steps(self, mock_factory: MagicMock):
        """
        Test execute with a scenario containing two simple steps.
        Checks length and instance types of the result.
        """
        main_scenario = {
            "scenario": [
                {"step": "Step1", "class": "DummyStep1", "arguments": {"arg1": "val1"}},
                {"step": "Step2", "class": "DummyStep2", "arguments": {"arg2": "val2"}},
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 2
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 2
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyStep1")
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyStep2")

        assert steps[0].register_listener.call_count == 1
        assert steps[1].register_listener.call_count == 1

        assert steps[0].step.step == "Step1"
        assert steps[1].step.step == "Step2"

    def test_execute_parallel_steps(self, mock_factory: MagicMock):
        """
        Test execute with a scenario containing one parallel block.
        """
        main_scenario = {
            "scenario": [
                {
                    "parallel": [
                        {"step": "PStep1", "class": "DummyPStep1"},
                        {"step": "PStep2", "class": "DummyPStep2"},
                    ],
                    "parallel_config": {"multi_process_count": 3},
                }
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockParallelProcessor)

        assert mock_factory.create.call_count == 2

        parallel_processor = steps[0]
        assert len(parallel_processor.instances) == 2

        assert parallel_processor.instances[0].register_listener.call_count == 1
        assert parallel_processor.instances[1].register_listener.call_count == 1

    def test_execute_mixed_steps(self, mock_factory: MagicMock):
        """
        Test execute with mixed simple and parallel steps.
        """
        main_scenario = {
            "scenario": [
                {"step": "Step1", "class": "DummyStep1"},
                {
                    "parallel": [
                        {"step": "PStep1", "class": "DummyPStep1"},
                    ]
                },
                {"step": "Step3", "class": "DummyStep3"},
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 3
        assert isinstance(steps[0], MockStepExecutor)
        assert isinstance(steps[1], MockParallelProcessor)
        assert isinstance(steps[2], MockStepExecutor)

        assert mock_factory.create.call_count == 3

        assert steps[0].register_listener.call_count == 1
        assert steps[1].instances[0].register_listener.call_count == 1
        assert steps[2].register_listener.call_count == 1

    def test_execute_merge_common_arguments(self, mock_factory: MagicMock):
        """
        Test: Common file arguments are merged correctly.
        """
        common_scenario = {
            "scenario": [
                {
                    "step": "CommonStep1",
                    "class": "DummyStep1",
                    "arguments": {"arg_common": "common_val", "arg_override": "common_override"},
                }
            ]
        }
        main_scenario = {
            "scenario": [
                {
                    "step": "MainStep1",
                    "class": "DummyStep1",
                    "arguments": {"arg_main": "main_val", "arg_override": "main_override"},
                }
            ]
        }

        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario, common_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            common_file="common.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        final_arguments = steps[0].step.arguments

        expected_arguments = {
            "arg_common": "common_val",
            "arg_main": "main_val",
            "arg_override": "main_override",
        }
        assert final_arguments == expected_arguments

    def test_execute_common_file_load_failure(
        self,
        mock_factory: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ):
        """
        Test: A warning log is emitted if the common file loader returns None.
        """
        main_scenario = {"scenario": [{"step": "Step1", "class": "DummyStep1"}]}

        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario, common_scenario_dict=None)

        common_file_path = "invalid_common.yml"
        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            common_file=common_file_path,
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        with caplog.at_level(logging.WARNING):
            steps = builder.execute()

        assert len(steps) == 1
        assert steps[0].step.step == "Step1"

        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.WARNING
        assert f"Failed to load {common_file_path}" in caplog.records[0].message

    def test_execute_with_single_listener(self, mock_factory: MagicMock):
        """
        Test: A step with a single listener (str). (Normal case)
        Checks factory calls and listener registration.
        """
        main_scenario = {
            "scenario": [
                {
                    "step": "Step1",
                    "class": "DummyStep1",
                    "listeners": "DummyListener",
                    "arguments": {"listener_arg": "val"},
                }
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 2
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyStep1")
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyListener")

        assert steps[0].register_listener.call_count == 2

    def test_execute_with_multiple_listeners(self, mock_factory: MagicMock):
        """
        Test: A step with multiple listeners (list[str]). (Normal case)
        Checks factory calls and listener registration after code fix.
        """
        main_scenario = {
            "scenario": [
                {
                    "step": "Step1",
                    "class": "DummyStep1",
                    "listeners": ["DummyListener1", "DummyListener2"],
                }
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 3

        self._assert_mock_called_at_least_once(mock_factory.create, "DummyStep1")
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyListener1")
        self._assert_mock_called_at_least_once(mock_factory.create, "DummyListener2")

        assert steps[0].register_listener.call_count == 3

    def test_execute_with_symbol(self):
        main_scenario = {
            "scenario": [
                {"step": "Step1", "class": "SampleStep", "arguments": {"memo": "val1"}},
                {"step": "Step2", "class": "SampleStepSub", "symbol": "Step1"},
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)
        mock_logger = MagicMock()

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_logger=mock_logger,
        )

        steps = builder.execute()
        assert type(steps[1].step) is SampleStepSub
        # steps[0].execute()
        steps[0].step.put_to_context("xyz")
        # self.debug_print(steps[1].step)
        # mock_logger.debug(f"{steps[1].step}: {steps[1].step.__dict__}")
        steps[1].execute()

        # print(mock_logger)
        # print(f"debug: {mock_logger.debug.call_args_list}")
        # print(f"info: {mock_logger.info.call_args_list}")
        # print(f"warning: {mock_logger.warning.call_args_list}")
        # print(f"error: {mock_logger.error.call_args_list}")
        # print(f"exception: {mock_logger.exception.call_args_list}")
        mock_logger.info.assert_any_call("symbol memo is val1")
        mock_logger.info.assert_any_call("symbol context is xyz")

    def test_execute_with_symbol_returns_instance_arguments_with_defaults(self):
        """
        Builder wires symbol_step to the referenced BaseStep; get_symbol_arguments()
        must expose Pydantic Arguments defaults absent from YAML.
        """
        main_scenario = {
            "scenario": [
                {"step": "Step1", "class": "SampleStep", "arguments": {"memo": "val1"}},
                {"step": "Step2", "class": "SampleStepSub", "symbol": "Step1"},
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)
        mock_logger = MagicMock()

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_logger=mock_logger,
        )

        steps = builder.execute()
        assert len(steps) == 2

        sym_args = steps[1].get_symbol_arguments()
        assert sym_args.get("memo") == "val1"
        assert sym_args.get("retry_count") == 3
        assert "retry_count" not in steps[0].raw_arguments

    def test_execute_with_symbol_returns_instance_arguments_with_defaults_v2(self):
        """
        Builder wires symbol_step to the referenced BaseStep; get_symbol_arguments()
        must expose instance arguments with defaults absent from YAML.
        """
        main_scenario = {
            "scenario": [
                {"step": "Step1", "class": "SampleStepSubV2", "arguments": {"memo": "val1"}},
                {"step": "Step2", "class": "SampleStepSub", "symbol": "Step1"},
            ]
        }
        DummyLoaderCls = self._create_dummy_loader_cls(main_scenario)
        mock_logger = MagicMock()

        builder = _ScenarioBuilder(
            scenario_file="main.yml",
            di_loader=DummyLoaderCls,
            di_logger=mock_logger,
        )

        steps = builder.execute()
        assert len(steps) == 2

        sym_args = steps[1].get_symbol_arguments()
        assert sym_args.get("memo") == "val1"
        assert sym_args.get("retry_count") == 3
        assert "retry_count" not in steps[0].raw_arguments


class TestRecipeDirsValidation:
    """Tests for _ScenarioBuilder._validate_recipe_dirs (static)."""

    def test_none_returns_empty_list(self):
        assert _ScenarioBuilder._validate_recipe_dirs(None) == []

    def test_empty_list_returns_empty_list(self):
        assert _ScenarioBuilder._validate_recipe_dirs([]) == []

    def test_existing_dirs_returned(self, tmp_path):
        d1 = tmp_path / "a"
        d2 = tmp_path / "b"
        d1.mkdir()
        d2.mkdir()
        result = _ScenarioBuilder._validate_recipe_dirs([str(d1), str(d2)])
        assert result == [str(d1), str(d2)]

    def test_non_list_rejected(self):
        from cliboa.util.exception import CliboaRuntimeError

        with pytest.raises(CliboaRuntimeError, match="must be a list"):
            _ScenarioBuilder._validate_recipe_dirs("/some/path")

    def test_non_string_entry_rejected(self):
        from cliboa.util.exception import CliboaRuntimeError

        with pytest.raises(CliboaRuntimeError, match="must contain path strings"):
            _ScenarioBuilder._validate_recipe_dirs([123])

    def test_non_existent_dir_rejected(self, tmp_path):
        from cliboa.util.exception import CliboaRuntimeError

        bogus = str(tmp_path / "does_not_exist")
        with pytest.raises(CliboaRuntimeError, match="non-existent directory"):
            _ScenarioBuilder._validate_recipe_dirs([bogus])

    def test_one_existing_one_missing(self, tmp_path):
        from cliboa.util.exception import CliboaRuntimeError

        good = tmp_path / "good"
        good.mkdir()
        bad = str(tmp_path / "bad")
        with pytest.raises(CliboaRuntimeError, match="non-existent directory"):
            _ScenarioBuilder._validate_recipe_dirs([str(good), bad])


class TestBuilderRecipeIntegration:
    """
    End-to-end tests using real scenario.yml + recipe.yml files. The builder
    is constructed normally (no DI for loader/expander) and the env's
    RECIPE_DIRS is patched per-test.
    """

    @pytest.fixture
    def patched_env(self, mocker):
        """
        Patch cliboa.core.builder.env.get to return a per-test RECIPE_DIRS
        while preserving other env lookups.
        """
        import cliboa.core.builder as builder_mod

        original_get = builder_mod.env.get

        def _factory(recipe_dirs):
            def fake_get(key, default=None):
                if key == "RECIPE_DIRS":
                    return recipe_dirs
                return original_get(key, default)

            mocker.patch.object(builder_mod.env, "get", side_effect=fake_get)

        return _factory

    def _write_yaml(self, path, content: dict) -> None:
        import os

        import yaml

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            yaml.safe_dump(content, f, sort_keys=False)

    def test_include_expands_end_to_end(self, tmp_path, patched_env):
        recipe_dir = tmp_path / "recipe"
        scenario_path = tmp_path / "scenario.yml"
        recipe_path = recipe_dir / "echo.yml"

        self._write_yaml(
            str(scenario_path),
            {
                "scenario": [
                    {
                        "recipe": "echo",
                        "arguments": {"msg": "hello"},
                    }
                ]
            },
        )
        self._write_yaml(
            str(recipe_path),
            {
                "parameters": {"msg": "a message"},
                "recipe": [
                    {
                        "step": "Say",
                        "class": "SampleStep",
                        "arguments": {"memo": "{{ args.msg }}"},
                    }
                ],
            },
        )

        patched_env([str(recipe_dir)])

        builder = _ScenarioBuilder(
            scenario_file=str(scenario_path),
            file_format="yaml",
        )
        steps = builder.execute()
        assert len(steps) == 1
        # The {{ args.msg }} reference was substituted at phase 1, then phase 2
        # (which only resolves `{{ ... }}` patterns and there are none left)
        # leaves the value untouched. SampleStep.args.memo reflects the result.
        assert steps[0].step.args.memo == "hello"

    def test_unset_recipe_dirs_disables_feature(self, tmp_path, patched_env):
        scenario_path = tmp_path / "scenario.yml"
        self._write_yaml(
            str(scenario_path),
            {
                "scenario": [
                    {
                        "step": "S",
                        "class": "SampleStep",
                        "arguments": {"memo": "v"},
                    }
                ]
            },
        )
        # RECIPE_DIRS undefined: builder should construct fine and run
        # a scenario with no recipe directives.
        patched_env(None)

        builder = _ScenarioBuilder(scenario_file=str(scenario_path), file_format="yaml")
        steps = builder.execute()
        assert len(steps) == 1

    def test_unset_recipe_dirs_errors_on_recipe_directive(self, tmp_path, patched_env):
        from cliboa.util.exception import CliboaRuntimeError

        scenario_path = tmp_path / "scenario.yml"
        self._write_yaml(
            str(scenario_path),
            {"scenario": [{"recipe": "any_recipe"}]},
        )
        patched_env(None)

        builder = _ScenarioBuilder(scenario_file=str(scenario_path), file_format="yaml")
        with pytest.raises(CliboaRuntimeError, match="RECIPE_DIRS is empty"):
            builder.execute()

    def test_non_existent_recipe_dir_errors_at_init(self, tmp_path, patched_env):
        from cliboa.util.exception import CliboaRuntimeError

        scenario_path = tmp_path / "scenario.yml"
        self._write_yaml(
            str(scenario_path),
            {"scenario": [{"step": "S", "class": "SampleStep", "arguments": {"memo": "v"}}]},
        )
        bogus = str(tmp_path / "no_such_dir")
        patched_env([bogus])

        with pytest.raises(CliboaRuntimeError, match="non-existent directory"):
            _ScenarioBuilder(scenario_file=str(scenario_path), file_format="yaml")

    def test_common_with_recipe_directive_errors(self, tmp_path, patched_env):
        from cliboa.util.exception import ScenarioFileInvalid

        recipe_dir = tmp_path / "recipe"
        scenario_path = tmp_path / "scenario.yml"
        common_path = tmp_path / "common.yml"
        recipe_path = recipe_dir / "x.yml"

        self._write_yaml(
            str(scenario_path),
            {"scenario": [{"step": "S", "class": "SampleStep", "arguments": {"memo": "v"}}]},
        )
        self._write_yaml(
            str(common_path),
            {"scenario": [{"recipe": "x"}]},
        )
        self._write_yaml(
            str(recipe_path),
            {"recipe": [{"step": "X", "class": "SampleStep"}]},
        )
        patched_env([str(recipe_dir)])

        builder = _ScenarioBuilder(
            scenario_file=str(scenario_path),
            common_file=str(common_path),
            file_format="yaml",
        )
        with pytest.raises(ScenarioFileInvalid, match="must contain only plain steps"):
            builder.execute()

import logging
from typing import Any, Dict, Type
from unittest.mock import ANY, MagicMock, Mock

import pytest

from cliboa.core.builder import _ScenarioBuilder
from cliboa.core.model import StepModel


class _IExecute(object):
    """Dummy interface for type checking."""

    pass


class MockStepExecutor(_IExecute):
    """Mock implementation of _StepExecutor."""

    def __init__(self, instance: Any, step: StepModel, cmd_arg: Any, *args, **kwargs):
        self.instance = instance
        self.step = step
        self.cmd_arg = cmd_arg
        self.register_listener = MagicMock()


class MockParallelProcessor(_IExecute):
    """Mock implementation of _ParallelProcessor."""

    def __init__(self, instances: list, parallel_config: Any, *args, **kwargs):
        self.instances = instances
        self.parallel_config = parallel_config


class MockStepStatusListener:
    """Mock implementation of StepStatusListener."""

    pass


class MockStepArgument:
    """
    Mock class for StepArgument.
    """

    _put = MagicMock()

    @classmethod
    def reset_mock(cls):
        """Helper to reset the class method mock for isolation."""
        cls._put.reset_mock()


class MockFactory:
    """
    Mock factory that asserts 'create' is called with a string.
    """

    def create(self, class_name: Any) -> Mock:
        """
        Mock create method.
        Requirement: Fail if argument is not a string.
        """
        if not isinstance(class_name, str):
            raise TypeError(f"Factory received non-string argument: {type(class_name)}")

        mock_instance = Mock()
        mock_instance.class_name = class_name
        return mock_instance


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

    @pytest.fixture
    def mock_factory(self) -> MagicMock:
        """
        Fixture for the custom MockFactory.
        """
        factory_instance = MockFactory()
        factory_instance.create = MagicMock(side_effect=factory_instance.create)
        return factory_instance

    @pytest.fixture
    def mock_step_argument_cls(self) -> Type[MockStepArgument]:
        """
        Fixture to reset the mock step argument class for test isolation.
        """
        MockStepArgument.reset_mock()
        return MockStepArgument

    def test_execute_simple_steps(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 2
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 2
        mock_factory.create.assert_any_call("DummyStep1")
        mock_factory.create.assert_any_call("DummyStep2")

        assert mock_step_argument_cls._put.call_count == 2

        assert steps[0].register_listener.call_count == 1
        assert steps[1].register_listener.call_count == 1

        assert steps[0].step.step == "Step1"
        assert steps[1].step.step == "Step2"

    def test_execute_parallel_steps(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockParallelProcessor)

        assert mock_factory.create.call_count == 2
        assert mock_step_argument_cls._put.call_count == 2

        parallel_processor = steps[0]
        assert len(parallel_processor.instances) == 2

        assert parallel_processor.instances[0].register_listener.call_count == 1
        assert parallel_processor.instances[1].register_listener.call_count == 1

    def test_execute_mixed_steps(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
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
        assert mock_step_argument_cls._put.call_count == 3

        assert steps[0].register_listener.call_count == 1
        assert steps[1].instances[0].register_listener.call_count == 1
        assert steps[2].register_listener.call_count == 1

    def test_execute_merge_common_arguments(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
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
        mock_step_argument_cls: Type[MockStepArgument],
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
            di_step_argument=mock_step_argument_cls,
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

    def test_execute_with_single_listener(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 2
        mock_factory.create.assert_any_call("DummyStep1")
        mock_factory.create.assert_any_call("DummyListener")

        assert steps[0].register_listener.call_count == 2

        assert mock_step_argument_cls._put.call_count == 1

    def test_execute_with_multiple_listeners(
        self, mock_factory: MagicMock, mock_step_argument_cls: Type[MockStepArgument]
    ):
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
            di_step_argument=mock_step_argument_cls,
            di_factory=mock_factory,
            di_step_executor=MockStepExecutor,
            di_parallel_processor=MockParallelProcessor,
            di_step_status_listener=MockStepStatusListener(),
        )

        steps = builder.execute()

        assert len(steps) == 1
        assert isinstance(steps[0], MockStepExecutor)

        assert mock_factory.create.call_count == 3

        mock_factory.create.assert_any_call("DummyStep1")
        mock_factory.create.assert_any_call("DummyListener1")
        mock_factory.create.assert_any_call("DummyListener2")

        assert mock_step_argument_cls._put.call_count == 1
        mock_step_argument_cls._put.assert_any_call("Step1", ANY)

        assert steps[0].register_listener.call_count == 3

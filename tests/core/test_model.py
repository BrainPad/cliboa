from datetime import date

import pytest
from pydantic import ValidationError

from cliboa.core.model import (
    ParallelConfigModel,
    ParallelStepModel,
    ScenarioModel,
    StepModel,
    _BaseWithVars,
)
from cliboa.util.exception import InvalidFormat, InvalidParameter, ScenarioFileInvalid


class TestBaseWithVars:
    """
    Tests for _BaseWithVars model
    """

    def test_calc_ok(self):
        # Test calculation of with_vars using date command
        today_str = date.today().strftime("%Y%m%d")
        model = _BaseWithVars(with_vars={"today": 'date +"%Y%m%d"'})
        model.calc()
        assert model._with_static_vars == {"today": today_str}

    def test_merge_static_vars_ok(self):
        # Test merging static vars
        model = _BaseWithVars()
        model._with_static_vars = {"a": "1", "b": "2"}
        model._merge_static_vars({"b": "override", "c": "3"})
        # Existing keys in _with_static_vars should take precedence
        assert model._with_static_vars == {"b": "2", "c": "3", "a": "1"}


class TestStepModel:
    """
    Tests for StepModel
    """

    def test_init_with_vars_extraction_ok(self):
        # Test if with_vars is correctly extracted from arguments
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {"arg1": "val1", "with_vars": {"dyn_var": "echo hello"}},
        }
        model = StepModel.model_validate(data)
        assert model.with_vars == {"dyn_var": "echo hello"}
        assert "with_vars" not in model.arguments

    def test_init_with_vars_duplicate_ng(self):
        # Test for duplicate definition of with_vars
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "with_vars": {"top_var": "echo top"},
            "arguments": {"arg1": "val1", "with_vars": {"dyn_var": "echo hello"}},
        }
        # do not wrap custom exceptions in ValidationError.
        with pytest.raises(InvalidFormat) as exc_info:
            StepModel.model_validate(data)
        assert "duplicate definition 'with_vars'" in str(exc_info.value)

    def test_replace_vars_static_ok(self):
        # Test replacing variables from _with_static_vars
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {"greeting": "Hello, {{ name }}"},
        }
        model = StepModel.model_validate(data)
        model._with_static_vars = {"name": "World"}
        model.replace_vars()
        assert model.arguments == {"greeting": "Hello, World"}

    def test_replace_vars_env_ok(self, monkeypatch):
        # Test replacing variables from environment variables
        monkeypatch.setenv("USER_NAME", "Cliboa")
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {"user": "User is {{ env.USER_NAME }}"},
        }
        model = StepModel.model_validate(data)
        model.replace_vars()
        assert model.arguments == {"user": "User is Cliboa"}

    def test_replace_vars_nested_ok(self, monkeypatch):
        # Test replacing variables in nested structures (dict and list)
        monkeypatch.setenv("ENV_VAL", "Production")
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {
                "config": {
                    "env": "{{ env.ENV_VAL }}",
                    "user": "{{ user_id }}",
                },
                "targets": ["target-{{ user_id }}", "target-{{ env.ENV_VAL }}"],
            },
        }
        model = StepModel.model_validate(data)
        model._with_static_vars = {"user_id": "123"}
        model.replace_vars()
        expected_args = {
            "config": {
                "env": "Production",
                "user": "123",
            },
            "targets": ["target-123", "target-Production"],
        }
        assert model.arguments == expected_args

    def test_replace_vars_empty_ng(self):
        # Test error handling for empty variable expression {{ }}
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {"arg": "Value is {{ }}"},
        }
        model = StepModel.model_validate(data)
        with pytest.raises(InvalidParameter) as exc_info:
            model.replace_vars()
        assert "name in variable expression was empty" in str(exc_info.value)

    def test_replace_vars_unresolved_ng(self):
        # Test error handling for unresolved variable
        data = {
            "step": "TestStep",
            "class": "TestClass",
            "arguments": {"arg": "Value is {{ unknown_var }}"},
        }
        model = StepModel.model_validate(data)
        with pytest.raises(ScenarioFileInvalid) as exc_info:
            model.replace_vars()
        assert "variable 'unknown_var' can not be resolved" in str(exc_info.value)


class TestParallelConfigModel:
    """
    Tests for ParallelConfigModel
    """

    def test_merge_ok(self):
        # Test merging two ParallelConfigModel instances
        config1 = ParallelConfigModel(multi_process_count=None, force_continue=True)
        config2 = ParallelConfigModel(multi_process_count=4, force_continue=False)

        config1.merge(config2)

        # config1.multi_process_count was None, so it takes value from config2
        assert config1.multi_process_count == 4
        # config1.force_continue was not None, so it keeps its value
        assert config1.force_continue is True

    def test_merge_all_none_ok(self):
        # Test merging when target has no values
        config1 = ParallelConfigModel(multi_process_count=None, force_continue=None)
        config2 = ParallelConfigModel(multi_process_count=4, force_continue=False)

        config1.merge(config2)

        assert config1.multi_process_count == 4
        assert config1.force_continue is False

    def test_merge_source_none_ok(self):
        # Test merging when source has None values
        config1 = ParallelConfigModel(multi_process_count=2, force_continue=True)
        config2 = ParallelConfigModel(multi_process_count=None, force_continue=None)

        config1.merge(config2)

        # Values in config1 should remain unchanged
        assert config1.multi_process_count == 2
        assert config1.force_continue is True

    def test_validation_multi_process_count_ng(self):
        # Test validation failure for multi_process_count < 2
        with pytest.raises(ValidationError) as exc_info:
            ParallelConfigModel(multi_process_count=1)
        assert "multi_process_count" in str(exc_info.value)

    def test_validation_multi_process_count_ok(self):
        # Test valid multi_process_count values
        model_ok = ParallelConfigModel(multi_process_count=2)
        assert model_ok.multi_process_count == 2

        model_none = ParallelConfigModel(multi_process_count=None)
        assert model_none.multi_process_count is None


class TestParallelStepModel:
    """
    Tests for ParallelStepModel
    """

    def test_merge_parallel_config_ok(self):
        # Test merging parallel_config when self.parallel_config is None
        step_data = {
            "parallel": [
                {"step": "p1", "class": "ClassA"},
            ]
        }
        model = ParallelStepModel.model_validate(step_data)
        config = ParallelConfigModel(multi_process_count=4)
        model._merge_parallel_config(config)
        assert model.parallel_config == config

    def test_merge_parallel_config_existing_ok(self):
        # Test merging parallel_config when self.parallel_config already exists
        step_data = {
            "parallel": [
                {"step": "p1", "class": "ClassA"},
            ],
            "parallel_config": {
                "multi_process_count": None,
                "force_continue": True,
            },
        }
        model = ParallelStepModel.model_validate(step_data)

        config_to_merge = ParallelConfigModel(multi_process_count=4, force_continue=False)
        model._merge_parallel_config(config_to_merge)

        # Check if merge logic was applied correctly
        assert model.parallel_config.multi_process_count == 4
        assert model.parallel_config.force_continue is True  # Original value is kept

    def test_validation_parallel_empty_ng(self):
        # Test validation failure for empty parallel list
        with pytest.raises(ValidationError) as exc_info:
            ParallelStepModel(parallel=[])
        assert "parallel" in str(exc_info.value)
        assert "at least 1 item" in str(exc_info.value)


class TestScenarioModel:
    """
    Tests for ScenarioModel
    """

    def test_validation_scenario_empty_ng(self):
        # Test validation failure for empty scenario list
        with pytest.raises(ValidationError) as exc_info:
            ScenarioModel(scenario=[])
        assert "scenario" in str(exc_info.value)
        assert "at least 1 item" in str(exc_info.value)

    def test_merge_ok(self):
        # Test merging common scenario settings
        scenario_data = {
            "scenario": [
                {"step": "s1", "class": "ClassA", "arguments": {"arg1": "step_val"}},
                {"step": "s2", "class": "ClassB"},
            ],
            "with_vars": {"scenario_var": "val1"},
            "parallel_config": {"force_continue": True},
        }
        common_data = {
            "scenario": [
                {
                    "step": "c1",
                    "class": "ClassA",
                    "arguments": {"arg1": "cmn_val", "arg2": "cmn_val2"},
                },
            ],
            "with_vars": {"common_var": "val2", "scenario_var": "val_cmn"},
            "parallel_config": {"multi_process_count": 4, "force_continue": None},
        }

        model = ScenarioModel.model_validate(scenario_data)
        common = ScenarioModel.model_validate(common_data)

        model.merge(common)

        # Test parallel_config merge
        assert model.parallel_config.force_continue is True  # Original value
        assert model.parallel_config.multi_process_count == 4  # Merged value

        # Test with_vars merge (scenario's 'scenario_var' overrides common's)
        assert model.with_vars == {"common_var": "val2", "scenario_var": "val1"}

        # Test step arguments merge (step's 'arg1' overrides common's)
        step1 = model.scenario[0]
        step2 = model.scenario[1]
        assert isinstance(step1, StepModel)
        assert step1.arguments == {"arg1": "step_val", "arg2": "cmn_val2"}
        assert step2.arguments == {}

    def test_setup_ok(self):
        # Test the full setup process
        today_str = date.today().strftime("%Y%m%d")
        scenario_data = {
            "scenario": [
                {
                    "step": "s1",
                    "class": "ClassA",
                    "arguments": {"date_arg": "Today is {{ today }}"},
                },
                {
                    "parallel": [
                        {
                            "step": "p1",
                            "class": "ClassB",
                            "arguments": {"parallel_date": "Date: {{ today }}"},
                        }
                    ]
                },
            ],
            "with_vars": {"today": 'date +"%Y%m%d"'},
        }

        model = ScenarioModel.model_validate(scenario_data)
        model.setup()

        # Check if top-level with_vars was calculated
        assert model._with_static_vars == {"today": today_str}

        # Check if variables were replaced in StepModel
        step1 = model.scenario[0]
        assert isinstance(step1, StepModel)
        assert step1.arguments == {"date_arg": f"Today is {today_str}"}

        # Check if variables were propagated and replaced in ParallelStepModel's StepModel
        parallel_step = model.scenario[1]
        assert isinstance(parallel_step, ParallelStepModel)
        p_step1 = parallel_step.parallel[0]
        assert p_step1._with_static_vars["today"] == today_str
        assert p_step1.arguments == {"parallel_date": f"Date: {today_str}"}

    def test_setup_with_parallel_config_propagation_ok(self):
        # Test setup with parallel_config propagation
        scenario_data = {
            "scenario": [
                {
                    "parallel": [
                        {"step": "p1", "class": "ClassA"},
                    ],
                    "parallel_config": {"force_continue": True},  # Step-level config
                },
                {
                    "parallel": [
                        {"step": "p2", "class": "ClassB"},
                    ]
                    # No config here, should inherit from top
                },
            ],
            "parallel_config": {"multi_process_count": 4},  # Top-level config
        }

        model = ScenarioModel.model_validate(scenario_data)
        model.setup()

        # Parallel step 1 should merge top-level config
        p_step1 = model.scenario[0]
        assert isinstance(p_step1, ParallelStepModel)
        assert p_step1.parallel_config.multi_process_count == 4
        assert p_step1.parallel_config.force_continue is True  # Keeps its own

        # Parallel step 2 should inherit top-level config
        p_step2 = model.scenario[1]
        assert isinstance(p_step2, ParallelStepModel)
        assert p_step2.parallel_config.multi_process_count == 4
        assert p_step2.parallel_config.force_continue is None

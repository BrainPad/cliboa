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
import logging
import os

import pytest
import yaml
from pydantic import ValidationError

from cliboa.core.loader import _ScenarioFormat
from cliboa.core.model import (
    RecipeModel,
    RecipeParameterSpec,
    RecipeStepModel,
    ScenarioModel,
    StepModel,
)
from cliboa.core.recipe import _RecipeExpander
from cliboa.util.exception import ScenarioFileInvalid


def _write_recipe(dir_path: str, rel_path: str, content: dict) -> str:
    """
    Write a recipe YAML file under dir_path / rel_path.yml and return the full path.
    """
    full = os.path.join(dir_path, rel_path + ".yml")
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        yaml.safe_dump(content, f, sort_keys=False)
    return full


def _make_expander(recipe_dirs: list[str]) -> _RecipeExpander:
    return _RecipeExpander(recipe_dirs, _ScenarioFormat.YAML)


def _expand(expander: _RecipeExpander, scenario_dict: dict) -> ScenarioModel:
    """
    Validate the raw scenario dict and run the expander on the resulting model.
    """
    scenario = ScenarioModel.model_validate(scenario_dict)
    return expander.expand(scenario)


class TestRecipeParameterSpec:
    def test_description_is_required(self):
        with pytest.raises(ValidationError):
            RecipeParameterSpec()

    def test_with_description_only_is_required_parameter(self):
        spec = RecipeParameterSpec(description="desc")
        assert spec.description == "desc"
        assert spec.default is None
        assert spec.is_required is True

    def test_with_default(self):
        spec = RecipeParameterSpec(description="d", default="/tmp")
        assert spec.is_required is False
        assert spec.default == "/tmp"

    def test_explicit_none_default_is_required(self):
        # Explicit `default: null` is treated identically to omitting the key:
        # both mean "no default" and therefore the parameter is required.
        spec = RecipeParameterSpec(description="d", default=None)
        assert spec.is_required is True

    def test_empty_string_default_is_optional(self):
        # "" is a valid (non-None) string default; the parameter is optional.
        assert RecipeParameterSpec(description="d", default="").is_required is False

    def test_numeric_default_is_coerced_to_string(self):
        # coerce_numbers_to_str=True makes int/float defaults usable in YAML.
        spec = RecipeParameterSpec(description="d", default=30)
        assert spec.default == "30"
        assert spec.is_required is False

    def test_non_string_non_numeric_default_is_rejected(self):
        # bool, list, dict are not coerced to str; they fail validation.
        with pytest.raises(ValidationError):
            RecipeParameterSpec(description="d", default=True)
        with pytest.raises(ValidationError):
            RecipeParameterSpec(description="d", default=[1, 2])
        with pytest.raises(ValidationError):
            RecipeParameterSpec(description="d", default={"k": "v"})


class TestRecipeModel:
    def _minimal_recipe(self) -> list[dict]:
        return [{"step": "Foo", "class": "FooStep"}]

    def test_string_shorthand_becomes_description(self):
        m = RecipeModel.model_validate(
            {"parameters": {"src": "desc here"}, "recipe": self._minimal_recipe()}
        )
        assert m.parameters["src"].description == "desc here"
        assert m.parameters["src"].is_required is True

    def test_null_parameter_value_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {"parameters": {"verbose": None}, "recipe": self._minimal_recipe()}
            )

    def test_empty_dict_parameter_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {"parameters": {"verbose": {}}, "recipe": self._minimal_recipe()}
            )

    def test_dict_without_description_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {
                    "parameters": {"foo": {"default": "/tmp"}},
                    "recipe": self._minimal_recipe(),
                }
            )

    def test_full_dict_form(self):
        m = RecipeModel.model_validate(
            {
                "parameters": {
                    "dest": {"description": "where", "default": "/tmp"},
                },
                "recipe": self._minimal_recipe(),
            }
        )
        assert m.parameters["dest"].description == "where"
        assert m.parameters["dest"].default == "/tmp"
        assert m.parameters["dest"].is_required is False

    def test_int_value_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {"parameters": {"foo": 42}, "recipe": self._minimal_recipe()}
            )

    def test_bool_value_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {"parameters": {"foo": True}, "recipe": self._minimal_recipe()}
            )

    def test_list_value_is_rejected(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate(
                {"parameters": {"foo": [1, 2]}, "recipe": self._minimal_recipe()}
            )

    def test_unknown_keys_in_parameter_spec_are_ignored(self):
        # extra="ignore" is the cliboa-wide default; unknown keys are dropped.
        m = RecipeModel.model_validate(
            {
                "parameters": {
                    "foo": {"description": "d", "required": True, "type": "str"},
                },
                "recipe": self._minimal_recipe(),
            }
        )
        assert m.parameters["foo"].description == "d"
        assert m.parameters["foo"].is_required is True  # 'required' key was ignored

    def test_recipe_is_required(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate({"parameters": {}})

    def test_recipe_min_length_one(self):
        with pytest.raises(ValidationError):
            RecipeModel.model_validate({"recipe": []})

    def test_recipe_step_must_be_valid_step_model_shape(self):
        # recipe entries are typed as StepModel, so a step missing 'class'
        # (required by StepModel) is rejected at recipe load time.
        with pytest.raises(ValidationError):
            RecipeModel.model_validate({"recipe": [{"step": "Foo"}]})

    def test_recipe_step_validated_as_step_model_instance(self):
        m = RecipeModel.model_validate({"recipe": [{"step": "Foo", "class": "Bar"}]})
        assert isinstance(m.recipe[0], StepModel)
        assert m.recipe[0].step == "Foo"
        assert m.recipe[0].class_name == "Bar"


class TestRecipeStepModel:
    """
    Tests for RecipeStepModel itself and its dispatch from ScenarioModel.
    """

    def test_recipe_field_required(self):
        with pytest.raises(ValidationError):
            RecipeStepModel()

    def test_defaults(self):
        m = RecipeStepModel(recipe="foo")
        assert m.recipe == "foo"
        assert m.arguments == {}

    def test_with_arguments(self):
        m = RecipeStepModel(recipe="foo", arguments={"k": "v"})
        assert m.arguments == {"k": "v"}

    def test_numeric_argument_coerced_to_string(self):
        # coerce_numbers_to_str=True lets callers write `timeout: 30` in YAML.
        m = RecipeStepModel(recipe="foo", arguments={"timeout": 30, "ratio": 1.5})
        assert m.arguments == {"timeout": "30", "ratio": "1.5"}

    def test_non_string_non_numeric_argument_rejected(self):
        # bool / list / dict are not coerced to str and fail validation.
        with pytest.raises(ValidationError):
            RecipeStepModel(recipe="foo", arguments={"enabled": True})
        with pytest.raises(ValidationError):
            RecipeStepModel(recipe="foo", arguments={"items": [1, 2]})

    def test_dispatched_as_recipe_in_scenario(self):
        s = ScenarioModel.model_validate({"scenario": [{"recipe": "foo", "arguments": {"k": "v"}}]})
        assert isinstance(s.scenario[0], RecipeStepModel)
        assert s.scenario[0].recipe == "foo"

    def test_dispatched_as_step_when_no_recipe(self):
        s = ScenarioModel.model_validate({"scenario": [{"step": "X", "class": "Y"}]})
        assert isinstance(s.scenario[0], StepModel)

    def test_non_dict_arguments_rejected_at_validation(self):
        # RecipeStepModel.arguments is dict[str, str]; a bare string is rejected.
        with pytest.raises(ValidationError):
            ScenarioModel.model_validate({"scenario": [{"recipe": "x", "arguments": "not a dict"}]})


class TestIncludePathValidation:
    def test_strip_whitespace(self, tmp_path):
        _write_recipe(str(tmp_path), "foo", {"recipe": [{"step": "S", "class": "X"}]})
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "  foo  "}]},
        )
        assert len(result.scenario) == 1

    def test_leading_slash_stripped(self, tmp_path):
        _write_recipe(str(tmp_path), "foo", {"recipe": [{"step": "S", "class": "X"}]})
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "/foo"}]},
        )
        assert len(result.scenario) == 1

    def test_dotdot_rejected(self, tmp_path):
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match=r"must not contain '\.\.'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "../etc/passwd"}]},
            )

    def test_dotdot_in_middle_rejected(self, tmp_path):
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match=r"must not contain '\.\.'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "foo/../bar"}]},
            )

    def test_empty_path_rejected(self, tmp_path):
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="empty"):
            _expand(
                expander,
                {"scenario": [{"recipe": "/"}]},
            )


class TestRecipePathResolution:
    def test_empty_recipe_dirs_disables_feature(self):
        expander = _make_expander([])
        result = _expand(
            expander,
            {"scenario": [{"step": "S", "class": "X"}]},
        )
        assert result.scenario[0].class_name == "X"

    def test_empty_recipe_dirs_errors_when_recipe_directive_used(self):
        from cliboa.util.exception import CliboaRuntimeError

        expander = _make_expander([])
        with pytest.raises(CliboaRuntimeError, match="RECIPE_DIRS is empty or undefined"):
            _expand(
                expander,
                {"scenario": [{"recipe": "foo"}]},
            )

    def test_not_found_in_any_dir(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        expander = _make_expander([str(d1), str(d2)])
        with pytest.raises(ScenarioFileInvalid, match="not found"):
            _expand(
                expander,
                {"scenario": [{"recipe": "missing"}]},
            )

    def test_first_dir_wins(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        _write_recipe(str(d1), "shared", {"recipe": [{"step": "FromD1", "class": "X"}]})
        _write_recipe(str(d2), "shared", {"recipe": [{"step": "FromD2", "class": "X"}]})
        expander = _make_expander([str(d1), str(d2)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "shared"}]},
        )
        assert result.scenario[0].step == "FromD1"

    def test_falls_through_to_next_dir(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        _write_recipe(str(d2), "only_d2", {"recipe": [{"step": "FromD2", "class": "X"}]})
        expander = _make_expander([str(d1), str(d2)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "only_d2"}]},
        )
        assert result.scenario[0].step == "FromD2"

    def test_nested_subdir_path(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "sftp/download",
            {"recipe": [{"step": "Dl", "class": "X"}]},
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "sftp/download"}]},
        )
        assert result.scenario[0].step == "Dl"


class TestBasicExpansion:
    def test_simple_recipe_splices_steps(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "two_steps",
            {
                "recipe": [
                    {"step": "First", "class": "FooStep"},
                    {"step": "Second", "class": "BarStep"},
                ],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {
                "scenario": [
                    {"step": "Before", "class": "PreStep"},
                    {"recipe": "two_steps"},
                    {"step": "After", "class": "PostStep"},
                ],
            },
        )
        assert [s.step for s in result.scenario] == [
            "Before",
            "First",
            "Second",
            "After",
        ]
        assert all(isinstance(s, StepModel) for s in result.scenario)

    def test_step_without_recipe_directive_passes_through(self, tmp_path):
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"step": "Plain", "class": "X"}]},
        )
        assert isinstance(result.scenario[0], StepModel)
        assert result.scenario[0].step == "Plain"


class TestArgumentResolution:
    def test_required_parameter_missing(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "needs_arg",
            {
                "parameters": {"src": "where to read"},
                "recipe": [{"step": "S", "class": "X"}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="required parameter 'src'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "needs_arg"}]},
            )

    def test_extra_argument_rejected(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "one_arg",
            {
                "parameters": {"a": "desc"},
                "recipe": [{"step": "S", "class": "X", "arguments": {"x": "{{ args.a }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="extra argument 'b'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "one_arg", "arguments": {"a": "v", "b": "v2"}}]},
            )

    def test_default_value_used_when_omitted(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "with_default",
            {
                "parameters": {
                    "dest": {"description": "d", "default": "/tmp/default"},
                },
                "recipe": [{"step": "S", "class": "X", "arguments": {"out": "{{ args.dest }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "with_default"}]},
        )
        assert result.scenario[0].arguments["out"] == "/tmp/default"

    def test_provided_value_overrides_default(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "with_default",
            {
                "parameters": {
                    "dest": {"description": "d", "default": "/tmp/default"},
                },
                "recipe": [{"step": "S", "class": "X", "arguments": {"out": "{{ args.dest }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "with_default", "arguments": {"dest": "/custom"}}]},
        )
        assert result.scenario[0].arguments["out"] == "/custom"

    def test_required_parameter_without_default(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "must_provide",
            {
                "parameters": {"verbose": {"description": "verbosity flag"}},
                "recipe": [{"step": "S", "class": "X"}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="required parameter 'verbose'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "must_provide"}]},
            )

    def test_explicit_null_default_is_required(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "null_default",
            {
                "parameters": {
                    "opt": {"description": "d", "default": None},
                },
                "recipe": [{"step": "S", "class": "X"}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="required parameter 'opt'"):
            _expand(
                expander,
                {"scenario": [{"recipe": "null_default"}]},
            )


class TestArgsSubstitution:
    def _write_echo_recipe(self, tmp_path, body_value: str = "{{ args.x }}") -> None:
        _write_recipe(
            str(tmp_path),
            "echo",
            {
                "parameters": {"x": "param"},
                "recipe": [{"step": "S", "class": "C", "arguments": {"val": body_value}}],
            },
        )

    def test_substitution_basic(self, tmp_path):
        self._write_echo_recipe(tmp_path)
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "echo", "arguments": {"x": "hello"}}]},
        )
        assert result.scenario[0].arguments["val"] == "hello"

    def test_substitution_without_spaces(self, tmp_path):
        self._write_echo_recipe(tmp_path, body_value="{{args.x}}")
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "echo", "arguments": {"x": "hi"}}]},
        )
        assert result.scenario[0].arguments["val"] == "hi"

    def test_substitution_multiple_in_one_string(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "two_refs",
            {
                "parameters": {"a": "param a", "b": "param b"},
                "recipe": [
                    {
                        "step": "S",
                        "class": "C",
                        "arguments": {"val": "{{ args.a }}-{{ args.b }}"},
                    }
                ],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "two_refs", "arguments": {"a": "foo", "b": "bar"}}]},
        )
        assert result.scenario[0].arguments["val"] == "foo-bar"

    def test_substitution_in_nested_structures(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "nested",
            {
                "parameters": {"x": "param"},
                "recipe": [
                    {
                        "step": "S",
                        "class": "C",
                        "arguments": {
                            "outer": {
                                "inner_list": ["{{ args.x }}", "static"],
                                "inner_dict": {"k": "{{ args.x }}"},
                            }
                        },
                    }
                ],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "nested", "arguments": {"x": "VAL"}}]},
        )
        a = result.scenario[0].arguments
        assert a["outer"]["inner_list"] == ["VAL", "static"]
        assert a["outer"]["inner_dict"]["k"] == "VAL"

    def test_with_vars_is_not_touched_by_args_substitution(self, tmp_path):
        # ``with_vars`` values are shell commands and are passed to the shell
        # as-is at phase 2; the expander does not substitute ``args.x`` there.
        _write_recipe(
            str(tmp_path),
            "with_vars_ref",
            {
                "parameters": {"name": "param"},
                "recipe": [
                    {
                        "step": "S",
                        "class": "C",
                        "with_vars": {"hello": "echo {{ args.name }}"},
                    }
                ],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "with_vars_ref", "arguments": {"name": "world"}}]},
        )
        assert result.scenario[0].with_vars["hello"] == "echo {{ args.name }}"

    def test_non_args_reference_preserved_for_phase2(self, tmp_path):
        # Non-args references in a recipe are left alone at expand time and
        # resolved during the standard phase-2 substitution.
        _write_recipe(
            str(tmp_path),
            "with_common_ref",
            {
                "parameters": {"x": "param"},
                "recipe": [{"step": "S", "class": "C", "arguments": {"val": "{{ common_var }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "with_common_ref", "arguments": {"x": "v"}}]},
        )
        # `{{ common_var }}` is preserved as-is until phase 2 resolves it.
        assert result.scenario[0].arguments["val"] == "{{ common_var }}"

    def test_undeclared_args_reference_rejected(self, tmp_path):
        # Undeclared `args.X` reference: phase-1 substitution attempts to
        # resolve it via the injected static vars, fails, and raises the
        # standard "can not be resolved" error.
        _write_recipe(
            str(tmp_path),
            "typo",
            {
                "parameters": {"src": "param"},
                "recipe": [{"step": "S", "class": "C", "arguments": {"val": "{{ args.scr }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="'args.scr' can not be resolved"):
            _expand(
                expander,
                {"scenario": [{"recipe": "typo", "arguments": {"src": "v"}}]},
            )

    def test_empty_expression_rejected(self, tmp_path):
        # `apply_args` raises ``InvalidParameter`` for an empty ``{{ }}`` token;
        # `_expand_recipe` catches it and re-raises as ``ScenarioFileInvalid``
        # with the recipe path prepended.
        _write_recipe(
            str(tmp_path),
            "empty",
            {
                "parameters": {},
                "recipe": [{"step": "S", "class": "C", "arguments": {"val": "{{  }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ScenarioFileInvalid, match="empty"):
            _expand(
                expander,
                {"scenario": [{"recipe": "empty"}]},
            )

    def test_caller_value_with_template_preserved_for_phase2(self, tmp_path):
        # Caller's argument value contains {{ common_var }}.
        # Phase 1 substitutes {{ args.src }} with the literal string and
        # the embedded {{ common_var }} is preserved for phase 2.
        _write_recipe(
            str(tmp_path),
            "passthrough",
            {
                "parameters": {"src": "param"},
                "recipe": [{"step": "S", "class": "C", "arguments": {"val": "{{ args.src }}"}}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        result = _expand(
            expander,
            {"scenario": [{"recipe": "passthrough", "arguments": {"src": "{{ base_dir }}/foo"}}]},
        )
        assert result.scenario[0].arguments["val"] == "{{ base_dir }}/foo"


class TestRecipeStructure:
    def test_scenario_top_level_rejected(self, tmp_path):
        # Recipe must use 'recipe:', not 'scenario:'. The 'scenario:' key is
        # an unknown field that is dropped, then pydantic complains that the
        # required 'recipe' field is missing.
        _write_recipe(
            str(tmp_path),
            "wrong",
            {"scenario": [{"step": "S", "class": "X"}]},
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ValidationError):
            _expand(
                expander,
                {"scenario": [{"recipe": "wrong"}]},
            )

    def test_missing_recipe_key_rejected(self, tmp_path):
        _write_recipe(
            str(tmp_path),
            "no_recipe",
            {"parameters": {"x": "d"}},
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ValidationError):
            _expand(
                expander,
                {"scenario": [{"recipe": "no_recipe", "arguments": {"x": "v"}}]},
            )

    def test_nested_recipe_rejected(self, tmp_path):
        # ``RecipeModel.recipe`` is typed as ``list[StepModel, ...]`` so an
        # entry shaped as ``{recipe: ...}`` fails StepModel validation.
        _write_recipe(
            str(tmp_path),
            "outer",
            {
                "parameters": {},
                "recipe": [{"recipe": "inner"}],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ValidationError):
            _expand(
                expander,
                {"scenario": [{"recipe": "outer"}]},
            )

    def test_recipe_with_parallel_rejected(self, tmp_path):
        # `parallel:` is type-level forbidden in recipes (RecipeModel.recipe
        # is list[StepModel, ...]). pydantic raises ValidationError at recipe
        # load time when the recipe contains a `parallel:` block.
        _write_recipe(
            str(tmp_path),
            "with_par",
            {
                "parameters": {},
                "recipe": [
                    {
                        "parallel": [
                            {"step": "A", "class": "X"},
                            {"step": "B", "class": "Y"},
                        ]
                    }
                ],
            },
        )
        expander = _make_expander([str(tmp_path)])
        with pytest.raises(ValidationError):
            _expand(
                expander,
                {"scenario": [{"recipe": "with_par"}]},
            )


class TestRecipeDirectiveInsideParallelRejected:
    """
    ``parallel:`` blocks may not contain ``recipe:`` (type-level enforcement).
    """

    def test_recipe_directive_inside_parallel_rejected_at_validation(self):
        # ParallelStepModel.parallel is typed as Tuple[StepModel, ...] so a
        # RecipeStepModel inside a parallel block fails pydantic validation.
        with pytest.raises(ValidationError):
            ScenarioModel.model_validate({"scenario": [{"parallel": [{"recipe": "x"}]}]})


class TestLogging:
    def test_logs_resolved_recipe_path(self, tmp_path, caplog):
        _write_recipe(str(tmp_path), "logged", {"recipe": [{"step": "S", "class": "X"}]})
        expander = _make_expander([str(tmp_path)])
        with caplog.at_level(logging.INFO):
            _expand(
                expander,
                {"scenario": [{"recipe": "logged"}]},
            )
        assert any("logged" in r.message for r in caplog.records)

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
import re
import subprocess
from typing import Any, Tuple

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

# 'typing.Self' is available in Python 3.11+
from typing_extensions import Self

from cliboa.util.base import _warn_deprecated
from cliboa.util.exception import InvalidFormat, InvalidParameter, ScenarioFileInvalid


class _BaseWithVars(BaseModel):
    with_vars: dict[str, str] = Field(default_factory=dict)
    _with_static_vars: dict[str, str] = PrivateAttr(default_factory=dict)

    def calc(self) -> None:
        """
        calculate with_vars shell commands, and store result.
        """
        for var_name, cmd in self.with_vars.items():
            self._with_static_vars[var_name] = self._exec_shell_cmd(cmd)

    def _exec_shell_cmd(self, cmd: str):
        """
        exec a shell script and return result string to replace as variable.
        """
        shell_output = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, shell=True
        ).communicate()[  # nosec
            0
        ]
        shell_output = shell_output.strip()
        # remove head byte string
        shell_output = re.sub("^b", "", str(shell_output))
        # remove '
        shell_output = re.sub("'", "", str(shell_output))
        return shell_output

    def _merge_static_vars(self, data: dict[str, str]) -> None:
        """
        merge calc result (overwrite is strictly prohibited)
        """
        for key in data.keys():
            if key in self._with_static_vars:
                raise InvalidFormat(
                    f"Scope conflict in 'with_vars': '{key}' is defined "
                    "in both global and step levels. It cannot be overwritten."
                )
        self._with_static_vars = data | self._with_static_vars


_warned_with_vars: bool = False


class StepModel(_BaseWithVars):
    step: str = Field(frozen=True)
    class_name: str = Field(alias="class", frozen=True)
    listeners: str | list[str] | None = None
    symbol: str | None = Field(default=None, frozen=True)
    arguments: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _extract_with_vars(cls, data: Any) -> Any:
        """
        v2 compatibility
        """
        if not isinstance(data, dict):
            return data
        arguments = data.get("arguments")
        if isinstance(arguments, dict) and "with_vars" in arguments:
            if "with_vars" in data:
                raise InvalidFormat("duplicate definition 'with_vars'")
            data["with_vars"] = arguments.pop("with_vars")
            global _warned_with_vars
            if not _warned_with_vars:
                _warn_deprecated(
                    "scenario file's scenario.[].arguments.with_vars",
                    "3.0",
                    "4.0",
                    "scenario.[].with_vars",
                )
                _warned_with_vars = True
        return data

    def get_listeners(self) -> list[str]:
        """
        Get the listeners in list format.
        """
        if isinstance(self.listeners, str):
            return [self.listeners]
        elif isinstance(self.listeners, list):
            return self.listeners
        else:
            return []

    def replace_vars(self) -> None:
        """
        Replace variable expressions in arguments.
        This is need to be called after call calc.
        """
        self.arguments = self._replace_arguments(self.arguments)

    def replace_args(self, args: dict[str, str]) -> None:
        """
        Inject ``args.*`` static vars and substitute matching references in ``arguments``.
        """
        self._with_static_vars.update({f"args.{k}": v for k, v in args.items()})
        self.arguments = self._replace_arguments(self.arguments, "args")

    def _replace_arguments(self, arguments: Any, var_namespace: str | None = None) -> Any:
        """
        Replace nested arguments, optionally limited to a single var namespace.
        """
        if isinstance(arguments, dict):
            return {k: self._replace_arguments(v, var_namespace) for k, v in arguments.items()}
        elif isinstance(arguments, list):
            return [self._replace_arguments(v, var_namespace) for v in arguments]
        elif isinstance(arguments, str):
            matches = re.compile(r"{{(.*?)}}").findall(arguments)
            for match in matches:
                var_name = match.strip()
                arguments = self._replace_vars(arguments, var_name, var_namespace)
            return arguments
        else:
            return arguments

    def _replace_vars(self, value: str, var_name: str, var_namespace: str | None = None) -> str:
        """
        Replace the value of ``{{ var_name }}``; skip if it is outside ``var_namespace``.
        """
        if not var_name:
            raise InvalidParameter("name in variable expression was empty.")
        # When a namespace is requested, leave references in other namespaces untouched.
        var_prefix = var_name.split(".", 1)[0] if "." in var_name else None
        if var_namespace is not None and var_prefix != var_namespace:
            return value
        if var_prefix == "env":
            replace_str = os.environ.get(var_name[4:])
        else:
            replace_str = self._with_static_vars.get(var_name)
        if replace_str is None:
            raise ScenarioFileInvalid(
                "scenario file is invalid." " variable '%s' can not be resolved." % var_name
            )
        return re.sub(r"{{\s*%s\s*}}" % re.escape(var_name), replace_str, value)


class ParallelConfigModel(BaseModel):
    """
    Configuration for the parallel execution feature.

    Warning:
        Unsupported feature. See ``docs/scenario_configuration.md``.
    """

    multi_process_count: int | None = Field(default=None, ge=2)
    force_continue: bool | None = None

    def merge(self, model: Self) -> None:
        """
        Merge model's props (only when self value is None)
        """
        if not isinstance(model, ParallelConfigModel):
            return
        for k, v in self.model_dump().items():
            if v is None:
                r = getattr(model, k)
                if r is not None:
                    setattr(self, k, r)

    def fill_default(self) -> Self:
        """
        Set defalut values if they are None
        """
        if self.multi_process_count is None:
            self.multi_process_count = 2
        if self.force_continue is None:
            self.force_continue = False
        return self


class RecipeStepModel(BaseModel):
    """
    Load-time directive representing a ``recipe:`` reference within a scenario list.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)

    recipe: str = Field(frozen=True)
    arguments: dict[str, str] = Field(default_factory=dict)


class ParallelStepModel(BaseModel):
    """
    Scenario step model representing a ``parallel:`` block.

    Warning:
        Unsupported feature. See ``docs/scenario_configuration.md``.
    """

    step: str | None = Field(default=None, frozen=True)
    parallel: Tuple[StepModel, ...] = Field(min_length=1, frozen=True)
    parallel_config: ParallelConfigModel = Field(default_factory=ParallelConfigModel)

    def _merge_parallel_config(self, data: ParallelConfigModel) -> None:
        """
        merge ParallelConfig to under this model.
        """
        if not isinstance(data, ParallelConfigModel):
            return
        self.parallel_config.merge(data)


class ScenarioModel(_BaseWithVars):
    scenario: list[RecipeStepModel | StepModel | ParallelStepModel, ...]
    parallel_config: ParallelConfigModel = Field(default_factory=ParallelConfigModel)

    def is_readable_as_common(self) -> bool:
        """
        True if this scenario is shaped to be loadable as a common scenario.
        """
        # Common scenarios serve only as a source of argument/with_vars defaults
        # merged by class_name into the main scenario, so every entry must be a
        # plain StepModel (recipe / parallel directives have nothing to merge).
        return all(isinstance(s, StepModel) for s in self.scenario)

    def merge(self, cmn: Self) -> None:
        """
        Merge common scenario settings.
        """
        self.parallel_config.merge(cmn.parallel_config)

        for key in cmn.with_vars.keys():
            if key in self.with_vars:
                raise InvalidFormat(
                    f"Global 'with_vars' conflict: '{key}' is defined in multiple files."
                )
        self.with_vars = cmn.with_vars | self.with_vars

        for step in self.scenario:
            if isinstance(step, StepModel):
                self._merge_step(step, cmn)
            elif isinstance(step, ParallelStepModel):
                for p_step in step.parallel:
                    self._merge_step(p_step, cmn)

    def _merge_step(self, step: StepModel, cmn: Self) -> None:
        """
        shallow merge
        """
        # ``cmn`` is guaranteed to contain only StepModel entries by upstream
        # validation (``is_readable_as_common``), so no isinstance check here.
        for cmn_step in cmn.scenario:
            if step.class_name != cmn_step.class_name:
                continue

            step.arguments = cmn_step.arguments | step.arguments

            for key in cmn_step.with_vars.keys():
                if key in step.with_vars:
                    raise InvalidFormat(
                        f"Step 'with_vars' conflict in class '{step.class_name}': "
                        f"'{key}' is defined in multiple files."
                    )
            step.with_vars = cmn_step.with_vars | step.with_vars
            return

    def setup(self) -> None:
        """
        Prepare to use scenario.

        1. calc scenario's with_vars
        2. calc step's  with_vars
        3. propagate calculated with_vars and parallel_config from scenario to steps
        4. replace step's arguments with calculated with_vars
        """
        self.calc()
        self._calc_steps()
        self._propagate()
        self._replace_vars_steps()

    def _apply_steps(self, func: str, *args, **kwargs) -> None:
        for step in self.scenario:
            if isinstance(step, StepModel):
                getattr(step, func)(*args, **kwargs)
            elif isinstance(step, ParallelStepModel):
                for p_step in step.parallel:
                    getattr(p_step, func)(*args, **kwargs)

    def _apply_parallel_steps(self, func: str, *args, **kwargs) -> None:
        for step in self.scenario:
            if isinstance(step, ParallelStepModel):
                getattr(step, func)(*args, **kwargs)

    def _calc_steps(self) -> None:
        """
        exec 'calc' method in all steps.
        """
        self._apply_steps("calc")

    def _propagate(self) -> None:
        """
        merge _with_static_vars and parallel_config to scenario steps.
        """
        if self._with_static_vars:
            self._apply_steps("_merge_static_vars", self._with_static_vars)
        self._apply_parallel_steps("_merge_parallel_config", self.parallel_config)

    def _replace_vars_steps(self) -> None:
        """
        exec 'replace_vars' method in all steps.
        """
        self._apply_steps("replace_vars")


class CommandArgument(BaseModel):
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def convert_single_string_to_list(cls, value):
        if not isinstance(value, list) and isinstance(value, str):
            if value == "":
                return []
            return [value]
        return value


class RecipeParameterSpec(BaseModel):
    """
    A single parameter declaration in a recipe file.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)

    description: str
    default: str | None = None

    @property
    def is_required(self) -> bool:
        """
        True when no default is set (the caller must supply a value).
        """
        return self.default is None


class RecipeModel(BaseModel):
    """
    Top-level model for a recipe file.
    """

    parameters: dict[str, RecipeParameterSpec] = Field(default_factory=dict)
    recipe: list[StepModel, ...] = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def _normalize_parameter_shorthand(cls, data: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize the string shorthand of parameter declarations into the standard dict form.
        """
        if not isinstance(data, dict):
            return data
        params = data.get("parameters")
        if not isinstance(params, dict):
            return data
        normalized: dict[str, Any] = {}
        for name, spec in params.items():
            if isinstance(spec, str):
                normalized[name] = {"description": spec}
            else:
                normalized[name] = spec
        data["parameters"] = normalized
        return data

    def apply_args(self, args: dict[str, str]) -> None:
        """
        Substitute ``{{ args.x }}`` references in every step under ``recipe:``.
        """
        for step in self.recipe:
            step.replace_args(args)

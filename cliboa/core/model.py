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

from pydantic import BaseModel, Field, PrivateAttr, model_validator

# 'typing.Self' is available in Python 3.11+
from typing_extensions import Self

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
        merge calc result (not overwrite)
        """
        self._with_static_vars = data | self._with_static_vars


class StepModel(_BaseWithVars):
    step: str = Field(frozen=True)
    class_name: str = Field(alias="class", frozen=True)
    listeners: str | list[str] | None = None
    symbol: str | None = Field(default=None, frozen=True)
    arguments: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _extract_with_vars(cls, data: Any) -> Any:
        """v2 compatibility"""
        if not isinstance(data, dict):
            return data
        arguments = data.get("arguments")
        if isinstance(arguments, dict) and "with_vars" in arguments:
            if "with_vars" in data:
                raise InvalidFormat("duplicate definition 'with_vars'")
            data["with_vars"] = arguments.pop("with_vars")
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

    def _replace_arguments(self, arguments: Any) -> Any:
        """
        Replace nested argments.
        """
        if isinstance(arguments, dict):
            return {k: self._replace_arguments(v) for k, v in arguments.items()}
        elif isinstance(arguments, list):
            return [self._replace_arguments(v) for v in arguments]
        elif isinstance(arguments, str):
            matches = re.compile(r"{{(.*?)}}").findall(arguments)
            for match in matches:
                var_name = match.strip()
                arguments = self._replace_vars(arguments, var_name)
            return arguments
        else:
            return arguments

    def _replace_vars(self, value: str, var_name: str):
        """
        This method replaces the value of {{ xxx }}
        """
        if not var_name:
            raise InvalidParameter("name in variable expression was empty.")
        if var_name.startswith("env."):
            replace_str = os.environ.get(var_name[4:])
        else:
            replace_str = self._with_static_vars.get(var_name)
        if replace_str is None:
            raise ScenarioFileInvalid(
                "scenario file is invalid." " variable '%s' can not be resolved." % var_name
            )
        return re.sub(r"{{(\s?)%s(\s?)}}" % var_name, replace_str, value)


class ParallelConfigModel(BaseModel):
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


class ParallelStepModel(BaseModel):
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
    scenario: list[StepModel | ParallelStepModel, ...]
    parallel_config: ParallelConfigModel = Field(default_factory=ParallelConfigModel)

    def merge(self, cmn: Self) -> None:
        """
        Merge common scenario settings.
        """
        self.parallel_config.merge(cmn.parallel_config)
        self.with_vars = cmn.with_vars | self.with_vars

        for step in self.scenario:
            if isinstance(step, StepModel):
                self._merge_step(step, cmn)
            elif isinstance(step, ParallelStepModel):
                for p_step in step.parallel:
                    self._merge_step(p_step, cmn)

    def _merge_step(self, step: StepModel, cmn: Self) -> None:
        """shallow marge"""
        for cmn_step in cmn.scenario:
            if not isinstance(cmn_step, StepModel):
                continue
            if step.class_name != cmn_step.class_name:
                continue

            step.arguments = cmn_step.arguments | step.arguments
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

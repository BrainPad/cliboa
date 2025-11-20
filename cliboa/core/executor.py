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
import copy
from abc import abstractmethod
from typing import Any

from cliboa.core.interface import _IContext, _IExecute
from cliboa.core.model import CommandArgument, StepModel
from cliboa.listener.interface import IScenarioExecutor
from cliboa.scenario.base import AbstractStep
from cliboa.scenario.interface import IParentStep
from cliboa.util.base import _BaseObject
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus


class _BaseExecutor(_BaseObject, _IExecute):
    """
    Base class of executor.
    Execute main logic with handling listeners.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._listeners = []

    def register_listener(self, listener) -> None:
        self._listeners.append(listener)

    def _get_listener_arg(self) -> Any:
        return self

    def _execute_listener(self, kind: str, e: Exception | None = None) -> None:
        for lis in self._listeners:
            try:
                target = getattr(lis, kind)
                if callable(target):
                    if e:
                        target(self._get_listener_arg(), e)
                    else:
                        target(self._get_listener_arg())
            except Exception:
                self._logger.exception(
                    f"Error occurred during {lis.__class__.__name__}.{kind}"
                    f" in {self.__class__.__name__}"
                )

    def execute(self) -> int | None:
        """
        Execute with listeners.
        """
        try:
            self._execute_listener("before")
            res = self._execute_main()
            self._execute_listener("after")
            return res
        except Exception as e:
            self._logger.exception(
                "{}({}) occurred during the execution of {}.{}".format(
                    e.__class__.__name__,
                    str(e),
                    self.__class__.__module__,
                    self.__class__.__name__,
                )
            )
            self._execute_listener("error", e)
            return StepStatus.ABNORMAL_TERMINATION
        finally:
            self._execute_listener("completion")

    @abstractmethod
    def _execute_main(self) -> int | None:
        raise NotImplementedError()


class _ScenarioExecutor(_BaseExecutor, IScenarioExecutor):
    """
    Executor for scenario
    """

    def __init__(self, steps: list[_IExecute], **kwargs):
        super().__init__(**kwargs)
        self._steps = steps
        self._max_steps_size = len(steps)

    @property
    def max_steps_size(self) -> int:
        """
        Get max inisteps size (set during initialization).
        """
        return self._max_steps_size

    @property
    def current_steps_size(self) -> int:
        """
        Get current remaining steps size.
        """
        return len(self._steps)

    def _execute_main(self) -> int:
        """
        Wrap steps with _StepExecutor and execute.
        """
        res = None
        while len(self._steps) > 0:
            step = self._steps.pop(0)
            res = step.execute()
            if res is None:
                continue
            elif res == StepStatus.SUCCESSFUL_TERMINATION:
                self._logger.info("Step response [successful termination]. Scenario will be end.")
                break
            else:
                self._logger.error(
                    f"Step response [abnormal termination: {res}]. Scenario will be end."
                )
                break

        return StepStatus.SUCCESSFUL_TERMINATION if res is None else res


class _StepExecutor(_BaseExecutor, IParentStep):
    """
    Executor for step
    """

    def __init__(
        self,
        step: AbstractStep,
        model: StepModel,
        cmd_arg: CommandArgument | None = None,
        context: _IContext | None = None,
        symbol_model: StepModel | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        step.parent = self
        step._set_arguments(model.arguments)
        self._step = step
        self._model = model
        self._context = context
        self._symbol_model = symbol_model
        self._exec_args = copy.deepcopy(cmd_arg.args) if cmd_arg and cmd_arg.args else []
        self._exec_kwargs = copy.deepcopy(cmd_arg.kwargs) if cmd_arg and cmd_arg.kwargs else {}

    @property
    def step(self) -> AbstractStep:
        return self._step

    @property
    def step_name(self) -> str:
        return self._model.step

    @property
    def symbol_name(self) -> str | None:
        return self._model.symbol

    def get_symbol_arguments(self) -> dict[str, Any]:
        if self._symbol_model:
            return self._symbol_model.arguments
        else:
            return {}

    def put_to_context(self, value: Any) -> None:
        if self._context:
            self._context.put(self.step_name, value)
        # v2 backward compability
        ObjectStore.put(self.step_name, value, quiet=True)

    def get_from_context(self, target: str | None = None) -> Any:
        if target is None:
            target = self.symbol_name
        if self._context:
            return self._context.get(target)

    def _get_listener_arg(self) -> Any:
        return self.step

    def _execute_main(self) -> int | None:
        return self.step.execute(*self._exec_args, **self._exec_kwargs)

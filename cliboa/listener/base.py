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
from abc import abstractmethod

from pydantic import BaseModel

from cliboa.listener.interface import IScenarioExecutor
from cliboa.scenario.base import BaseStep
from cliboa.scenario.interface import IParentStep
from cliboa.util.base import _BaseObject
from cliboa.util.exception import CliboaException


class BaseListener(_BaseObject):
    """
    Base listener for all the listener classes
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._executor = None

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @abstractmethod
    def before(self) -> None:
        """
        Execute before main logic start.
        """
        pass

    @abstractmethod
    def after(self) -> None:
        """
        Execute after main logic end normally.
        """
        pass

    @abstractmethod
    def error(self, e: Exception) -> None:
        """
        Execute after main logic raises Exception.
        """
        pass

    @abstractmethod
    def completion(self) -> None:
        """
        Execute after main logic always.
        """
        pass


class BaseScenarioListener(BaseListener):
    """
    Listener for scenario
    """

    def _prepare(self, executor: IScenarioExecutor) -> None:
        self._executor = executor

    @property
    def executor(self) -> IScenarioExecutor:
        if not self._executor:
            raise CliboaException("No scenario executor instance found.")
        return self._executor

    def before(self) -> None:
        pass

    def after(self) -> None:
        pass

    def error(self, e: Exception) -> None:
        pass

    def completion(self) -> None:
        pass


class BaseStepListener(BaseListener):
    """
    Listener for step
    """

    Arguments: type[BaseModel] | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._step = None
        self._args = None

    def _prepare(self, executor: IParentStep, step: BaseStep) -> None:
        self._executor = executor
        self._step = step
        if self.Arguments is not None:
            self._args = self.Arguments.model_validate(executor.raw_arguments)

    @property
    def parent(self) -> IParentStep:
        if not self._executor:
            raise CliboaException("No step executor instance found.")
        return self._executor

    @property
    def step(self) -> BaseStep:
        if not self._step:
            raise CliboaException("No step instance found.")
        return self._step

    @property
    def args(self) -> BaseModel | None:
        return self._args

    def before(self) -> None:
        pass

    def after(self) -> None:
        pass

    def error(self, e: Exception) -> None:
        pass

    def completion(self) -> None:
        pass

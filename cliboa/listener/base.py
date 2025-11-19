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
from abc import abstractmethod

from cliboa.listener.interface import IScenarioExecutor
from cliboa.scenario.base import AbstractStep
from cliboa.util.base import _BaseObject


class BaseListener(_BaseObject):
    """
    Base listener for all the listener classes
    """

    @abstractmethod
    def before(self, obj) -> None:
        """
        Execute before main logic start.
        """
        pass

    @abstractmethod
    def after(self, obj) -> None:
        """
        Execute after main logic end normally.
        """
        pass

    @abstractmethod
    def error(self, obj, e: Exception) -> None:
        """
        Execute after main logic raises Exception.
        """
        pass

    @abstractmethod
    def completion(self, obj) -> None:
        """
        Execute after main logic always.
        """
        pass


class BaseScenarioListener(BaseListener):
    """
    Listener for scenario
    """

    def before(self, executor: IScenarioExecutor) -> None:
        pass

    def after(self, executor: IScenarioExecutor) -> None:
        pass

    def error(self, executor: IScenarioExecutor, e: Exception) -> None:
        pass

    def completion(self, executor: IScenarioExecutor) -> None:
        pass


class BaseStepListener(BaseListener):
    """
    Listener for step
    """

    def before(self, step: AbstractStep) -> None:
        pass

    def after(self, step: AbstractStep) -> None:
        pass

    def error(self, step: AbstractStep, e: Exception) -> None:
        pass

    def completion(self, step: AbstractStep) -> None:
        pass

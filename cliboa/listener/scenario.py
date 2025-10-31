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
from cliboa import state
from cliboa.listener.base import BaseScenarioListener
from cliboa.listener.interface import IScenarioExecutor


class ScenarioStatusListener(BaseScenarioListener):
    """
    Listener for scenario execution status
    """

    def before(self, executor: IScenarioExecutor) -> None:
        state.set("_ExecuteScenario")
        self._logger.info(f"Start scenario execution. StepQueue size is {executor.max_steps_size}")

    def after(self, executor: IScenarioExecutor) -> None:
        state.set("_ExecuteScenario")

    def error(self, executor: IScenarioExecutor, e: Exception) -> None:
        state.set("_ExecuteScenario")

    def completion(self, executor: IScenarioExecutor) -> None:
        state.set("_ExecuteScenario")
        self._logger.info(
            f"Complete scenario execution. StepQueue size is {executor.current_steps_size}"
        )

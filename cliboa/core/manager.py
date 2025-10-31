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
from cliboa.core.builder import _ScenarioBuilder
from cliboa.core.executor import _ScenarioExecutor
from cliboa.core.model import CommandArgument
from cliboa.listener.scenario import ScenarioStatusListener
from cliboa.util.base import _BaseObject


class ScenarioManager(_BaseObject):
    """
    Main class to execute pipeline by scenario files.
    """

    def __init__(
        self,
        scenario_file: str,
        common_file: str | list[str] | None = None,
        file_format: str = "yaml",
        cmd_arg: CommandArgument | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._builder = self._resolve(
            "scenario_builder", _ScenarioBuilder, scenario_file, common_file, file_format, cmd_arg
        )

    def execute(self) -> int:
        """
        Main logic of cliboa.
        """
        # 1. Load scenario files and build scenario step instances which are ready to execute.
        state.set("_BuildScenario")
        steps = self._builder.execute()

        # 2. Prepare the steps by wrapping them in an executor instance.
        state.set("_PrepareScenario")
        executor = self._resolve("scenario_executor", _ScenarioExecutor, steps)
        executor.register_listener(
            self._resolve("scenario_status_listener", ScenarioStatusListener)
        )

        # 3. Execute the scenario and return the int result code.
        state.set("_ExecuteScenario")
        return executor.execute()

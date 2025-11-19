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

from cliboa.core.executor import _StepExecutor
from cliboa.core.factory import _cliboa_factory, _get_scenario_loader_class
from cliboa.core.interface import _IExecute
from cliboa.core.loader import _ScenarioLoader
from cliboa.core.model import CommandArgument, ParallelStepModel, ScenarioModel, StepModel
from cliboa.core.processor import _ParallelProcessor
from cliboa.listener.base import BaseStepListener
from cliboa.listener.step import StepStatusListener
from cliboa.util.base import _BaseObject
from cliboa.util.cache import StepArgument
from cliboa.util.exception import CliboaException


class _ScenarioBuilder(_BaseObject):
    """
    Build executable instances from scenario file and command argument.
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
        self._scenario_file = scenario_file
        if isinstance(common_file, str):
            self._common_files = [common_file]
        elif isinstance(common_file, list):
            self._common_files = common_file
        else:
            self._common_files = []
        self._loader_cls: _ScenarioLoader = self._resolve_cls(
            "loader", _get_scenario_loader_class(file_format)
        )
        self._scenario_model_cls = self._resolve_cls("scenario_model", ScenarioModel)
        self._step_argument_cls = self._resolve_cls("step_argument", StepArgument)
        self._factory = self._resolve("factory", _cliboa_factory)
        self._cmd_arg = cmd_arg

    def execute(self) -> list[_IExecute]:
        """
        Main logic of builder.
        """
        self._logger.info("Start to build scenario.")
        # 1. Get a scenario model including step models.
        scenario = self._parse_scenario()
        # 2. Set up the scenario model - propagate settings and calc with_vars.
        scenario.setup()
        # 3. Create step instances by step models which are in the scenario model.
        steps = self._create_steps(scenario)
        self._logger.info("Complete to build scenario.")
        return steps

    def _parse_scenario(self) -> ScenarioModel:
        """
        Load main scenario file and generate a model instance.

        if common file exists, merge it to main.
        """
        self._logger.info("Start to parse scenario files.")

        self._logger.info(f"Load main scenario file {self._scenario_file}")
        top_dict = self._loader_cls(self._scenario_file, True)()
        main_scenario = self._scenario_model_cls.model_validate(top_dict)

        for cmn_scenario_file in self._common_files:
            self._logger.info(f"Load common scenario file {cmn_scenario_file}")
            top_dict = self._loader_cls(cmn_scenario_file, False)()
            if top_dict:
                cmn_scenario = self._scenario_model_cls.model_validate(top_dict)
                main_scenario.merge(cmn_scenario)
            else:
                self._logger.warning(
                    f"Failed to load {cmn_scenario_file}, continue to parse scenario."
                )

        self._logger.info("Complete to parse scenario files.")
        return main_scenario

    def _create_steps(self, scenario: ScenarioModel) -> list[_IExecute]:
        """
        Add executable instance to the queue
        """
        steps = []
        for step in scenario.scenario:
            instance = self._create_step_instance(step)
            steps.append(instance)
        return steps

    def _create_step_instance(
        self, step: StepModel | ParallelStepModel
    ) -> _StepExecutor | _ParallelProcessor:
        """
        Create executable instances
        """
        if isinstance(step, StepModel):
            instance = self._create_executor(step)
            # save arguments to refer by symbol as a global variables.
            self._step_argument_cls._put(step.step, instance.step)
            return instance
        elif isinstance(step, ParallelStepModel):
            instances = []
            for p_step in step.parallel:
                instance = self._create_executor(p_step)
                instances.append(instance)
                # save arguments to refer by symbol as a global variables.
                self._step_argument_cls._put(p_step.step, instance.step)
            return self._resolve(
                "parallel_processor", _ParallelProcessor, instances, step.parallel_config
            )
        else:
            raise CliboaException(f"Unexpected step instance: {step.__class__.__name__}")

    def _create_executor(self, step: StepModel) -> _StepExecutor:
        """
        Create _StepExecutor instance - wraps BaseStep.
        """
        cls_name = step.class_name
        self._logger.debug("Create %s instance" % cls_name)

        instance = self._factory.create(cls_name)
        executor = self._resolve("step_executor", _StepExecutor, instance, step, self._cmd_arg)
        for lis in self._create_listeners(step):
            executor.register_listener(lis)
        return executor

    def _create_listeners(self, step: StepModel) -> list[BaseStepListener]:
        listeners = [self._resolve("step_status_listener", StepStatusListener())]
        if step.listeners is not None:
            arguments = copy.deepcopy(step.arguments)
            if isinstance(step.listeners, str):
                lis_classes = [step.listeners]
            elif isinstance(step.listeners, list):
                lis_classes = step.listeners
            else:
                raise CliboaException(
                    f"Unexpectedly, the scenario step's 'listeners' was {type(step.listeners)}"
                    ", neither a str, list, nor None."
                )
            for lis_cls in lis_classes:
                clz = self._factory.create(lis_cls)
                clz.__dict__.update(arguments)
                listeners.append(clz)
        return listeners

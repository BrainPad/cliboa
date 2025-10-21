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
import os

from cliboa.conf import env
from cliboa.core.file_parser import ScenarioParser
from cliboa.core.listener import StepStatusListener
from cliboa.core.model import ParallelConfigModel, ParallelStepModel, ScenarioModel, StepModel
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import StepQueue
from cliboa.scenario import *  # noqa
from cliboa.util.base import _BaseObject
from cliboa.util.cache import StepArgument
from cliboa.util.class_util import ClassUtil
from cliboa.util.exception import CliboaException, InvalidFormat
from cliboa.util.helper import Helper
from cliboa.util.parallel_with_config import ParallelWithConfig


class ScenarioManager(_BaseObject):
    """
    Base class which is to create individual instances from scenario files,
    and push them to the executable queue.

    Note : Currently only yaml format scenario file is implemented.
    """

    def __init__(self, project_name: str, scenario_format: str):
        super().__init__()
        if scenario_format == "yaml":
            pj_scenario_file = (
                os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_FILE_NAME) + ".yml"
            )
            cmn_scenario_file = os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + ".yml"
        elif scenario_format == "json":
            pj_scenario_file = (
                os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_FILE_NAME)
                + "."
                + scenario_format
            )
            cmn_scenario_file = (
                os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + "." + scenario_format
            )
        else:
            raise InvalidFormat(f"scenario format '{scenario_format}' is invalid.")

        self._parser = ScenarioParser(pj_scenario_file, cmn_scenario_file, scenario_format)

    def create_scenario_queue(self):
        """
        Main logic
        """
        self._logger.info("Start to create scenario queue.")
        scenario = self._parser.parse()
        scenario.setup()
        queue = self._create_queue(scenario)
        # save queue as a global variable
        setattr(ScenarioQueue, "step_queue", queue)
        self._logger.info("Finish to create scenario queue.")

    def _create_queue(self, scenario: ScenarioModel) -> StepQueue:
        """
        Add executable instance to the queue
        """
        # StepQueue is custom class extending queue.Queue
        queue = StepQueue()
        for step in scenario.scenario:
            instance = self._create_step_instance(step)
            queue.put(instance)

        return queue

    def _create_step_instance(
        self, step: StepModel | ParallelStepModel
    ) -> BaseStep | ParallelWithConfig:  # noqa
        """
        Create executable instances
        """
        if isinstance(step, StepModel):
            instance = self._create_instance(step)
            # save arguments to refer by symbol as a global variables.
            StepArgument._put(step.step, instance)
            return instance
        elif isinstance(step, ParallelStepModel):
            instances = []
            for p_step in step.parallel:
                instance = self._create_instance(p_step)
                instances.append(instance)
                # save arguments to refer by symbol as a global variables.
                StepArgument._put(p_step.step, instance)
            # Memo: ParallelWithConfig is named-tuple.
            if step.parallel_config is None:
                return ParallelWithConfig(instances, ParallelConfigModel().fill_default())
            else:
                return ParallelWithConfig(instances, step.parallel_config.fill_default())
        else:
            raise CliboaException(f"Unexpected step instance: {step.__class__.__name__}")

    def _create_instance(self, step: StepModel) -> BaseStep:  # noqa
        """
        Create instance

        Memo: resolve with_vars in this method.
        """
        cls_name = step.class_name
        self._logger.debug("Create %s instance" % cls_name)

        # Create BaseStep instance.
        if ClassUtil().is_custom_cls(cls_name) is True:
            from cliboa.core.factory import CustomInstanceFactory

            instance = CustomInstanceFactory.create(cls_name)
        else:
            cls = globals()[cls_name]
            instance = cls()

        # Set arguments to instance.
        if step.arguments:
            for k, v in step.arguments:
                Helper.set_property(instance, k, v)

        # Set metadata to instance.
        Helper.set_property(instance, "step", step.step)
        Helper.set_property(instance, "symbol", step.symbol)
        # Add listeners
        Helper.set_property(instance, "listeners", self._generate_listeners(step))

        return instance

    def _append_listeners(self, step: StepModel):
        listeners = [StepStatusListener()]
        if step.listeners is not None:
            from cliboa.core.factory import CustomInstanceFactory

            arguments = copy.deepcopy(step.arguments)
            if type(step.listeners) is str:
                clz = CustomInstanceFactory.create(step.listeners)
                clz.__dict__.update(arguments)
                listeners.append(clz)
            elif type(step.listeners) is list:
                for listener_cls in step.listeners:
                    clz = CustomInstanceFactory.create(listener_cls)
                    clz.__dict__.update(arguments)
                    listeners.append(CustomInstanceFactory.create(clz))
        return listeners

#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
from cliboa.conf import env
from cliboa.core.manager import YamlScenarioManager, JsonScenarioManager
from cliboa.core.step_queue import *
from cliboa.core.strategy import *
from cliboa.scenario.sample_step import *


class ScenarioManagerFactory(object):
    """
    Create scenario manager instance
    """

    @staticmethod
    def create(cmd_args):
        """
        Create ScenarioManager Instance(YamlScenarioManager or JsonScenarioManager)
        Args:
            cmd_args: Command Line Arguments
        Returns:
            scenario manager instance
        """
        scenario_file_format = cmd_args.format
        scenario_manager_cls = scenario_file_format.capitalize() + "ScenarioManager"
        instance = globals()[scenario_manager_cls]
        return instance(cmd_args)


class StepQueueFactory(object):
    """
    Create scenario queue instance
    """

    @staticmethod
    def create(scenario_type):
        """
        Args:
            scenario_type: step or extract ot transform or load or None
        Returns:
            scenario queue instance
        """
        if scenario_type:
            queue_cls = scenario_type.capitalize() + "Queue"
        else:
            queue_cls = "StepQueue"

        instance = globals()[queue_cls]
        return instance()


class StepExecutorFactory(object):
    """
    Create step execution strategy instance
    """

    @staticmethod
    def create(obj):
        """
        Args:
            obj: queue which stores execution target steps
        Returns:
            step execution strategy instance
        """
        if len(obj) > 1:
            return MultiProcExecutor(obj)

        return SingleProcExecutor(obj)


class CustomInstanceFactory(object):
    """
    Import python module and create instance dynamically
    """

    @staticmethod
    def create(cls_name):
        custom_classes = env.COMMON_CUSTOM_CLASSES + env.PROJECT_CUSTOM_CLASSES
        custom_cls_list = [c for c in custom_classes if cls_name in c]
        custom_cls = " ".join(str(c) for c in custom_cls_list)
        components = custom_cls.split(".")
        mod = __import__(components[0])
        instance = getattr(mod, components[1])
        return instance()

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
from cliboa.core.manager import JsonScenarioManager, YamlScenarioManager  # noqa
from cliboa.core.strategy import MultiProcExecutor, SingleProcExecutor
from cliboa.util.class_util import ClassUtil
from importlib import import_module


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

    Return:
        Created instance.
        None: If cls_name was not found in the defined class list.
    """

    @staticmethod
    def create(cls_name):
        ret = ClassUtil().describe_class(cls_name)
        if ret is None:
            return None
        (root, mod_name) = ret
        instance = getattr(import_module(root), mod_name)
        return instance()

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
from importlib import import_module

from cliboa.core.loader import _JsonScenarioLoader, _ScenarioLoader, _YamlScenarioLoader
from cliboa.core.strategy import MultiProcExecutor, SingleProcExecutor, _StepExecutor
from cliboa.scenario.base import BaseStep
from cliboa.util.class_util import ClassUtil
from cliboa.util.exception import InvalidFormat
from cliboa.util.parallel_with_config import ParallelWithConfig


def _get_scenario_loader_class(scenario_format: str) -> type[_ScenarioLoader]:
    """
    Create scenario loader instance
    """
    if scenario_format == "yaml":
        return _YamlScenarioLoader
    elif scenario_format == "json":
        return _JsonScenarioLoader
    else:
        raise InvalidFormat(f"scenario format '{scenario_format}' is invalid.")


def _create_step_executor(obj: BaseStep | ParallelWithConfig) -> _StepExecutor:
    """
    Create step execution strategy instance

    Args:
        obj: queue which stores execution target steps
    Returns:
        step execution strategy instance
    """
    if isinstance(obj, ParallelWithConfig):
        return MultiProcExecutor(obj)
    else:
        return SingleProcExecutor(obj)


def _create_custom_instance(cls_name: str):
    """
    Import python module and create instance dynamically

    Return:
        Created instance.
        None: If cls_name was not found in the defined class list.
    """
    ret = ClassUtil().describe_class(cls_name)
    if ret is None:
        return None
    (root, mod_name) = ret
    instance = getattr(import_module(root), mod_name)
    return instance()

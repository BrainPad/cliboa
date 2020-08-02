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
import os

from cliboa.core.step_queue import StepQueue
from cliboa.util.exception import (
    DirStructureInvalid,
    FileNotFound,
    InvalidCount,
    ScenarioFileInvalid
)


class ValidatorChain(object):
    """
    Chain of responsibility pattern
    """

    def __init__(self, val):
        """
        Args:
            val: validation target value
        """
        self._val = val


class ScenarioYamlType(ValidatorChain):
    def __call__(self):
        """
        Validate parsed scenario.yml instance type
        """
        yaml_dict = self._val
        if not isinstance(yaml_dict, dict):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. Check scenario.yml format."
            )


class ScenarioYamlKey(ValidatorChain):
    def __call__(self):
        """
        Validate scenario key in scenario.yml
        """
        yaml_dict = self._val
        scenario = yaml_dict.get("scenario")
        if not scenario:
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
            )


class ProjectDirectoryExistence(object):
    """
    If project directory exists or not
    """

    def __call__(self, pj_dir):
        exists_pj_dir = os.path.isdir(pj_dir)
        if not exists_pj_dir:
            raise DirStructureInvalid("Project directory %s does not exist" % pj_dir)


class ScenarioFileExistence(object):
    """
    If scenario file exists or not
    """

    def __call__(self, scenario_file):
        exists_scenario_file = os.path.isfile(scenario_file)
        if not exists_scenario_file:
            raise FileNotFound("scenario.yml %s does not exist" % scenario_file)


class EssentialParameters(object):
    """
    Essential parameter validation
    """

    def __init__(self, cls_name, param_list):
        """
        Args:
            cls_name: class name which has validation target parameters
            param_list: list of validation target parameters
        """
        self._cls_name = cls_name
        self._param_list = param_list

    def __call__(self):
        for p in self._param_list:
            if not p:
                raise Exception(
                    "The essential parameter is not specified in %s." % self._cls_name
                )


class DIScenarioFormat(object):
    """
    Validation for dependency injection format in scenario.yml
    """

    def __init__(self, di_key, di_params):
        self.__di_key = di_key
        self.__di_params = di_params

    def __call__(self):
        if not (self.__di_params):
            raise ScenarioFileInvalid(
                "Dependeny Injection parameters are essential after %s" % self.__di_key
            )
        if not (self.__di_params.get("class")):
            raise ScenarioFileInvalid(
                "class: is not specified after %s" % self.__di_key
            )


class MultiProcessCount(object):
    """

    """

    def __init__(self, scenario_yaml_list):
        self.__scenario_yaml_list = scenario_yaml_list


class EssentialKeys(object):
    """
    Check if 'step: ' and 'class: $class_name' exist in scenario.yml
    """

    def __init__(self, scenario_yaml_list):
        self.__scenario_yaml_list = scenario_yaml_list

    def __call__(self):
        for scenario_yaml_dict in self.__scenario_yaml_list:
            multi_proc_cnt = scenario_yaml_dict.get("multi_process_count")
            parallel_steps = scenario_yaml_dict.get("parallel")
            if multi_proc_cnt:
                if multi_proc_cnt < StepQueue.DEFAULT_PARALLEL_CNT:
                    raise InvalidCount(
                        "Must specify more than %s as multi process count."
                        % StepQueue.DEFAULT_PARALLEL_CNT
                    )
                continue
            elif parallel_steps:
                for s in parallel_steps:
                    self.__exists_step(s)
                    self.__exists_class(s)
            else:
                self.__exists_step(scenario_yaml_dict)
                self.__exists_class(scenario_yaml_dict)

    def __exists_step(self, dict):
        if "step" not in dict.keys():
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'step:' does not exist."
            )

    def __exists_class(self, dict):
        if not dict.get("class"):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'class:' key does not exist, or 'class:' value does not exist."  # noqa
            )

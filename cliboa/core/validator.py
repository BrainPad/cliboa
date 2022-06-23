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
import os

from cliboa.util.exception import DirStructureInvalid, FileNotFound, ScenarioFileInvalid


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
            raise ScenarioFileInvalid("scenario.yml is invalid. Check scenario.yml format.")


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


class ScenarioJsonType(ValidatorChain):
    def __call__(self):
        """
        Validate parsed scenario.json instance type
        """
        json_dict = self._val
        if not isinstance(json_dict, dict):
            raise ScenarioFileInvalid("scenario.json is invalid. Check scenario.json format.")


class ScenarioJsonKey(ValidatorChain):
    def __call__(self):
        """
        Validate scenario key in scenario.json
        """
        json_dict = self._val
        scenario = json_dict.get("scenario")
        if not scenario:
            raise ScenarioFileInvalid(
                "scenario.json is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
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
                raise Exception("The essential parameter is not specified in %s." % self._cls_name)


class EssentialKeys(object):
    """
    Check if 'step: ' and 'class: $class_name' exist in scenario.yml
    # TODO change variable and comment dependenced by yaml
    """

    def __init__(self, scenario_yaml_list):
        self._scenario_yaml_list = scenario_yaml_list

    def __call__(self):
        if type(self._scenario_yaml_list) is not list:
            raise ScenarioFileInvalid("scenario.yml is invalid. it wad not a list")
        for scenario_yaml_dict in self._scenario_yaml_list:
            multi_proc_cnt = scenario_yaml_dict.get("multi_process_count")
            force_continue = scenario_yaml_dict.get("force_continue")
            parallel_steps = scenario_yaml_dict.get("parallel")
            parallel_with_config = scenario_yaml_dict.get("parallel_with_config")
            if multi_proc_cnt:
                continue
            elif force_continue is not None:
                continue
            elif parallel_steps:
                for s in parallel_steps:
                    self._exists_step(s)
                    self._exists_class(s)
            elif parallel_with_config:
                self._exists_config(parallel_with_config)
                self._exists_multi_process_count(parallel_with_config["config"])
                self._exists_steps(parallel_with_config)
                for s in parallel_with_config["steps"]:
                    self._exists_step(s)
                    self._exists_class(s)
            else:
                self._exists_step(scenario_yaml_dict)
                self._exists_class(scenario_yaml_dict)

    def _exists_step(self, dict):
        if "step" not in dict.keys():
            raise ScenarioFileInvalid("scenario.yml is invalid. 'step:' does not exist.")

    def _exists_class(self, dict):
        if not dict.get("class"):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'class:' key does not exist, or 'class:' value does not exist."  # noqa
            )

    def _exists_config(self, dict):
        if not dict.get("config"):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'config:' key does not exist, or 'config:' value does not exist."  # noqa
            )

    def _exists_steps(self, dict):
        if not dict.get("steps"):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'steps:' key does not exist, or 'steps:' value does not exist."  # noqa
            )

    def _exists_multi_process_count(self, dict):
        if not dict.get("multi_process_count"):
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'multi_process_count:' key does not exist, or 'multi_process_count:' value does not exist."  # noqa
            )

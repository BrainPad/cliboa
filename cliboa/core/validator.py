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
from cliboa.scenario.validator import EssentialParameters as ValidEP
from cliboa.util.base import _warn_deprecated
from cliboa.util.exception import ScenarioFileInvalid


class EssentialParameters(ValidEP):
    """
    DEPRECATED: Use cliboa.scenario.validator.EssentialParameters instead.
    This class will be removed in a future version (v3 or later).
    """

    def __init__(self, *args, **kwargs):
        _warn_deprecated(
            ".".join(("cliboa.core.validator", self.__class__.__name__)),
            ".".join(("cliboa.scenario.validator", self.__class__.__name__)),
        )
        super().__init__(*args, **kwargs)


class EssentialKeys(object):
    """
    Check if 'step: ' and 'class: $class_name' exist in scenario file
    # TODO change variable and comment dependenced by yaml
    """

    def __init__(self, scenario_yaml_list):
        self._scenario_yaml_list = scenario_yaml_list

    def __call__(self):
        if type(self._scenario_yaml_list) is not list:
            raise ScenarioFileInvalid("scenario file is invalid. it wad not a list")
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
            raise ScenarioFileInvalid("scenario file is invalid. 'step:' does not exist.")

    def _exists_class(self, dict):
        if not dict.get("class"):
            raise ScenarioFileInvalid(
                "scenario file is invalid. 'class:' key does not exist, or 'class:' value does not exist."  # noqa
            )

    def _exists_config(self, dict):
        if not dict.get("config"):
            raise ScenarioFileInvalid(
                "scenario file is invalid. 'config:' key does not exist, or 'config:' value does not exist."  # noqa
            )

    def _exists_steps(self, dict):
        if not dict.get("steps"):
            raise ScenarioFileInvalid(
                "scenario file is invalid. 'steps:' key does not exist, or 'steps:' value does not exist."  # noqa
            )

    def _exists_multi_process_count(self, dict):
        if not dict.get("multi_process_count"):
            raise ScenarioFileInvalid(
                "scenario file is invalid. 'multi_process_count:' key does not exist, or 'multi_process_count:' value does not exist."  # noqa
            )

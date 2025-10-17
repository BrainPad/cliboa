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
import re
import subprocess
from typing import Tuple

from cliboa.conf import env
from cliboa.core.file_parser import ScenarioParser
from cliboa.core.listener import StepStatusListener
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import StepQueue
from cliboa.scenario import *  # noqa
from cliboa.util.base import _BaseObject
from cliboa.util.cache import StepArgument
from cliboa.util.class_util import ClassUtil
from cliboa.util.exception import InvalidFormat, InvalidParameter, ScenarioFileInvalid
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
        scenario_list = self._parser.parse()
        self._save_scenario_queue(scenario_list)
        self._logger.info("Finish to create scenario queue.")

    def _save_scenario_queue(self, scenario_list: list[dict]) -> None:
        self._validate_scenario_list(scenario_list)
        queue = self._create_queue(scenario_list)
        # save queue as a global variable
        setattr(ScenarioQueue, "step_queue", queue)

    def _validate_scenario_list(self, scenario_list: list[dict]) -> None:
        if not isinstance(scenario_list, list):
            raise ScenarioFileInvalid("scenario file is invalid.")
        if len(scenario_list) == 0:
            raise ScenarioFileInvalid("scenario file is empty.")

    def _create_queue(self, scenario_list: list[dict]) -> StepQueue:
        """
        Add executable instance to the queue
        """
        # StepQueue is custom class extending queue.Queue
        queue = StepQueue()
        for block in scenario_list:
            # TODO: Redesign scenario data structure, especially all scenario configuration values.
            if "multi_process_count" in block.keys():
                Helper.set_property(queue, "multi_proc_cnt", block.get("multi_process_count"))
            elif "force_continue" in block.keys():
                Helper.set_property(queue, "force_continue", block.get("force_continue"))
            else:
                instance = self._create_step_instances(block)
                queue.put(instance)

        return queue

    def _create_step_instances(self, s_dict: dict) -> list[BaseStep | ParallelWithConfig]:  # noqa
        """
        Create executable instances

        This function returns a list, and its contents can be one of three patterns:
        1. A list containing a single BaseStep instance. This will be executed sequentially.
        2. A list containing multiple BaseStep instances. These will be executed in parallel.
        3. A list containing a single ParallelWithConfig instance.
           This will also be executed in parallel.
        """
        instances = []
        if "parallel" in s_dict.keys():
            for row in s_dict.get("parallel"):
                instance = self._create_instance(row)
                instances.append(instance)
                # save arguments to refer by symbol as a global variables.
                StepArgument._put(row["step"], instance)
        elif "parallel_with_config" in s_dict.keys():
            steps, config = self._split_steps_config(s_dict.get("parallel_with_config"))
            # Memo: ParallelWithConfig is named-tuple.
            parallel = ParallelWithConfig([], config)
            for row in steps:
                instance = self._create_instance(row)
                parallel.steps.append(instance)
                # save arguments to refer by symbol as a global variables.
                StepArgument._put(row["step"], instance)
            instances.append(parallel)
        else:
            instance = self._create_instance(s_dict)
            instances.append(instance)
            # save arguments to refer by symbol as a global variables.
            StepArgument._put(s_dict["step"], instance)

        return instances

    def _create_instance(self, s_dict: dict) -> BaseStep:  # noqa
        """
        Create instance

        Memo: resolve with_vars in this method.
        """
        cls_name = s_dict["class"]
        self._logger.debug("Create %s instance" % cls_name)

        # Create BaseStep instance.
        if ClassUtil().is_custom_cls(cls_name) is True:
            from cliboa.core.factory import CustomInstanceFactory

            instance = CustomInstanceFactory.create(cls_name)
        else:
            cls = globals()[cls_name]
            instance = cls()

        # Set arguments to instance.
        cls_attrs_dict = {}
        if "arguments" in s_dict.keys():
            cls_attrs_dict = s_dict["arguments"]

        values = {}
        if cls_attrs_dict:
            with_vars = cls_attrs_dict.pop("with_vars", {})
            ret = self._replace_arguments(cls_attrs_dict, copy.deepcopy(with_vars))
            for dict_k, dict_v in ret.items():
                Helper.set_property(instance, dict_k, dict_v)
            values.update(ret)

        # Set metadata to instance.
        base_args = ["step", "symbol", "parallel"]
        for arg in base_args:
            Helper.set_property(instance, arg, s_dict.get(arg))
        self._append_listeners(instance, s_dict.get("listeners"), values)

        return instance

    def _replace_arguments(self, arguments, with_vars):
        """
        Nested replacement of argments.

        First args:
            arguments (dict): Arguments of steps
            with_vars (dict): with_vars parameter

        Returns:
            dict: Dictionary of arguments which was replaced.
        """
        if isinstance(arguments, dict):
            return {
                dict_k: self._replace_arguments(dict_v, with_vars)
                for dict_k, dict_v in arguments.items()
            }
        elif isinstance(arguments, list):
            return [self._replace_arguments(list_v, with_vars) for list_v in arguments]
        elif isinstance(arguments, str):
            matches = re.compile(r"{{(.*?)}}").findall(arguments)
            for match in matches:
                var_name = match.strip()
                if not var_name:
                    raise InvalidParameter("Alternative argument was empty.")
                if var_name.startswith("env."):
                    arguments = self._replace_envs(arguments, var_name)
                else:
                    cmd = with_vars[var_name]
                    if not cmd:
                        raise ScenarioFileInvalid(
                            "scenario file is invalid."
                            " 'with_vars' definition against %s does not exist." % var_name
                        )
                    arguments = self._replace_vars(arguments, var_name, cmd)
            return arguments
        else:
            return arguments

    def _replace_vars(self, yaml_v, var_name, cmd):
        """
        This method replaces the value of {{ xxx }} based on the result of the shell script.

        Ex.
          -- IN --
          yaml_v: /resources/{{ yyyyMMdd }}
          var_name: yyyyMMdd
          cmd: date '+%Y%m%d'

          -- OUT --
          /resources/20210101

        Args:
            yaml_v: Yaml value. Must contain {{ xxx }}
            var_name: Name of xxx
            cmd: Shell script

        Returns:
            str: replaced value
        """
        shell_output = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, shell=True
        ).communicate()[  # nosec
            0
        ]
        shell_output = shell_output.strip()
        # remove head byte string
        shell_output = re.sub("^b", "", str(shell_output))
        # remove '
        shell_output = re.sub("'", "", str(shell_output))

        return re.sub(r"{{(\s?)%s(\s?)}}" % var_name, shell_output, yaml_v)

    def _replace_envs(self, yaml_v, var_name):
        """
        This method replaces the value of {{ env.xxx }} from system environment values.
        If values are not found in environment, key error will be raised.

        Args:
            yaml_v: Yaml value. Must contain {{ env.xxx }}
            var_name: Name of env.xxx

        Returns:
            str: replaced value
        """
        env_value = os.environ[var_name[4:]]
        return re.sub(r"{{(\s?)%s(\s?)}}" % var_name, env_value, yaml_v)

    def _split_steps_config(self, block) -> Tuple[list, dict]:
        """
        If "config" exist in arguments of parallel_with_config,
        split into two(list of steps and config)

        Args:
            arguments (dict): List of steps

        Returns:
            tuple: (steps, config parameter)
        """
        exists_config = "config" in block.keys()
        config = {}
        if exists_config:
            variables = block["config"]
            for k, v in variables.items():
                config[k] = v
        steps = []
        if "steps" in block.keys():
            steps = block["steps"]
        return steps, config

    def _append_listeners(self, instance, args, values):
        listeners = [StepStatusListener()]

        if args is not None:
            from cliboa.core.factory import CustomInstanceFactory

            if type(args) is str:
                clz = CustomInstanceFactory.create(args)
                clz.__dict__.update(values)
                listeners.append(clz)
            elif type(args) is list:
                for arg in args:
                    clz = CustomInstanceFactory.create(arg)
                    clz.__dict__.update(values)
                    listeners.append(CustomInstanceFactory.create(clz))
        Helper.set_property(instance, "listeners", listeners)

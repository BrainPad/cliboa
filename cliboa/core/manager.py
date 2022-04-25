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
import json
import os
import re
import subprocess
from abc import abstractmethod

from cliboa.conf import env
from cliboa.core.file_parser import JsonScenarioParser, YamlScenarioParser
from cliboa.core.listener import StepStatusListener
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import StepQueue
from cliboa.core.validator import ProjectDirectoryExistence, ScenarioFileExistence
from cliboa.scenario import *  # noqa
from cliboa.util.cache import StepArgument
from cliboa.util.class_util import ClassUtil
from cliboa.util.exception import InvalidParameter, ScenarioFileInvalid
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.parallel_with_config import ParallelWithConfig

__all__ = ["YamlScenarioManager", "JsonScenarioManager"]


class ScenarioManager(object):
    """
    Base class which is to create individual instances from scenario files,
    and push them to the executable queue.

    Note : Currently only yaml format scenario file is implemented.
    """

    def __init__(self, cmd_args):
        self._logger = LisboaLog.get_logger(__name__)
        self._cmd_args = cmd_args
        self._pj_dir = os.path.join(env.PROJECT_DIR, cmd_args.project_name)
        self._pj_scenario_dir = os.path.join(
            env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_DIR_NAME
        )
        if cmd_args.format == "yaml":
            self._pj_scenario_file = (
                os.path.join(env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_FILE_NAME)
                + ".yml"
            )
            self._cmn_scenario_file = os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + ".yml"
        else:
            self._pj_scenario_file = (
                os.path.join(env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_FILE_NAME)
                + "."
                + cmd_args.format
            )
            self._cmn_scenario_file = (
                os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + "." + cmd_args.format
            )

    def create_scenario_queue(self):
        # validation
        self._valid_essential_dir()
        self._valid_essential_files()

        scenario_list = self.parse_file()
        if not scenario_list or isinstance(scenario_list, list) is False:
            raise ScenarioFileInvalid("scenario file is invalid.")

        self._logger.info("Start to create scenario queue")
        queue = StepQueue()
        self._add_queue(queue, scenario_list)

        # save queue to static area
        setattr(ScenarioQueue, "step_queue", queue)
        self._logger.info("Finish to invoke scenario")

    @abstractmethod
    def parse_file(self, file):
        """
        Parse file contents to the list object
        """

    def _valid_essential_dir(self):
        """
        Project directory validation
        """
        valid_instance = ProjectDirectoryExistence()
        valid_instance(self._pj_dir)

    def _valid_essential_files(self):
        """
        Scenario file validation
        """
        valid_instance = ScenarioFileExistence()
        valid_instance(self._pj_scenario_file)

    def _add_queue(self, queue, scenario_list):
        """
        Add executable instance to the queue
        """
        for block in scenario_list:
            if "multi_process_count" in block.keys():
                Helper.set_property(queue, "multi_proc_cnt", block.get("multi_process_count"))
            elif "force_continue" in block.keys():
                Helper.set_property(queue, "force_continue", block.get("force_continue"))
            else:
                instance = self._create_executable_instances(block)
                queue.push(instance)

        self._logger.info("Finish to create scenario queue")

    def _create_executable_instances(self, s_dict):
        """
        Create executable instances

        Returns:
            list: Executable instances
        """
        instances = []
        if "parallel" in s_dict.keys():
            for row in s_dict.get("parallel"):
                instance = self._create_instance(row)
                instances.append(instance)
                StepArgument._put(row["step"], instance)
        elif "parallel_with_config" in s_dict.keys():
            steps_config_block = s_dict.get("parallel_with_config")
            steps, config = self._split_steps_config(steps_config_block)
            parallel = ParallelWithConfig([], config)
            for row in steps:
                instance = self._create_instance(row)
                parallel.steps.append(instance)
                StepArgument._put(row["step"], instance)
            instances.append(parallel)
        else:
            instance = self._create_instance(s_dict)
            instances.append(instance)
            StepArgument._put(s_dict["step"], instance)

        return instances

    def _create_instance(self, s_dict):
        """
        Create instance
        """
        cls_name = s_dict["class"]
        self._logger.debug("Create %s instance" % cls_name)

        if ClassUtil().is_custom_cls(cls_name) is True:
            from cliboa.core.factory import CustomInstanceFactory

            instance = CustomInstanceFactory.create(cls_name)
        else:
            cls = globals()[cls_name]
            instance = cls()

        cls_attrs_dict = {}
        if "arguments" in s_dict.keys():
            cls_attrs_dict = s_dict["arguments"]

        values = {}
        if cls_attrs_dict:
            cls_attrs_dict, with_vars = self._split_class_vars(cls_attrs_dict)
            ret = self._set_values(instance, cls_attrs_dict, with_vars)
            values.update(ret)

        base_args = ["step", "symbol", "parallel", "listeners"]
        for arg in base_args:
            if arg == "listeners":
                self._append_listeners(instance, s_dict.get(arg), values)
            else:
                Helper.set_property(instance, arg, s_dict.get(arg))

        Helper.set_property(
            instance,
            "logger",
            LisboaLog.get_logger(instance.__class__.__name__),
        )

        return instance

    def _split_class_vars(self, arguments):
        """
        If "with_vars" exist in arguments of individual steps,
        split into two(arguments except with_vars and with_vars)

        Args:
            arguments (dict): Arguments of steps

        Returns:
            tuple: (step attribute, with_vars parameter)
        """
        exists_with_vars = "with_vars" in arguments.keys()
        with_vars = {}
        if exists_with_vars:
            variables = arguments["with_vars"]
            for yaml_k, yaml_v in variables.items():
                with_vars[yaml_k] = yaml_v
            del arguments["with_vars"]
        return arguments, with_vars

    def _set_values(self, instance, arguments, with_vars):
        """
        Set parameters to the instance.

        Args:
            instance (class): instance
            arguments (dict): Arguments of steps
            with_vars (dict): with_vars parameter

        Returns:
            dict: Dictionary of arguments which was set
        """
        pattern = re.compile(r"{{(.*?)}}")
        values = {}
        for yaml_k, yaml_v in arguments.items():
            js = json.dumps(yaml_v)
            matches = pattern.findall(js)
            for match in matches:
                var_name = match.strip()
                if not var_name:
                    raise InvalidParameter("Alternative argument was empty.")
                if var_name.startswith("env."):
                    js = self._replace_envs(js, var_name)
                else:
                    cmd = with_vars[var_name]
                    if not cmd:
                        raise ScenarioFileInvalid(
                            "scenario file is invalid. 'with_vars' definition against %s does not exist."  # noqa
                            % var_name
                        )
                    js = self._replace_vars(js, var_name, cmd)
                yaml_v = json.loads(js)
            Helper.set_property(instance, yaml_k, yaml_v)
            values[yaml_k] = yaml_v
        return values

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
        shell_output = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0]
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

    def _split_steps_config(self, block):
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


class YamlScenarioManager(ScenarioManager):
    """
    Create instances from yaml format scenario file.
    """

    def parse_file(self):
        """
        Parse yaml format file to list object
        """
        parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        return parser.parse()


class JsonScenarioManager(ScenarioManager):
    """
    Create instances from json format scenario file.
    """

    def parse_file(self):
        """
        Parse json format file to list object
        """
        parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        return parser.parse()

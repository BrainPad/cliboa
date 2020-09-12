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
import re
import subprocess
from abc import abstractmethod

from cliboa.conf import env
from cliboa.core.file_parser import YamlScenarioParser
from cliboa.core.listener import StepStatusListener
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.core.step_queue import StepQueue
from cliboa.core.validator import DIScenarioFormat, ProjectDirectoryExistence, ScenarioFileExistence
from cliboa.scenario import *  # noqa
from cliboa.util.cache import StepArgument
from cliboa.util.exception import ScenarioFileInvalid
from cliboa.util.helper import Helper
from cliboa.util.http import BasicAuth, FormAuth  # noqa
from cliboa.util.lisboa_log import LisboaLog

__all__ = ["YamlScenarioManager", "JsonScenarioManager"]


class ScenarioManager(object):
    """
    Management of scenario defined in files
    """

    # dependency injection keys in scenario.yml
    DI_KEYS = ["auth", "base_auth"]

    def __init__(self, cmd_args):
        self._logger = LisboaLog.get_logger(__name__)
        self._cmd_args = cmd_args
        self._pj_dir = os.path.join(env.PROJECT_DIR, cmd_args.project_name)
        self._pj_scenario_dir = os.path.join(
            env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_DIR_NAME
        )
        if cmd_args.format == "yaml":
            self._pj_scenario_file = (
                os.path.join(
                    env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_FILE_NAME
                )
                + ".yml"
            )
            self._cmn_scenario_file = (
                os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + ".yml"
            )
        else:
            self._pj_scenario_file = (
                os.path.join(
                    env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_FILE_NAME
                )
                + "."
                + cmd_args.format
            )
            self._cmn_scenario_file = (
                os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME)
                + "."
                + cmd_args.format
            )

        # key and var of dinamic variables
        self._dynamic_key_and_val = {}

    @abstractmethod
    def create_scenario_queue(self):
        """
        Create Scenario Queue
        """


class YamlScenarioManager(ScenarioManager):
    """
    Manage scenario with yml format
    """

    def create_scenario_queue(self):
        self._logger.info("Start to create scenario queue")

        # validation
        self.__valid_essential_dir()
        self.__valid_essential_files()

        # parse scenario.yml
        parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        yaml_scenario_list = parser.parse()

        if yaml_scenario_list and isinstance(yaml_scenario_list, list):
            self.__invoke_steps(yaml_scenario_list)
        else:
            raise ScenarioFileInvalid("scenario.yml is invalid.")

        self._logger.info("Finish to create scenario queue")

    def __valid_essential_dir(self):
        """
        Project directory validation
        """
        valid_instance = ProjectDirectoryExistence()
        valid_instance(self._pj_dir)

    def __valid_essential_files(self):
        """
        Scenairo file validation
        """
        valid_instance = ScenarioFileExistence()
        valid_instance(self._pj_scenario_file)

    def __invoke_steps(self, yaml_scenario_list):
        """
        Create executable instance and push them to queue
        Args:
            yaml_scenario_list: parsed yaml list
        """
        self._logger.info("Start to invoke scenario")

        # Create queue to save step instances
        q = StepQueue()

        for s_dict in yaml_scenario_list:
            if "multi_process_count" in s_dict.keys():
                q.multi_proc_cnt = s_dict.get("multi_process_count")
                continue

            instances = []
            if "parallel" in s_dict.keys():
                for row in s_dict.get("parallel"):
                    instance = self.__create_instance(row, yaml_scenario_list)
                    Helper.set_property(
                        instance,
                        "logger",
                        LisboaLog.get_logger(instance.__class__.__name__),
                    )

                    instances.append(instance)
                    StepArgument._put(row["step"], instance)
            else:
                instance = self.__create_instance(s_dict, yaml_scenario_list)
                Helper.set_property(
                    instance,
                    "logger",
                    LisboaLog.get_logger(instance.__class__.__name__),
                )
                instances.append(instance)
                StepArgument._put(s_dict["step"], instance)

            # Put instance to queue
            q.push(instances)

        # save queue to static area
        setattr(ScenarioQueue, "step_queue", q)
        self._logger.info("Finish to invoke scenario")

    def __create_instance(self, s_dict, yaml_scenario_list):
        cls_name = s_dict["class"]
        self._logger.debug("Create %s instance" % cls_name)

        if self.__is_custom_cls(cls_name) is True:
            from cliboa.core.factory import CustomInstanceFactory

            instance = CustomInstanceFactory.create(cls_name)
        else:
            cls = globals()[cls_name]
            instance = cls()

        base_args = ["step", "symbol", "parallel", "io", "listeners"]
        for arg in base_args:
            if arg == "listeners":
                self._append_listeners(instance, s_dict.get(arg))
            else:
                Helper.set_property(instance, arg, s_dict.get(arg))

        cls_attrs_dict = {}
        if isinstance(yaml_scenario_list, list) and "arguments" in s_dict.keys():
            cls_attrs_dict = s_dict["arguments"]

        # loop and set class attribute
        di_key = None
        di_instance = None
        if cls_attrs_dict:
            cls_attrs_dict = self.__extract_with_vars(cls_attrs_dict)

            # check if the keys of dependency injection
            di_keys, di_instances = self.__create_di_instance(cls_attrs_dict)
            if di_keys and di_instances:
                di_keys_and_instances = zip(di_keys, di_instances)
                for di_key, di_instance in di_keys_and_instances:
                    self._logger.debug(
                        "Inject %s to %s object." % (di_instance, instance)
                    )
                    # Injection
                    setattr(instance, di_key, di_instance)
                    del cls_attrs_dict[di_key]

            pattern = re.compile(r"{{(.*?)}}")
            for yaml_k, yaml_v in cls_attrs_dict.items():
                # if value includes {{ var }}, replace value specified by with_vars
                if isinstance(yaml_v, str):
                    matches = pattern.findall(yaml_v)
                    for match in matches:
                        var_name = match.strip()
                        yaml_v = self.__replace_vars(yaml_v, var_name)

                Helper.set_property(instance, yaml_k, yaml_v)

        return instance

    def __is_custom_cls(self, cls_name):
        """
        The specified class in scenario.yml is a custom step class or not
        Args:
            cls_name: the specified step class in scenario.yml
        Return:
            True: custom step class
            False: default step class
        """
        custom_classes = env.COMMON_CUSTOM_CLASSES + env.PROJECT_CUSTOM_CLASSES
        for cc in custom_classes:
            split_cc = cc.split(".")
            custom_cls_name = split_cc[1]
            if cls_name == custom_cls_name:
                return True
        return False

    def __extract_with_vars(self, cls_attrs_dict):
        """
        If 'with_vars' exist in scenario.yml, extract and save to dictionary.
        After that, remove from list
        """
        # Extract with_vars if exists
        exists_with_vars = "with_vars" in cls_attrs_dict.keys()
        if exists_with_vars:
            variables = cls_attrs_dict["with_vars"]
            for yaml_k, yaml_v in variables.items():
                self._dynamic_key_and_val[yaml_k] = yaml_v
            del cls_attrs_dict["with_vars"]
        return cls_attrs_dict

    def __replace_vars(self, yaml_v, var_name):
        """
        Replace {{ var }} in string
        Args:
            yaml_v: replace target value
            var_name: means {{ var }} itself
        """
        cmd = self._dynamic_key_and_val[var_name]
        if not cmd:
            raise ScenarioFileInvalid(
                "scenario.yml is invalid. 'with_vars' definition against %s does not exist."  # noqa
                % var_name
            )
        shell_output = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, shell=True
        ).communicate()[0]
        shell_output = shell_output.strip()
        # remove head byte string
        shell_output = re.sub("^b", "", str(shell_output))
        # remove '
        shell_output = re.sub("'", "", str(shell_output))
        return re.sub(r"{{(.*?)%s(.*?)}}" % var_name, shell_output, yaml_v)

    def __create_di_instance(self, cls_attrs):
        """
        Create an instance to be injected
        Args:
            step class attributes
        Retrurn:
            DI attribute names, DI instances
        """
        di_keys = []
        di_instance = None
        di_instances = []
        di_params = None
        for k in cls_attrs.keys():
            if k in self.DI_KEYS:
                di_keys.append(k)
                di_params = cls_attrs.get(k)
                valid = DIScenarioFormat(k, di_params)
                valid()
                di_cls = di_params["class"]
                di_cls = globals()[di_cls]
                di_instance = di_cls()
                if di_instance:
                    self._logger.debug(
                        "An instance %s to be injected exists." % di_instance
                    )
                    del di_params["class"]

                # set attributes to instance
                if di_params:
                    for k, v in di_params.items():
                        Helper.set_property(di_instance, k, v)
                di_instances.append(di_instance)
        return di_keys, di_instances

    def _append_listeners(self, instance, args):
        listeners = [StepStatusListener()]

        if args is not None:
            from cliboa.core.factory import CustomInstanceFactory

            if type(args) is str:
                listeners.append(CustomInstanceFactory.create(args))
            elif type(args) is list:
                for arg in args:
                    listeners.append(CustomInstanceFactory.create(arg))
        Helper.set_property(instance, "listeners", listeners)


class JsonScenarioManager(ScenarioManager):
    """
    Manage scenario with json format
    """

    def create_scenario_queue(self):
        """
        TODO: implement in the future
        """

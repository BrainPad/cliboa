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
import argparse
import logging.config
import os
import sys

from cliboa.conf import env
from cliboa.core.factory import ScenarioManagerFactory
from cliboa.core.listener import ScenarioStatusListener
from cliboa.core.worker import ScenarioWorker
from cliboa.util.lisboa_log import LisboaLog


class ScenarioRunner(object):
    """
    Senario Runner
    """

    def __init__(self, cmd_args):
        """
        Set project directory, scenario file path,
        scenario file format, other command line arguments
        """
        logging.config.fileConfig(
            env.BASE_DIR + "/conf/logging.conf", disable_existing_loggers=False
        )
        self._logger = LisboaLog.get_logger(__name__)
        self._pj_scenario_dir = os.path.join(
            env.PROJECT_DIR, cmd_args.project_name, env.SCENARIO_DIR_NAME
        )
        self._cmd_args = cmd_args

    def add_system_path(self):
        """
        Add system path
        """
        add_target_paths = env.SYSTEM_APPEND_PATHS
        add_target_paths.append(self._pj_scenario_dir)
        for path in add_target_paths:
            sys.path.append(path)

    def create_scenario_queue(self):
        """
        Create scenario queue
        """
        manager = ScenarioManagerFactory.create(self._cmd_args)
        manager.create_scenario_queue()

    def execute_scenario(self):
        """
        Execute scenario
        """
        worker = ScenarioWorker(self._cmd_args)
        worker.regist_listeners(ScenarioStatusListener())
        return worker.execute_scenario()


class CommandArgumentParser:
    """
    Parse command line arguments
    """

    def parse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "project_name", default="hoge", help="Specify execution target project name"
        )
        parser.add_argument("execute_method_argument", nargs="*", default="")
        parser.add_argument(
            "-f",
            "--format",
            nargs="?",
            default="yaml",
            help="Specify yaml or json as FORMAT. Default foramt is yaml",
        )
        args = parser.parse_args()
        return args


def run():
    cmd_parser = CommandArgumentParser()
    cmd_args = cmd_parser.parse()
    runner = ScenarioRunner(cmd_args)
    runner.add_system_path()
    runner.create_scenario_queue()
    return runner.execute_scenario()

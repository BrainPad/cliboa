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
import argparse
import logging
import logging.config
import os
import sys
from typing import Tuple

from cliboa.conf import env
from cliboa.core.manager import ScenarioManager
from cliboa.core.model import CommandArgument
from cliboa.util.exception import InvalidFormat
from cliboa.util.log import CliboaLogRecord


def _parse_args():
    """
    Parse command line arguments
    """
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


def _add_system_path(project_name: str):
    """
    Deprecated: This is provided for v2 backward compatibility, but is no longer supported.

    Adds project dir to system path for easy loading scenario classes.

    Depends on envs: PROJECT_DIR, SCENARIO_DIR_NAME, SYSTEM_APPEND_PATHS
    """
    pj_scenario_dir = os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_DIR_NAME)
    add_target_paths = env.SYSTEM_APPEND_PATHS
    add_target_paths.append(pj_scenario_dir)
    for path in add_target_paths:
        sys.path.append(path)


def _initialize_cliboa_logging():
    """
    initialize cliboa logging - load logging.conf and set CliboaLogRecord.

    Depends on envs: BASE_DIR
    """
    logging.config.fileConfig(env.BASE_DIR + "/conf/logging.conf", disable_existing_loggers=False)
    logging.setLogRecordFactory(CliboaLogRecord)


def _generate_scenario_path(project_name: str, scenario_format: str) -> Tuple[str, str]:
    """
    generate project's scenario path and common scenario path from project_name and scenario_format.

    Depends on envs: PROJECT_DIR, COMMON_DIR, SCENARIO_FILE_NAME
    """
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

    return pj_scenario_file, cmn_scenario_file


def run(add_system_path: bool = True):
    """
    The primary entry point function for cliboa.

    It's recommended to call this function if you want to use the cliboa application easily.
    TODO: When change the factory logic to creating scenario classes without additional path
          dependence, add_system_path's default value will be False.
    """
    cmd_args = _parse_args()
    if add_system_path:
        _add_system_path(cmd_args.project_name)

    _initialize_cliboa_logging()
    pj, cmn = _generate_scenario_path(cmd_args.project_name, cmd_args.format)

    manager = ScenarioManager(
        pj, cmn, cmd_args.format, CommandArgument(args=cmd_args.execute_method_argument)
    )
    return manager.execute()

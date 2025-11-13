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
from typing import Tuple

from cliboa.conf import env
from cliboa.core.manager import ScenarioManager
from cliboa.core.model import CommandArgument
from cliboa.util.base import _warn_deprecated, _warn_removed
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


def _initialize_cliboa_logging():
    """
    initialize cliboa logging - load logging.conf and set CliboaLogRecord.
    """
    config_dict = env.get("LOGGING_CONFIG_DICT")
    if config_dict:
        logging.config.dictConfig(config_dict)
    else:
        base_dir = env.get("BASE_DIR")
        config_path = env.get(
            "LOGGING_CONFIG_PATH",
            os.path.join(base_dir, "conf", "logging.conf") if base_dir is not None else None,
        )
        if config_path:
            logging.config.fileConfig(config_path, disable_existing_loggers=False)
    logging.setLogRecordFactory(CliboaLogRecord)


def _generate_scenario_path(project_name: str, scenario_format: str) -> Tuple[str, str]:
    """
    generate project's scenario path and common scenario path from project_name and scenario_format.
    """
    if scenario_format == "yaml":
        ext = env.get("SCENARIO_YAML_EXT", ".yml")
        pj_scenario_file = os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_FILE_NAME) + ext
        cmn_scenario_file = os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + ext
    elif scenario_format == "json":
        ext = "." + scenario_format
        pj_scenario_file = os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_FILE_NAME) + ext
        cmn_scenario_file = os.path.join(env.COMMON_DIR, env.SCENARIO_FILE_NAME) + ext
    else:
        raise InvalidFormat(f"scenario format '{scenario_format}' is invalid.")

    return pj_scenario_file, cmn_scenario_file


def _warn_backward_compatibility():
    if env.get("SCENARIO_DIR_NAME"):
        _warn_deprecated(
            "{CLIBOA_ENV}.SCENARIO_DIR_NAME",
            "{CLIBOA_ENV}.PROJECT_SCENARIO_DIR_NAME",
            end_version="3.0",
        )
    if env.get("SYSTEM_APPEND_PATHS"):
        _warn_removed("{CLIBOA_ENV}.SYSTEM_APPEND_PATHS", end_version="3.0")
    base_dir = env.get("BASE_DIR")
    if base_dir and os.path.exists(os.path.join(base_dir, "conf", "cliboa.ini")):
        _warn_removed("configuration in cliboa.ini", end_version="3.0")


def run():
    """
    The primary entry point function for cliboa.

    It's recommended to call this function if you want to use the cliboa application easily.
    """
    _warn_backward_compatibility()
    _initialize_cliboa_logging()
    cmd_args = _parse_args()

    pj, cmn = _generate_scenario_path(cmd_args.project_name, cmd_args.format)
    manager = ScenarioManager(
        pj,
        cmn,
        cmd_args.format,
        CommandArgument(args=cmd_args.execute_method_argument),
        project_name=cmd_args.project_name,
    )
    return manager.execute()

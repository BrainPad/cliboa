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
import os
import sys

from cliboa.conf import env
from cliboa.core.runner import ScenarioRunner


def _parse():
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
    Deprecated: This is provided for v2 compatibility, but is deprecated.

    Adds project dir to system path for easy loading scenario classes.
    """
    pj_scenario_dir = os.path.join(env.PROJECT_DIR, project_name, env.SCENARIO_DIR_NAME)
    add_target_paths = env.SYSTEM_APPEND_PATHS
    add_target_paths.append(pj_scenario_dir)
    for path in add_target_paths:
        sys.path.append(path)


def run(add_system_path: bool = True):
    """
    The primary entry point function for cliboa.

    It's recommended to call this function if you want to use the cliboa application easily.
    TODO: When change the factory logic to creating scenario classes without additional path
          dependence, add_system_path's default value will be False.
    """
    cmd_args = _parse()
    if add_system_path:
        _add_system_path(cmd_args.project_name)

    runner = ScenarioRunner(
        cmd_args.project_name, cmd_args.format, cmd_args.execute_method_argument
    )
    return runner.execute()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
from importlib import import_module
import os
import sys
from shutil import copyfile
import site


class CliboAdmin(object):
    """
    cliboa administrator command
    """

    def __init__(self, args):
        self.__args = args
        self.__cmn_dir = None
        self.__bin_dir = None

    def main(self):
        """
        Usage:
            cliboadmin init $directory_name
            cliboadmin create $project_name
        """
        if self.__args.option == "init":
            self.__init_pj(self.__args.dir_name)
            print(
                "Initialization of cliboa project '"
                + self.__args.dir_name
                + "' was successful."
            )
        elif self.__args.option == "create":
            self.__create_new_pj(self.__args.dir_name)
            print(
                "Adding a new project '" + self.__args.dir_name + "' was successfule."
            )

    def __init_pj(self, ini_dir):
        """
        Initialize program configuration
        """
        self.__create_ess_dirs(ini_dir)
        self.__create_ess_files(ini_dir)

    def __create_ess_dirs(self, ini_dir):
        """
        create essential directories
        """
        os.makedirs(ini_dir, exist_ok=False)
        self.__bin_dir = os.path.join(ini_dir, "bin")
        os.makedirs(self.__bin_dir, exist_ok=False)
        self.__cmn_dir = os.path.join(ini_dir, "common")
        os.makedirs(self.__cmn_dir, exist_ok=False)
        os.makedirs(os.path.join(self.__cmn_dir, "scenario"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "conf"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "logs"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "project"), exist_ok=False)

    def __create_ess_files(self, ini_dir):
        """
        create essential files
        """
        lisboa_install_paths = site.getsitepackages()
        lisboa_install_path = (
            lisboa_install_paths[0]
            if os.path.exists(os.path.join(lisboa_install_paths[0], "cliboa"))
            else lisboa_install_paths[1]
        )

        run_cmd_path = os.path.join(
            lisboa_install_path, "cliboa", "template", "bin", "clibomanager.py"
        )
        copyfile(run_cmd_path, os.path.join(self.__bin_dir, "clibomanager.py"))

        requirements_path = os.path.join(
            lisboa_install_path, "cliboa/template", "requirements.txt"
        )
        copyfile(requirements_path, os.path.join(ini_dir, "requirements.txt"))

        cmn_env_path = os.path.join(
            lisboa_install_path, "cliboa", "conf", "default_environment.py"
        )
        copyfile(cmn_env_path, os.path.join(self.__cmn_dir, "environment.py"))

        cmn_scenario_path = os.path.join(ini_dir, "common", "scenario.yml")
        with open(cmn_scenario_path, "w") as yaml:
            yaml.write("scenario:" + "\n")

        cmn_ini_path = os.path.join(ini_dir, "common", "__init__.py")
        open(cmn_ini_path, "w").close()

    def __create_new_pj(self, new_pj_dir):
        """
        Create an individual project configuration
        """
        # check if being under lisboa project directory
        sys.path.append(os.getcwd())
        env = import_module("common.environment")

        # make essential directories and files
        os.makedirs(os.path.join("project", new_pj_dir), exist_ok=False)
        os.makedirs(os.path.join("project", new_pj_dir, "scenario"), exist_ok=False)
        with open(os.path.join("project", new_pj_dir, "scenario.yml"), "w") as yaml:
            yaml.write("scenario:" + "\n")


class CommandArgumentParser(object):
    def parse(self):
        """
        Parse cliboadmin arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("option", choices=["init", "create"], help="init or create")
        parser.add_argument(
            "dir_name", help="init $directory_name or create $project_directory_name"
        )
        return parser.parse_args()


def main():
    parser = CommandArgumentParser()
    args = parser.parse()
    admin = CliboAdmin(args)
    admin.main()


if __name__ == "__main__":
    main()

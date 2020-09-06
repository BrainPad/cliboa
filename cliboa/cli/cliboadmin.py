#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
import site
import sys
from importlib import import_module
from shutil import copyfile


class CliboAdmin(object):
    """
    cliboa administrator command
    """

    def __init__(self, args):
        self._args = args
        self._cmn_dir = None
        self._bin_dir = None

    def main(self):
        """
        Usage:
            cliboadmin init $directory_name
            cliboadmin create $project_name
        """
        if self._args.option == "init":
            self._init_pj(self._args.dir_name)
            print(
                "Initialization of cliboa project '"
                + self._args.dir_name
                + "' was successful."
            )
        elif self._args.option == "create":
            self._create_new_pj(self._args.dir_name)
            print("Adding a new project '" + self._args.dir_name + "' was successfule.")

    def _init_pj(self, ini_dir):
        """
        Initialize program configuration
        """
        self._create_ess_dirs(ini_dir)
        self._create_ess_files(ini_dir)

    def _create_ess_dirs(self, ini_dir):
        """
        create essential directories
        """
        os.makedirs(ini_dir, exist_ok=False)
        self._bin_dir = os.path.join(ini_dir, "bin")
        os.makedirs(self._bin_dir, exist_ok=False)
        self._cmn_dir = os.path.join(ini_dir, "common")
        os.makedirs(self._cmn_dir, exist_ok=False)
        os.makedirs(os.path.join(self._cmn_dir, "scenario"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "cliboa/conf"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "conf"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "logs"), exist_ok=False)
        os.makedirs(os.path.join(ini_dir, "project"), exist_ok=False)

    def _create_ess_files(self, ini_dir):
        """
        create essential files
        """
        cliboa_install_paths = site.getsitepackages()
        cliboa_install_path = (
            cliboa_install_paths[0]
            if os.path.exists(os.path.join(cliboa_install_paths[0], "cliboa"))
            else cliboa_install_paths[1]
        )

        run_cmd_path = os.path.join(
            cliboa_install_path, "cliboa", "template", "bin", "clibomanager.py"
        )
        copyfile(run_cmd_path, os.path.join(self._bin_dir, "clibomanager.py"))

        # copy Pipfile
        pipfile_path = self._get_pipfile_path(cliboa_install_path) 
        copyfile(pipfile_path, os.path.join(ini_dir, "Pipfile"))

        # copy environment.py
        cmn_env_path = os.path.join(
            cliboa_install_path, "cliboa", "conf", "default_environment.py"
        )
        copyfile(cmn_env_path, os.path.join(self._cmn_dir, "environment.py"))

        # copy logging.conf
        conf_path = os.path.join(cliboa_install_path, "cliboa", "conf", "logging.conf")
        copyfile(conf_path, os.path.join(ini_dir, "conf", "logging.conf"))

        # copy cliboa.ini
        conf_path = os.path.join(cliboa_install_path, "cliboa", "conf", "cliboa.ini")
        copyfile(conf_path, os.path.join(ini_dir, "conf", "cliboa.ini"))

        # create __init__.py
        cmn_ini_path = os.path.join(ini_dir, "common", "__init__.py")
        open(cmn_ini_path, "w").close()

    def _create_new_pj(self, new_pj_dir):
        """
        Create an individual project configuration
        """
        # check if being under cliboa project directory
        sys.path.append(os.getcwd())
        import_module("common.environment")

        # make essential directories and files
        os.makedirs(os.path.join("project", new_pj_dir), exist_ok=False)
        os.makedirs(os.path.join("project", new_pj_dir, "scenario"), exist_ok=False)
        with open(os.path.join("project", new_pj_dir, "scenario.yml"), "w") as yaml:
            yaml.write("scenario:" + "\n")

    def _get_pipfile_path(self, cliboa_install_path):
        """
        Get path of requirements.txt and Pipfile for current python version
        """
        py_ver_info = sys.version
        py_ver_info = py_ver_info.split(" ")
        py_ver = py_ver_info[0].split(".")
        py_major_ver = py_ver[0] + "." + py_ver[1]
        py_major_ver_and_pipfile = {
            "3.5": "Pipfile.above35",
            "3.6": "Pipfile.above36",
            "3.7": "Pipfile.above37",
        }
        pipfile_path = os.path.join(
            cliboa_install_path,
            "cliboa/template",
            py_major_ver_and_pipfile[py_major_ver],
        )
        return pipfile_path


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

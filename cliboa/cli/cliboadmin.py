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
from pathlib import Path
from shutil import copyfile

import cliboa


class CliboAdmin:
    """
    cliboa administrator command
    """

    def __init__(self, args):
        self._args = args
        self._cwd = Path.cwd().resolve()
        print(f"Working root dir is {self._cwd}")
        self._cliboa_install_path = os.path.dirname(cliboa.__path__[0])

    def main(self):
        """
        Usage:
            cliboadmin init $app_name
            cliboadmin project|pj $project_name
        """
        if self._args.cmd == "init":
            self._init_project(self._args.app)
        elif self._args.cmd in ("project", "pj"):
            if self._args.project:
                self._create_new_project(self._args.project)
            else:
                print("Please specify -p PROJECT_NAME.")

    def _init_project(self, app_name: str | None):
        """
        Initialize program configuration
        """
        self._create_dir("common")
        self._create_dir("project")
        if self._create_dir("conf"):
            self._copy_file(
                self._get_cliboa_path("cli", "template", "logging.conf"),
                self._get_dest_path("conf", "logging.conf"),
            )
            self._create_dir("logs")
        if app_name:
            self._create_dir(app_name)
            dest_path = self._get_dest_path(app_name, "cliboa_run.py")
            self._copy_file(self._get_cliboa_path("cli", "template", "cliboa_run.py"), dest_path)
            self._replace_app_in_file(dest_path, app_name)
            self._copy_file(
                self._get_cliboa_path("conf", "default_environment.py"),
                self._get_dest_path(app_name, "cliboa_environment.py"),
            )

    def _create_dir(self, *args) -> bool:
        target = self._get_dest_path(*args)
        print(f"Create {target}")
        try:
            if os.path.isdir(target):
                print(f"Already exists {target}")
                return True
            os.makedirs(target, exist_ok=False)
            return True
        except Exception as e:
            print(f"Warning: Failed create dir {target}: {e}")
            return False

    def _copy_file(self, src: str, dest: str) -> bool:
        try:
            print(f"Create {dest}")
            if os.path.isfile(dest):
                print(f"Warning: Already exists {dest}")
                return False
            copyfile(src, dest)
            return True
        except Exception as e:
            print(f"Warning: Failed create file {dest}: {e}")
            return False

    def _get_cliboa_path(self, *args):
        return os.path.join(self._cliboa_install_path, "cliboa", *args)

    def _get_dest_path(self, *args):
        return os.path.join(self._cwd, *args)

    def _replace_app_in_file(self, dest_path: str, app_name: str) -> bool:
        """
        Convert "app.cliboa_environment"
        """
        if not os.path.exists(dest_path):
            return False
        original_string = "app.cliboa_environment"
        replacement_string = f"{app_name}.cliboa_environment"
        try:
            with open(dest_path, "r", encoding="utf-8") as f:
                content = f.read()
            new_content = content.replace(original_string, replacement_string)
            if new_content != content:
                with open(dest_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
            return True
        except Exception:
            print(f"Warning: Failed update {dest_path} for {app_name}")
            return False

    def _create_new_project(self, pj_name: str):
        """
        Create an individual project configuration
        """
        if self._create_dir("project", pj_name):
            self._copy_file(
                self._get_cliboa_path("cli", "template", "scenario-sample.yml"),
                self._get_dest_path("project", pj_name, "scenario.yml"),
            )
        self._create_dir("project", pj_name, "scenario")


class CommandArgumentParser:
    def parse(self):
        """
        Parse cliboadmin arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("cmd", choices=["init", "project", "pj"], help="init or project(pj)")
        parser.add_argument("-a", "--app", help="Your application name")
        parser.add_argument("-p", "--project", help="Project name")
        return parser.parse_args()


def main():
    parser = CommandArgumentParser()
    args = parser.parse()
    admin = CliboAdmin(args)
    admin.main()


if __name__ == "__main__":
    main()

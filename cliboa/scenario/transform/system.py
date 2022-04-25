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
import os
import subprocess

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep


class ExecuteShellScript(BaseStep):
    """
    Execute Shell Script
    """

    def __init__(self):
        super().__init__()
        self._command = ""
        self._work_dir = ""

    def command(self, command):
        self._command = command

    def work_dir(self, work_dir):
        self._work_dir = work_dir

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._command],
        )
        valid()

        # Setting Up Directory
        default_dir = os.getcwd()

        try:
            if self._work_dir:
                os.chdir(self._work_dir)

            # Run Commands
            content = self._command.get("content")
            file_path = self._command.get("file")

            if content:
                for command in content.split("&&"):
                    subprocess.run(command.strip().split(" "))
            elif file_path:
                subprocess.call(file_path)
        finally:
            # Set Directory to default
            if self._work_dir:
                os.chdir(default_dir)

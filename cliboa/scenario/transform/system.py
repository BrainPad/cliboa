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

from pydantic import BaseModel

from cliboa.scenario.base import BaseStep


class _CommandDict(BaseModel):
    content: str | None = None
    file: str | None = None


class ExecuteShellScript(BaseStep):
    """
    Execute Shell Script
    """

    class Arguments(BaseModel):
        command: _CommandDict | str
        work_dir: str | None = None

    def execute(self, *args):
        # Setting Up Directory
        default_dir = os.getcwd()

        try:
            if self.args.work_dir:
                os.chdir(self.args.work_dir)

            if isinstance(self.args.command, str):
                content = self.args.command
            else:
                content = self.args.command.content

            if content:
                for command in content.split("&&"):
                    subprocess.run(command.strip().split(" "))
            elif self.args.command.file:
                subprocess.call(self.args.command.file)
        finally:
            # Set Directory to default
            if self.args.work_dir:
                os.chdir(default_dir)

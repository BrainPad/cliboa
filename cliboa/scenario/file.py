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
from pydantic import BaseModel

from cliboa.adapter.file import File
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated_args
from cliboa.util.exception import FileNotFound


class FileRead(BaseStep):
    """
    The parent class to read the specified file
    """

    class Arguments(BaseModel):
        src_dir: str
        src_pattern: str
        encoding: str = "utf-8"
        nonfile_error: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _src_dir(self):
        return self.args.src_dir

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _src_pattern(self):
        return self.args.src_pattern

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _encoding(self):
        return self.args.encoding

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _nonfile_error(self):
        return self.args.nonfile_error

    def get_src_files(self, *args, **kwargs) -> list[str]:
        return self._resolve("adapter_file", File).get_target_files(
            self.args.src_dir, self.args.src_pattern, *args, **kwargs
        )

    def check_file_existence(self, files: list[str]) -> bool:
        """
        Check whether files exist.
        If no files, the scenario will continue or an error is raised,
        depends on parameter[nonfile_error].
        """
        if len(files) == 0:
            if self.args.nonfile_error is True:
                raise FileNotFound("No files are found.")
            else:
                self.logger.info("No files are found. Nothing to do.")
                return False
        self.logger.info("Files found %s" % files)
        return True


class FileWrite(BaseStep):
    """
    Load file to server
    """

    class Arguments(BaseModel):
        dest_dir: str | None = None
        dest_name: str | None = None
        encoding: str = "utf-8"

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dest_dir(self):
        return self.args.dest_dir

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dest_name(self):
        return self.args.dest_name

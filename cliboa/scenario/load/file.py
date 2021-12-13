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
from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.util.constant import StepStatus
from cliboa.util.exception import FileNotFound, InvalidFileCount


class FileWrite(BaseStep):
    """
    Load file to server
    """

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_path = None
        self._encoding = "utf-8"
        self._mode = "a"

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def encoding(self, encoding):
        self._encoding = encoding

    def mode(self, mode):
        self._mode = mode


class FileExists(BaseStep):
    """
    Check local files.
    """

    DEFAULT_DL_CNT = 1
    UNLIMITED_DL_CNT = -1

    def __init__(self):
        super().__init__()
        self._src_dir = ""
        self._src_pattern = ""
        self._count = self.DEFAULT_DL_CNT
        self._raise_error = True

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def count(self, count):
        self._count = count

    def raise_error(self, raise_error):
        self._raise_error = raise_error

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)

        if len(files) == 0:
            path = os.path.join(self._src_dir, self._src_pattern)
            if self._raise_error:
                raise FileNotFound("No files are found in the path of %s" % path)
            else:
                return StepStatus.SUCCESSFUL_TERMINATION

        for file in files:
            meta = {
                "path": file,
                "stat": os.stat(file)
            }
            self._logger.info(meta)

        if self._count != self.UNLIMITED_DL_CNT and len(files) != self._count:
            if self._raise_error:
                raise InvalidFileCount(
                    "Files found %s, but expected count was %s" % (len(files), self._count))
            else:
                return StepStatus.SUCCESSFUL_TERMINATION

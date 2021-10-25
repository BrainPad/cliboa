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
from cliboa.scenario.sftp import BaseSftp
from cliboa.util.constant import StepStatus
from cliboa.util.sftp import Sftp


class SftpBaseLoad(BaseSftp):
    def __init__(self):
        super().__init__()


class SftpUpload(SftpBaseLoad):
    """
    Upload file to sftp server
    """

    def __init__(self):
        super().__init__()
        self._quit = False
        self._ignore_empty_file = False

    def quit(self, quit):
        self._quit = quit

    def ignore_empty_file(self, ignore_empty_file):
        self._ignore_empty_file = ignore_empty_file

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._user, self._src_dir, self._src_pattern, self._dest_dir],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        adaptor = super().get_adaptor()
        if len(files) > 0:
            for file in files:
                if self._ignore_empty_file and os.path.getsize(file) == 0:
                    self._logger.info("0 byte file will no be uploaded %s." % file)
                    continue

                obj = Sftp().put_file(
                    src=file,
                    dest=os.path.join(self._dest_dir, os.path.basename(file)),
                    endfile_suffix=self._endfile_suffix,
                )
                adaptor.execute(obj)
                self._logger.info("%s is successfully uploaded." % file)
        else:
            self._logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self._src_dir, self._src_pattern)
                )
            )
            if self._quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION

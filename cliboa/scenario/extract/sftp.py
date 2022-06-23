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
import re

from cliboa.scenario.sftp import BaseSftp
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus
from cliboa.util.sftp import Sftp


class SftpExtract(BaseSftp):
    def __init__(self):
        super().__init__()


class SftpDownload(SftpExtract):
    """
    Download files from sftp server
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
            [self._host, self._user, self._src_dir, self._src_pattern],
        )
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        obj = Sftp().list_files(
            dir=self._src_dir,
            dest=self._dest_dir,
            pattern=re.compile(self._src_pattern),
            endfile_suffix=self._endfile_suffix,
            ignore_empty_file=self._ignore_empty_file,
        )

        adaptor = super().get_adaptor()
        files = adaptor.execute(obj)

        if self._quit is True and len(files) == 0:
            self._logger.info("No file was found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        self._logger.info("Files downloaded %s" % files)

        # cache downloaded file names
        ObjectStore.put(self._step, files)


class SftpDelete(SftpExtract):
    """
    Delete file from sftp server
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._user, self._src_dir, self._src_pattern],
        )
        valid()

        obj = Sftp().clear_files(
            dir=self._src_dir,
            pattern=re.compile(self._src_pattern),
        )

        adaptor = super().get_adaptor()
        adaptor.execute(obj)


class SftpDownloadFileDelete(SftpExtract):
    """
    Delete all downloaded files.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        files = ObjectStore.get(self._symbol)

        if files is not None and len(files) > 0:
            self._logger.info("Delete files %s" % files)

            self._host = super().get_step_argument("host")
            self._user = super().get_step_argument("user")
            self._password = super().get_step_argument("password")
            self._key = super().get_step_argument("key")
            self._timeout = super().get_step_argument("timeout")
            self._retry_count = super().get_step_argument("retry_count")
            self._port = super().get_step_argument("port")
            self._endfile_suffix = super().get_step_argument("endfile_suffix")
            self._src_dir = super().get_step_argument("src_dir")

            adaptor = super().get_adaptor()

            endfile_suffix = super().get_step_argument("endfile_suffix")
            for file in files:
                obj = Sftp().remove_specific_file(
                    dir=self._src_dir,
                    fname=file,
                )
                adaptor.execute(obj)
                self._logger.info("%s is successfully deleted." % file)

                if endfile_suffix:
                    obj = Sftp().remove_specific_file(
                        dir=self._src_dir,
                        fname=file + endfile_suffix,
                    )
                    self._logger.info("%s is successfully deleted." % (file + endfile_suffix))
        else:
            self._logger.info("No files to delete.")


class SftpFileExistsCheck(SftpExtract):
    """
    File check in sftp server
    """

    def __init__(self):
        super().__init__()
        self._ignore_empty_file = False

    def ignore_empty_file(self, ignore_empty_file):
        self._ignore_empty_file = ignore_empty_file

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._user, self._src_dir, self._src_pattern],
        )
        valid()

        obj = Sftp().file_exists_check(
            dir=self._src_dir,
            pattern=re.compile(self._src_pattern),
            ignore_empty_file=self._ignore_empty_file,
        )

        adaptor = super().get_adaptor()
        files = adaptor.execute(obj)

        if len(files) == 0:
            self._logger.info("File not found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        self._logger.info("File was found. After process will be processed")

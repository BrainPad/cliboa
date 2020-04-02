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
import os
import re

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus
from cliboa.util.ftp_util import FtpUtil


class FtpExtract(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = ""
        self._host = None
        self._port = 21
        self._user = None
        self._password = None
        self._timeout = 30
        self._retry_count = 3
        self._tls = False

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def host(self, host):
        self._host = host

    def port(self, port):
        self._port = port

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count

    def tls(self, tls):
        self._tls = tls


class FtpDownload(FtpExtract):
    """
    Download files from ftp server
    """

    def __init__(self):
        super().__init__()
        self._quit = False

    def quit(self, quit):
        self._quit = quit

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._user, self._src_dir, self._src_pattern],
        )
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        # fetch src
        ftp_util = FtpUtil(
            self._host,
            self._user,
            self._password,
            self._timeout,
            self._retry_count,
            self._port,
            self._tls,
        )
        files = ftp_util.list_files(
            self._src_dir, self._dest_dir, re.compile(self._src_pattern)
        )

        if self._quit is True and len(files) == 0:
            self._logger.info("No file was found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        # cache downloaded file names
        ObjectStore.put(self._step, files)


class FtpDownloadFileDelete(FtpExtract):
    """
    Delete all downloaded files.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        files = ObjectStore.get(self._symbol)

        if files is not None and len(files) > 0:
            self._logger.info("Delete files %s" % files)

            ftp_util = FtpUtil(
                super().get_step_argument("host"),
                super().get_step_argument("user"),
                super().get_step_argument("password"),
                super().get_step_argument("timeout"),
                super().get_step_argument("retry_count"),
                super().get_step_argument("port"),
                super().get_step_argument("tls"),
            )
            for file in files:
                ftp_util.remove_specific_file(
                    super().get_step_argument("src_dir"), file
                )
        else:
            self._logger.info("No files to delete.")

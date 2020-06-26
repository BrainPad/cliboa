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

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.util.constant import StepStatus
from cliboa.util.sftp import Sftp


class SftpBaseLoad(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_dir = ""
        self._src_pattern = ""
        self._dest_dir = ""
        self._host = None
        self._port = 22
        self._user = None
        self._password = None
        self._key = None
        self._timeout = 30
        self._retry_count = 3

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

    def key(self, key):
        self._key = key

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count


# deprecated
class SftpFileLoad(SftpBaseLoad):
    """
    Upload file to sftp server
    """

    def __init__(self):
        SftpBaseLoad.__init__(self)

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._user, self._src_dir, self._src_pattern, self._dest_dir],
        )
        valid()

        sftp = Sftp(
            self._host,
            self._user,
            self._password,
            self._key,
            self._timeout,
            self._retry_count,
            self._port,
        )

        files = super().get_target_files(self._src_dir, self._src_pattern)
        for file in files:
            sftp.put_file(file, os.path.join(self._dest_dir, os.path.basename(file)))


class SftpUpload(SftpBaseLoad):
    """
    Upload file to sftp server
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
            [self._host, self._user, self._src_dir, self._src_pattern, self._dest_dir],
        )
        valid()

        sftp = Sftp(
            self._host,
            self._user,
            self._password,
            self._key,
            self._timeout,
            self._retry_count,
            self._port,
        )
        files = super().get_target_files(self._src_dir, self._src_pattern)

        if len(files) > 0:
            for file in files:
                sftp.put_file(
                    file, os.path.join(self._dest_dir, os.path.basename(file))
                )
                self._logger.info("%s is successfully uploaded." % file)
        else:
            self._logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self._src_dir, self._src_pattern)
                )
            )
            if self._quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION

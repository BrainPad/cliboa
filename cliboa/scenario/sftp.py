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

from cliboa.adapter.sftp import SftpAdapter
from cliboa.scenario.base import BaseStep


class BaseSftp(BaseStep):
    def __init__(self):
        super().__init__()

        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = ""
        self._host = None
        self._port = 22
        self._user = None
        self._password = None
        self._key = None
        self._passphrase = None
        self._endfile_suffix = None
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

    def passphrase(self, passphrase):
        self._passphrase = passphrase

    def endfile_suffix(self, endfile_suffix):
        self._endfile_suffix = endfile_suffix

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count

    def get_adaptor(self):
        if isinstance(self._key, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `key` will be changed to accept only dictionary types. "
                    "Please see more information "
                    "https://github.com/BrainPad/cliboa/blob/master/docs/modules/sftp_download.md"
                )
            )
            key_filepath = self._key
        else:
            key_filepath = self._source_path_reader(self._key)

        return SftpAdapter(
            host=self._host,
            user=self._user,
            password=self._password,
            key=key_filepath,
            passphrase=self._passphrase,
            timeout=self._timeout,
            retryTimes=self._retry_count,
            port=self._port,
        )

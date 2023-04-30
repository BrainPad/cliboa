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
from cliboa.adapter.ftp import FtpAdapter
from cliboa.scenario.base import BaseStep


class BaseFtp(BaseStep):
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

    def get_adaptor(self):
        return FtpAdapter(
            host=self._host,
            user=self._user,
            password=self._password,
            timeout=self._timeout,
            retryTimes=self._retry_count,
            port=self._port,
            tls=self._tls,
        )

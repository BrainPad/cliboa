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

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.http import Download
from requests.auth import HTTPBasicAuth


class HttpExtract(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_url = None
        self._src_pattern = None
        self._dest_dir = None
        self._dest_pattern = None
        self._timeout = 30
        self._retry_count = 3

    def src_url(self, src_url):
        self._src_url = src_url

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def dest_pattern(self, dest_pattern):
        self._dest_pattern = dest_pattern

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_url, self._src_pattern, self._dest_dir]
        )
        valid()


class HttpDownload(HttpExtract):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        super().execute()

        os.makedirs(self._dest_dir, exist_ok=True)

        url = os.path.join(self._src_url, self._src_pattern)
        dest_path = (
            os.path.join(self._dest_dir, self._dest_pattern)
            if self._dest_pattern
            else os.path.join(self._dest_dir, self._src_pattern)
        )

        d = Download(url, dest_path, self._timeout, self._retry_count, **self.get_params())
        d.execute()

    def get_params(self):
        # This parameter will be passed to **kwargs when calling the request.
        # Please implement in subclasses if necessary.
        return {}


class HttpDownloadViaBasicAuth(HttpDownload):
    def __init__(self):
        super().__init__()
        self._user = None
        self._password = None

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__, [self._user, self._password]
        )
        valid()

        super().execute()

    def get_params(self):
        return {"auth": HTTPBasicAuth(self._user, self._password)}

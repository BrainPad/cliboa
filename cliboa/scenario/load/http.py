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

from cliboa.adapter.http import Remove, Update, Upload
from cliboa.scenario.http import HttpBase
from cliboa.scenario.validator import EssentialParameters


class HttpPost(HttpBase):
    def __init__(self):
        super().__init__()
        self._payload = {}
        self._headers = {"accept": "application/json", "Content-Type": "application/json"}

    def payload(self, payload):
        self._payload = payload

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        u = Upload(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            params={
                "data": self._payload,
                "headers": self._headers,
            },
        )
        u.execute()


class HttpPut(HttpBase):
    def __init__(self):
        super().__init__()
        self._payload = {}
        self._headers = {"accept": "application/json", "Content-Type": "application/json"}

    def payload(self, payload):
        self._payload = payload

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        u = Update(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            params={
                "data": self._payload,
                "headers": self._headers,
            },
        )
        u.execute()


class HttpDelete(HttpBase):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        r = Remove(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            params=self.get_params(),
        )
        r.execute()

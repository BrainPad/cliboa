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
from requests.auth import HTTPBasicAuth

from cliboa.scenario.base import BaseStep


class HttpBase(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_url = None
        self._dest_dir = None
        self._dest_name = None
        self._timeout = 30
        self._retry_count = 3
        self._retry_intvl_sec = 10
        self._basic_auth = False
        self._user = None
        self._password = None

    def src_url(self, src_url):
        self._src_url = src_url

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def dest_name(self, dest_name):
        self._dest_name = dest_name

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count

    def retry_intvl_sec(self, retry_intvl_sec):
        self._retry_intvl_sec = retry_intvl_sec

    def basic_auth(self, basic_auth):
        self._basic_auth = basic_auth

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def execute(self, *args):
        pass

    def get_params(self):
        if self._basic_auth:
            return {"auth": HTTPBasicAuth(self._user, self._password)}
        return {}

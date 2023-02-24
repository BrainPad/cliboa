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
import shutil

import pytest
from requests.exceptions import HTTPError

from cliboa.conf import env
from cliboa.scenario.extract.http import HttpDownload, HttpDownloadViaBasicAuth
from cliboa.test import BaseCliboaTest
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestHttpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir)
            instance = HttpDownload()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com")
            Helper.set_property(instance, "src_pattern", "get?foo1=bar1&foo2=bar2")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.result")
            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert "postman-echo.com" in result


class TestDownloadViaBasicAuth(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpDownloadViaBasicAuth()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com")
            Helper.set_property(instance, "src_pattern", "basic-auth")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.result")
            Helper.set_property(instance, "user", "postman")
            Helper.set_property(instance, "password", "password")

            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert '{\n  "authenticated": true\n}' in result

    def test_execute_ng(self):
        instance = HttpDownloadViaBasicAuth()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://postman-echo.com")
        Helper.set_property(instance, "src_pattern", "basic-auth")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.result")
        Helper.set_property(instance, "user", "postman")
        Helper.set_property(instance, "password", "xxxxx")

        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed. HTTP Status code: 401" in str(execinfo.value)

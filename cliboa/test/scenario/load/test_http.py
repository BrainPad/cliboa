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
from cliboa.scenario.load.http import HttpDelete, HttpPost, HttpPut
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestHttpPost(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpPost()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com/post")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.result")
            Helper.set_property(instance, "payload", {"key": "value"})
            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        instance = HttpPost()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://spam.com/post")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.result")
        Helper.set_property(instance, "payload", {"key": "value"})
        Helper.set_property(instance, "retry_count", 1)
        Helper.set_property(instance, "retry_intvl_sec", 1)
        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed. HTTP Status code: 404" in str(execinfo.value)


class TestHttpPut(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpPut()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com/put")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.result")
            Helper.set_property(instance, "payload", {"key": "value"})
            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        instance = HttpPut()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://spam.com/put")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.result")
        Helper.set_property(instance, "payload", {"key": "value"})
        Helper.set_property(instance, "retry_count", 1)
        Helper.set_property(instance, "retry_intvl_sec", 1)
        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed. HTTP Status code: 404" in str(execinfo.value)


class TestHttpDelete(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpDelete()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com/delete")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.result")
            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        instance = HttpDelete()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://spam.com/delete")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.result")
        Helper.set_property(instance, "retry_count", 1)
        Helper.set_property(instance, "retry_intvl_sec", 1)
        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed. HTTP Status code: 404" in str(execinfo.value)

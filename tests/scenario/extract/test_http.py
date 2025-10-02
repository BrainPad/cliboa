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
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import HTTPError

from cliboa.conf import env
from cliboa.scenario.extract.http import HttpDownload, HttpDownloadViaBasicAuth, HttpGet
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from tests import BaseCliboaTest


class TestHttpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch("requests.get")
    def test_execute_ok(self, mock_get):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = (
            '{"args":{"foo1":"bar1","foo2":"bar2"},"headers":{"host":"postman-echo.com"}}'
        )
        mock_response.content = (
            b'{"args":{"foo1":"bar1","foo2":"bar2"},"headers":{"host":"postman-echo.com"}}'
        )
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        try:
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
        mock_get.assert_called_once()


class TestDownloadViaBasicAuth(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch("requests.get")
    def test_execute_ok(self, mock_get):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{\n  "authenticated": true\n}'
        mock_response.content = b'{\n  "authenticated": true\n}'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

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
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_execute_ng(self, mock_get):
        # Mock HTTP error
        mock_get.side_effect = HTTPError("Http request failed. HTTP Status code: 401")

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
        assert "Http request failed" in str(execinfo.value)


class TestHttpGet(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch("requests.get")
    def test_execute_ok_1(self, mock_get):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = (
            '{"args":{"foo1":"bar1","foo2":"bar2"},"headers":{"host":"postman-echo.com"}}'
        )
        mock_response.content = (
            b'{"args":{"foo1":"bar1","foo2":"bar2"},"headers":{"host":"postman-echo.com"}}'
        )
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpGet()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(
                instance, "src_url", "https://postman-echo.com/get?foo1=bar1&foo2=bar2"
            )
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.result")
            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert "postman-echo.com" in result
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_execute_ok_2(self, mock_get):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{\n  "authenticated": true\n}'
        mock_response.content = b'{\n  "authenticated": true\n}'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        try:
            os.makedirs(self._data_dir, exist_ok=True)
            instance = HttpGet()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(instance, "src_url", "https://postman-echo.com/basic-auth")  # noqa
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.result")
            Helper.set_property(instance, "user", "postman")
            Helper.set_property(instance, "password", "password")
            Helper.set_property(instance, "basic_auth", True)

            instance.execute()
            f = open(os.path.join(self._data_dir, "test.result"), "r")
            result = f.read()
            f.close()
        finally:
            shutil.rmtree(self._data_dir)
        assert '{\n  "authenticated": true\n}' in result
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_execute_ng_1(self, mock_get):
        # Mock HTTP error
        mock_get.side_effect = HTTPError("Http request failed. HTTP Status code: 404")

        instance = HttpGet()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://spam.com/get?foo1=bar1&foo2=bar2")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.result")
        Helper.set_property(instance, "retry_count", 1)
        Helper.set_property(instance, "retry_intvl_sec", 1)
        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed" in str(execinfo.value)

    @patch("requests.get")
    def test_execute_ng_2(self, mock_get):
        # Mock HTTP error
        mock_get.side_effect = HTTPError("Http request failed. HTTP Status code: 401")

        instance = HttpGet()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        # use Postman echo
        Helper.set_property(instance, "src_url", "https://postman-echo.com/basic-auth")  # noqa
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.result")
        Helper.set_property(instance, "retry_count", 1)
        Helper.set_property(instance, "retry_intvl_sec", 1)
        Helper.set_property(instance, "user", "postman")
        Helper.set_property(instance, "password", "xxxxx")
        Helper.set_property(instance, "basic_auth", True)

        with pytest.raises(HTTPError) as execinfo:
            instance.execute()
        assert "Http request failed" in str(execinfo.value)

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
from cliboa.scenario.load.http import HttpDelete, HttpPost, HttpPut
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestHttpPost(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch("cliboa.scenario.load.http.requests.post")
    def test_execute_ok(self, mock_post):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

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
        mock_post.assert_called_once()

    @patch("cliboa.scenario.load.http.requests.post")
    def test_execute_ng(self, mock_post):
        # Mock HTTP error
        mock_post.side_effect = HTTPError("Http request failed. HTTP Status code: 404")

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

    @patch("cliboa.scenario.load.http.requests.put")
    def test_execute_ok(self, mock_put):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_put.return_value = mock_response

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
        mock_put.assert_called_once()

    @patch("cliboa.scenario.load.http.requests.put")
    def test_execute_ng(self, mock_put):
        # Mock HTTP error
        mock_put.side_effect = HTTPError("Http request failed. HTTP Status code: 404")

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

    @patch("cliboa.scenario.load.http.requests.delete")
    def test_execute_ok(self, mock_delete):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_delete.return_value = mock_response

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
        mock_delete.assert_called_once()

    @patch("cliboa.scenario.load.http.requests.delete")
    def test_execute_ng(self, mock_delete):
        # Mock HTTP error
        mock_delete.side_effect = HTTPError("Http request failed. HTTP Status code: 404")

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

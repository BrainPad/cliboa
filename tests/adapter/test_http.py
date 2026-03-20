import os
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import HTTPError

from cliboa.adapter.http import Download, Remove, Update, Upload


class TestHttp(object):

    _DUMMY_URL = "http://spam"
    _DUMMY_ID = 999
    _DUMMY_PASSWORD = "spam"


class TestDownload(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    @patch("cliboa.adapter.http.requests.get")
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

        url = "https://postman-echo.com/get?foo1=bar1&foo2=bar2"
        timeout = 10
        retry_cnt = 3
        d = Download(url, self._dest_path, timeout, retry_cnt)
        d.execute()

        # Verify file was created with mocked content
        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result
        mock_get.assert_called_once()

    @patch("cliboa.adapter.http.requests.get")
    def test_execute_ng(self, mock_get):
        # Mock HTTP error
        mock_get.side_effect = HTTPError("Http request failed.")

        url = "https://spam.com/get"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        d = Download(url, self._dest_path, timeout, retry_cnt, retry_intvl_sec)
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed" in str(execinfo.value)


class TestUpload(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    @patch("cliboa.adapter.http.requests.post")
    def test_execute_ok(self, mock_post):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.content = b'{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        url = "https://postman-echo.com/post"
        timeout = 10
        retry_cnt = 3
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Upload(
            url, self._dest_path, timeout, retry_cnt, params={"data": payload, "headers": headers}
        )
        d.execute()

        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result
        mock_post.assert_called_once()

    @patch("cliboa.adapter.http.requests.post")
    def test_execute_ng(self, mock_post):
        # Mock HTTP error
        mock_post.side_effect = HTTPError("Http request failed.")

        url = "https://spam.com/post"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Upload(
            url,
            self._dest_path,
            timeout,
            retry_cnt,
            retry_intvl_sec,
            params={"data": payload, "headers": headers},
        )
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed" in str(execinfo.value)


class TestUpdate(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    @patch("cliboa.adapter.http.requests.put")
    def test_execute_ok(self, mock_put):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.content = b'{"data":{"key":"value"},"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_put.return_value = mock_response

        url = "https://postman-echo.com/put"
        timeout = 10
        retry_cnt = 3
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Update(
            url, self._dest_path, timeout, retry_cnt, params={"data": payload, "headers": headers}
        )
        d.execute()

        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result
        mock_put.assert_called_once()

    @patch("cliboa.adapter.http.requests.put")
    def test_execute_ng(self, mock_put):
        # Mock HTTP error
        mock_put.side_effect = HTTPError("Http request failed.")

        url = "https://spam.com/put"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Update(
            url,
            self._dest_path,
            timeout,
            retry_cnt,
            retry_intvl_sec,
            params={"data": payload, "headers": headers},
        )
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed" in str(execinfo.value)


class TestDelete(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    @patch("cliboa.adapter.http.requests.delete")
    def test_execute_ok(self, mock_delete):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"headers":{"host":"postman-echo.com"}}'
        mock_response.content = b'{"headers":{"host":"postman-echo.com"}}'
        mock_response.raise_for_status.return_value = None
        mock_delete.return_value = mock_response

        url = "https://postman-echo.com/delete"
        timeout = 10
        retry_cnt = 3
        d = Remove(url, self._dest_path, timeout, retry_cnt)
        d.execute()

        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result
        mock_delete.assert_called_once()

    @patch("cliboa.adapter.http.requests.delete")
    def test_execute_ng(self, mock_delete):
        # Mock HTTP error
        mock_delete.side_effect = HTTPError("Http request failed.")

        url = "https://spam.com/delete"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        d = Remove(url, self._dest_path, timeout, retry_cnt, retry_intvl_sec)
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed" in str(execinfo.value)

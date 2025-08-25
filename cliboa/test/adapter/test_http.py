import os

import pytest
from requests.exceptions import HTTPError

# FIXME: This import is placed here as a temporary workaround to prevent a circular
#       import error between `cliboa.adapter.http` and modules under `cliboa.scenario`.
#       This dependency cycle should be properly resolved by refactoring.
from cliboa.scenario.validator import EssentialParameters  # noqa: F401
from cliboa.adapter.http import Download, FormAuth, Remove, Update, Upload


class TestHttp(object):

    _DUMMY_URL = "http://spam"
    _DUMMY_ID = 999
    _DUMMY_PASSWORD = "spam"


class TestFormAuth(TestHttp):
    def test_execute(self):
        a = FormAuth()
        setattr(a, "form_url", self._DUMMY_URL)
        setattr(a, "form_id", self._DUMMY_ID)
        setattr(a, "form_password", self._DUMMY_PASSWORD)
        a.execute()
        assert a.form_url == self._DUMMY_URL
        assert a.form_id == self._DUMMY_ID
        assert a.form_password == self._DUMMY_PASSWORD


class TestDownload(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    def test_execute_ok(self):
        # use Postman echo
        url = "https://postman-echo.com/get?foo1=bar1&foo2=bar2"
        timeout = 10
        retry_cnt = 3
        d = Download(url, self._dest_path, timeout, retry_cnt)
        d.execute()
        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        # use url which does not exist
        url = "https://spam.com/get"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        d = Download(url, self._dest_path, timeout, retry_cnt, retry_intvl_sec)
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed." in str(execinfo.value)


class TestUpload(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    def test_execute_ok(self):
        # use Postman echo
        url = "https://postman-echo.com/post"
        timeout = 10
        retry_cnt = 3
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Upload(url, self._dest_path, timeout, retry_cnt, data=payload, headers=headers)
        d.execute()
        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        # use url which does not exist
        url = "https://spam.com/post"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Upload(
            url, self._dest_path, timeout, retry_cnt, retry_intvl_sec, data=payload, headers=headers
        )
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed." in str(execinfo.value)


class TestUpdate(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    def test_execute_ok(self):
        # use Postman echo
        url = "https://postman-echo.com/put"
        timeout = 10
        retry_cnt = 3
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Update(url, self._dest_path, timeout, retry_cnt, data=payload, headers=headers)
        d.execute()
        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self._dest_path)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        # use url which does not exist
        url = "https://spam.com/put"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        payload = {"key": "value"}
        headers = {"content-type": "application/json"}
        d = Update(
            url, self._dest_path, timeout, retry_cnt, retry_intvl_sec, data=payload, headers=headers
        )
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed." in str(execinfo.value)


class TestDelete(object):
    def setup_method(self, method):
        self._dest_path = "/tmp/test.result"

    def test_execute_ok(self):
        # use Postman echo
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

    def test_execute_ng(self):
        # use url which does not exist
        url = "https://spam.com/delete"
        timeout = 1
        retry_cnt = 1
        retry_intvl_sec = 1
        d = Remove(url, self._dest_path, timeout, retry_cnt, retry_intvl_sec)
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed." in str(execinfo.value)

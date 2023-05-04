import os

import pytest
from requests.exceptions import HTTPError

from cliboa.adapter.http import Download, FormAuth


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

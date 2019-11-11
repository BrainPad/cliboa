import os
import sys
import pytest
import shutil
from pprint import pprint
from requests.exceptions import HTTPError

from cliboa.conf import env
from cliboa.util.http import Download


class TestDownload(object):
    def setup_method(self, method):
        self.__dest_path = "/tmp/test.result"

    def test_execute_ok(self):
        # use Postman echo
        url = "https://postman-echo.com/get?foo1=bar1&foo2=bar2"
        timeout = 10
        retry_count = 3
        d = Download(url, self.__dest_path, timeout, retry_count)
        d.execute()
        f = open("/tmp/test.result", "r")
        result = f.read()
        f.close()
        os.remove(self.__dest_path)
        assert "postman-echo.com" in result

    def test_execute_ng(self):
        # use url which does not exist
        url = "https://spam.com/get"
        timeout = 5
        retry_count = 1
        d = Download(url, self.__dest_path, timeout, retry_count)
        with pytest.raises(HTTPError) as execinfo:
            d.execute()
        assert "Http request failed." in str(execinfo.value)

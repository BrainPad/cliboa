import os
import re
import sys
import pytest
import shutil
from pprint import pprint

from cliboa.conf import env
from cliboa.util.ftp_util import FtpUtil


class TestFtpUtil(object):
    def setup_method(self, method):
        self.__dest_dir = "data"

    def test_list_files_ok(self):
        # use public ftp
        host = "test.rebex.net"
        ftp_util = FtpUtil(host, "demo", "password")
        src_dir = "/"
        pattern = "(.*).txt"
        os.makedirs(self.__dest_dir)
        files = ftp_util.list_files(src_dir, self.__dest_dir, re.compile(pattern))
        exists_downloaded_file = os.path.exists(
            os.path.join(self.__dest_dir, "readme.txt")
        )
        shutil.rmtree(self.__dest_dir)
        assert exists_downloaded_file is True

    def test_list_files_ng(self):
        # use public ftp
        host = "test.rebex.net"
        # Invalid password
        timeout = 5
        retry_count = 1
        ftp_util = FtpUtil(host, "demo", "pass", timeout, retry_count)
        src_dir = "/"
        pattern = "(.*).txt"
        os.makedirs(self.__dest_dir)
        with pytest.raises(IOError) as execinfo:
            files = ftp_util.list_files(src_dir, self.__dest_dir, re.compile(pattern))
        shutil.rmtree(self.__dest_dir)
        assert "FTP failed." in str(execinfo.value)

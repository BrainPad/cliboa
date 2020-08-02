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
import re
import shutil

import pytest

from cliboa.util.ftp_util import FtpUtil
from tests import BaseCliboaTest


class TestFtpUtil(BaseCliboaTest):
    def setup_method(self, method):
        self._dest_dir = "data"

    def test_list_files_with_ftp_ok(self):
        # use public ftp
        host = "test.rebex.net"
        ftp_util = FtpUtil(host, "demo", "password")
        src_dir = "/"
        pattern = "(.*).txt"
        os.makedirs(self._dest_dir, exist_ok=True)
        ftp_util.list_files(src_dir, self._dest_dir, re.compile(pattern))
        exists_downloaded_file = os.path.exists(
            os.path.join(self._dest_dir, "readme.txt")
        )
        shutil.rmtree(self._dest_dir)
        self.assertTrue(exists_downloaded_file)

    def test_list_files_with_tls_ng(self):
        # use public ftp
        host = "test.rebex.net"
        # Invalid password
        timeout = 5
        retry_count = 1
        ftp_util = FtpUtil(host, "demo", "pass", timeout, retry_count, tls=True)
        src_dir = "/"
        pattern = "(.*).txt"
        os.makedirs(self._dest_dir, exist_ok=True)
        with pytest.raises(IOError) as execinfo:
            ftp_util.list_files(src_dir, self._dest_dir, re.compile(pattern))
        shutil.rmtree(self._dest_dir)
        assert "FTP failed." in str(execinfo.value)

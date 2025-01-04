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

from cliboa.conf import env
from cliboa.scenario.extract.ftp import FtpDownload
from cliboa.test import BaseCliboaTest
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestFtpDownload(BaseCliboaTest):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir)
            instance = FtpDownload()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use public ftp
            """
            Helper.set_property(instance, "host", "test.rebex.net")
            Helper.set_property(instance, "user", "demo")
            Helper.set_property(instance, "password", "password")
            Helper.set_property(instance, "src_dir", "/")
            Helper.set_property(instance, "src_pattern", "(.*).txt")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            instance.execute()
            exists_file = os.path.exists(os.path.join(self._data_dir, "readme.txt"))
            """
        finally:
            shutil.rmtree(self._data_dir)
        # self.assertTrue(exists_file)

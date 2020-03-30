#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
import os, pytest, sys, shutil
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.extract.http import HttpDownload
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.helper import Helper


class TestHttpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        try:
            os.makedirs(self._data_dir)
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

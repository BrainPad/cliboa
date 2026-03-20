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

import pytest

from cliboa.conf import env
from cliboa.scenario.extract.ftp import FtpDownload
from tests import BaseCliboaTest


@pytest.mark.skip(reason="The test result is not meaningful.")
class TestFtpDownload(BaseCliboaTest):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok(self):
        # TODO: Implement necessary tests using mocks.
        try:
            os.makedirs(self._data_dir)
            instance = FtpDownload()  # noqa
            """
            instance._set_arguments({
                "host": "test.rebex.net",
                "user": "demo",
                "password": "password",
                "src_dir": "/",
                "src_pattern": "(.*).txt",
                "dest_dir": self._data_dir,
            }
            instance.execute()
            exists_file = os.path.exists(os.path.join(self._data_dir, "readme.txt"))
            """
        finally:
            shutil.rmtree(self._data_dir)
        # self.assertTrue(exists_file)

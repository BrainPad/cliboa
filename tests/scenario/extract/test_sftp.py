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

import os
import sys
import pytest
import shutil
from pprint import pprint
from cliboa.conf import env
from cliboa.scenario.extract.sftp import SftpDownload
from cliboa.util.lisboa_log import LisboaLog


class TestSftpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @pytest.mark.skip(reason="CiecleCI failed to execute")
    def test_execute_ok(self):
        os.makedirs(self._data_dir)
        instance = SftpDownload()
        instance.logger = LisboaLog.get_logger(__name__)
        # use public sftp
        setattr(instance, "host", "test.rebex.net")
        setattr(instance, "user", "demo")
        setattr(instance, "password", "password")
        setattr(instance, "src_dir", "/")
        setattr(instance, "src_pattern", "(.*).txt")
        setattr(instance, "dest_dir", self._data_dir)
        instance.execute()
        exists_file = os.path.exists(os.path.join(self._data_dir, "readme.txt"))
        shutil.rmtree(self._data_dir)
        assert exists_file is True

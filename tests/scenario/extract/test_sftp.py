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
import shutil

from cliboa.conf import env
from cliboa.scenario.extract.sftp import SftpDownload
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from contextlib import ExitStack
from unittest.mock import patch


class TestSftpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_with_files(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch('cliboa.util.sftp.Sftp.list_files'))
            mock_sftp.return_value = ["test.txt"]

            instance.execute()

            assert ObjectStore.get("sftp_class") == ["test.txt"]
            shutil.rmtree(self._data_dir)

    def test_execute_nofiles_return(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")
        Helper.set_property(instance, "quit", True)

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch('cliboa.util.sftp.Sftp.list_files'))
            mock_sftp.return_value = []

            ret = instance.execute()

            assert ret == StepStatus.SUCCESSFUL_TERMINATION
            shutil.rmtree(self._data_dir)

    def test_execute_nofiles_continue(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")
        Helper.set_property(instance, "quit", False)

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch('cliboa.util.sftp.Sftp.list_files'))
            mock_sftp.return_value = []

            ret = instance.execute()

            assert ret is None
            assert ObjectStore.get("sftp_class") == []
            shutil.rmtree(self._data_dir)

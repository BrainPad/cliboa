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
from contextlib import ExitStack
from unittest.mock import patch

from cliboa.conf import env
from cliboa.scenario.extract.sftp import SftpDownload, SftpFileExistsCheck
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus
from cliboa.util.helper import Helper
from cliboa.util.log import _get_logger


class TestSftpDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def test_execute_with_files(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = ["test.txt"]

            instance.execute()

            assert mock_sftp.called
            assert ObjectStore.get("sftp_class") == ["test.txt"]

    def test_execute_nofiles_return(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")
        Helper.set_property(instance, "quit", True)

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = []

            ret = instance.execute()

            assert mock_sftp.called
            assert ret == StepStatus.SUCCESSFUL_TERMINATION

    def test_execute_nofiles_continue(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")
        Helper.set_property(instance, "quit", False)

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = []

            ret = instance.execute()

            assert mock_sftp.called
            assert ret is None
            assert ObjectStore.get("sftp_class") == []

    def test_execute_with_key(self):
        dummy_pass = os.path.join(self._data_dir, "id_rsa")
        with open(dummy_pass, "w") as f:
            f.write("test")

        instance = SftpDownload()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "key", dummy_pass)
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = ["test.txt"]

            instance.execute()

    def test_execute_with_key_content(self):
        instance = SftpDownload()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "key", {"content": "dummy_rsa"})
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "step", "sftp_class")

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = ["test.txt"]

            instance.execute()

            assert mock_sftp.called
            assert ObjectStore.get("sftp_class") == ["test.txt"]


class TestSftpFileExistsCheck:
    def test_execute_file_found(self):
        instance = SftpFileExistsCheck()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = ["test.txt"]

            instance.execute()
            assert mock_sftp.called

    def test_execute_file_not_found(self):
        instance = SftpFileExistsCheck()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "host", "dummy.host")
        Helper.set_property(instance, "user", "dummy_user")
        Helper.set_property(instance, "password", "dummy_pass")
        Helper.set_property(instance, "src_dir", "/")
        Helper.set_property(instance, "src_pattern", ".*.txt")
        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))
            mock_sftp.return_value = []

            res = instance.execute()

            assert mock_sftp.called
            assert res == StepStatus.SUCCESSFUL_TERMINATION

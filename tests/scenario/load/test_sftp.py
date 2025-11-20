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
from pathlib import Path
from unittest.mock import patch

from cliboa.conf import env
from cliboa.scenario.load.sftp import SftpUpload


class TestSftpUpload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        os.makedirs(self._data_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def test_execute_with_files(self):
        dir_path = Path(self._data_dir)
        (dir_path / "a.txt").touch()
        (dir_path / "b.txt").touch()
        (dir_path / "c.exe").touch()

        instance = SftpUpload()
        instance._set_arguments(
            {
                "host": "dummy.host",
                "user": "dummy_user",
                "password": "dummy_pass",
                "src_dir": self._data_dir,
                "src_pattern": ".*.txt",
                "dest_dir": self._data_dir,
                "step": "sftp_class",
            }
        )

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))

            instance.execute()

            assert mock_sftp.called

    def test_execute_with_key(self):
        dummy_pass = os.path.join(self._data_dir, "id_rsa")
        with open(dummy_pass, "w") as f:
            f.write("test")
        dir_path = Path(self._data_dir)
        (dir_path / "a.txt").touch()
        (dir_path / "b.txt").touch()
        (dir_path / "c.exe").touch()

        instance = SftpUpload()
        instance._set_arguments(
            {
                "host": "dummy.host",
                "user": "dummy_user",
                "key": dummy_pass,
                "src_dir": self._data_dir,
                "src_pattern": ".*.txt",
                "dest_dir": self._data_dir,
                "step": "sftp_class",
            }
        )

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))

            instance.execute()

            assert mock_sftp.called

    def test_execute_with_key_content(self):
        dir_path = Path(self._data_dir)
        (dir_path / "a.txt").touch()
        (dir_path / "b.txt").touch()
        (dir_path / "c.exe").touch()
        instance = SftpUpload()
        instance._set_arguments(
            {
                "host": "dummy.host",
                "user": "dummy_user",
                "key": {"content": "dummy_rsa"},
                "src_dir": self._data_dir,
                "src_pattern": ".*.txt",
                "dest_dir": self._data_dir,
                "step": "sftp_class",
            }
        )

        with ExitStack() as stack:
            mock_sftp = stack.enter_context(patch("cliboa.adapter.sftp.SftpAdapter.execute"))

            instance.execute()

            assert mock_sftp.called

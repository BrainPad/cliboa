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
import sys

import pytest

from cliboa.conf import env
from cliboa.util.exception import CliboaException
from cliboa.util.gpg import Gpg
from tests import BaseCliboaTest


class TestGpg(BaseCliboaTest):
    """
    gpg test
    """

    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        self._result_dir = os.path.join(self._data_dir, "result")
        self._gpg_dir = os.path.join(self._data_dir, "gpg")
        os.makedirs(self._data_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)
        os.makedirs(self._gpg_dir, exist_ok=True)

        self._file_name = "test.txt"
        self._encrypt_name = "test.txt.gpg"
        self._file_path = os.path.join(self._data_dir, "test.txt")
        with open(self._file_path, mode="w", encoding="utf-8") as f:
            f.write("This is test")

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def test_generate_key_and_encrypt_decrypt_ok(self):
        """
        Generate rsa key, encryption, decryption, everything is ok.
        """
        if sys.version_info.minor < 7:
            # For some reasons, test fails under version of python 3.6
            return

        _gpg = Gpg(self._gpg_dir)

        _gpg.generate_key(
            dest_dir=self._data_dir, name_email="test@email.com", passphrase="password"
        )

        _gpg.encrypt(
            src_path=self._file_path,
            dest_path=os.path.join(self._result_dir, self._file_name),
            recipients="test@email.com",
        )

        _gpg.decrypt(
            src_path=os.path.join(self._result_dir, self._encrypt_name),
            dest_path=os.path.join(self._result_dir, self._file_name),
            passphrase="password",
        )

        with open(os.path.join(self._result_dir, self._file_name), mode="r", encoding="utf-8") as f:
            txt = f.read()
            assert txt == "This is test"

    def test_generate_key_and_encrypt_ng(self):
        """
        Encryption fails due to unexpected recipient.
        """
        if sys.version_info.minor < 7:
            return

        _gpg = Gpg(self._gpg_dir)

        _gpg.generate_key(
            dest_dir=self._data_dir, name_email="test@email.com", passphrase="password"
        )

        with pytest.raises(CliboaException) as execinfo:
            _gpg.encrypt(
                src_path=self._file_path,
                dest_path=os.path.join(self._result_dir, self._file_name),
                recipients="test_ng@email.com",
            )
        assert "Failed to encrypt gpg." in str(execinfo.value)

    def test_generate_key_and_decrypt_ng(self):
        """
        Decryption fails due to wrong password.
        """
        if sys.version_info.minor < 7:
            return

        _gpg = Gpg(self._gpg_dir)

        _gpg.generate_key(
            dest_dir=self._data_dir, name_email="test@email.com", passphrase="password"
        )

        _gpg.encrypt(
            src_path=self._file_path,
            dest_path=os.path.join(self._result_dir, self._file_name),
            recipients="test@email.com",
        )

        with pytest.raises(CliboaException) as execinfo:
            _gpg.decrypt(
                src_path=os.path.join(self._result_dir, self._encrypt_name),
                dest_path=os.path.join(self._result_dir, self._file_name),
                passphrase="password2",
            )
        assert "Failed to decrypt gpg." in str(execinfo.value)

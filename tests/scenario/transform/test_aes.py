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
from cliboa.scenario.transform.aes import AesDecrypt, AesEncrypt
from tests import BaseCliboaTest


class TestAes(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        self._result_dir = os.path.join(self._data_dir, "result")
        os.makedirs(self._data_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)

        self._file_name = "test.txt"
        self._file_path = os.path.join(self._data_dir, self._file_name)
        with open(self._file_path, mode="w", encoding="utf-8") as f:
            f.write("This is test")

        self._target_name = "target.txt"
        self._file_path = os.path.join(self._data_dir, self._target_name)
        with open(self._file_path, mode="w", encoding="utf-8") as f:
            f.write("This is target")

        self._key_name = "test.key"
        self._key_path = os.path.join(self._data_dir, self._key_name)
        with open(self._key_path, mode="w", encoding="utf-8") as f:
            f.write("pMMt08w5hh_wzlVmHPMbKyYYBY5978jfo0hJeWPl1SI=")

    def tearDown(self):
        shutil.rmtree(self._data_dir)

    def test_encrypt_decrypt_ok(self):
        # Encryption
        instance = AesEncrypt()
        instance._set_arguments(
            {
                "src_dir": self._data_dir,
                "src_pattern": r"test\.txt",
                "dest_dir": self._result_dir,
                "key_dir": self._data_dir,
                "key_pattern": r"test\.key",
            }
        )
        instance.execute()

        # Decryption
        instance = AesDecrypt()
        instance._set_arguments(
            {
                "src_dir": self._result_dir,
                "src_pattern": r"test\.txt",
                "dest_dir": self._result_dir,
                "key_dir": self._data_dir,
                "key_pattern": r"test\.key",
            }
        )
        instance.execute()

        with open(os.path.join(self._result_dir, self._file_name), mode="r", encoding="utf-8") as f:
            txt = f.read()
        assert txt == "This is test"

    def test_by_multiple_encrypt_decrypt_ok(self):
        # Encryption
        instance = AesEncrypt()
        instance._set_arguments(
            {
                "src_dir": self._data_dir,
                "src_pattern": r"(.*)\.txt",
                "dest_dir": self._result_dir,
                "key_dir": self._data_dir,
                "key_pattern": r"test\.key",
            }
        )
        instance.execute()

        # Decryption
        instance = AesDecrypt()
        instance._set_arguments(
            {
                "src_dir": self._result_dir,
                "src_pattern": r"(.*)\.txt",
                "dest_dir": self._result_dir,
                "key_dir": self._data_dir,
                "key_pattern": r"test\.key",
            }
        )
        instance.execute()

        with open(
            os.path.join(self._result_dir, self._target_name), mode="r", encoding="utf-8"
        ) as f:
            txt = f.read()
        assert txt == "This is target"

        with open(os.path.join(self._result_dir, self._file_name), mode="r", encoding="utf-8") as f:
            txt = f.read()
        assert txt == "This is test"

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
import sqlite3
from pprint import pprint

from cliboa.conf import env
from cliboa.util.exception import InvalidCount
from cliboa.util.file import File


class TestFile(object):
    def setup_method(self, method):
        self.__data_dir = os.path.join(env.BASE_DIR, "data")
        self.__data_subdir = os.path.join(self.__data_dir, "sub")

    def test_get_target_files_ok_file_exists(self):
        # create test file
        os.makedirs(self.__data_dir, exist_ok=True)
        os.makedirs(self.__data_subdir, exist_ok=True)

        test_file = os.path.join(self.__data_dir, "test.csv")
        open(test_file, "w").close()

        test_file_sub = os.path.join(self.__data_subdir, "test_sub.csv")
        open(test_file_sub, "w").close()

        # execute1
        target_files = File().get_target_files(
            self.__data_dir, "test(.*).csv", tree=True
        )
        assert len(target_files) == 2
        assert target_files[0] == os.path.join(self.__data_dir, "test.csv")
        assert target_files[1] == os.path.join(self.__data_dir, "sub", "test_sub.csv")

        # execute2
        target_files = File().get_target_files(
            self.__data_dir, "test(.*).csv", tree=False
        )
        assert len(target_files) == 1
        assert target_files[0] == os.path.join(self.__data_dir, "test.csv")

        shutil.rmtree(self.__data_subdir)
        shutil.rmtree(self.__data_dir)

    def test_get_target_files_ok_no_files(self):
        # create test file
        os.makedirs(self.__data_dir, exist_ok=True)

        # execute
        target_files = File().get_target_files(self.__data_dir, "test(.*).csv")

        shutil.rmtree(self.__data_dir)
        assert target_files == []

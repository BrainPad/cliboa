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
import csv
import os
import sys
import pytest
import shutil
import sqlite3
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.load.file import FileWrite, CsvWrite
from cliboa.util.cache import StorageIO
from cliboa.util.exception import CliboaException, FileNotFound
from cliboa.util.lisboa_log import LisboaLog


class TestFileWrite(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")


class TestCsvWrite(TestFileWrite):
    def test_execute_ok(self):
        os.makedirs(self._data_dir)
        # create dummy input data
        instance = CsvWrite()
        instance.logger = LisboaLog.get_logger(__name__)
        instance.__dict__["_s"].save({"id": 1, "name": 1, "age": 1})
        test_csv = os.path.join(self._data_dir, "test.csv")
        setattr(instance, "io", "output")
        setattr(instance, "dest_path", test_csv)
        instance.execute()
        exists_file = os.path.exists(test_csv)
        shutil.rmtree(self._data_dir)
        assert exists_file is True

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
from cliboa.scenario.extract.file import FileRead, CsvRead
from cliboa.util.cache import StorageIO
from cliboa.util.exception import CliboaException, FileNotFound

class TestFileRead(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

class TestCsvRead(TestFileRead):
    def test_execute_ng_multi_files(self):
        os.makedirs(self._data_dir)
        instance = CsvRead()
        test_csv = os.path.join(self._data_dir, "test*.csv")
        test_csv1 = os.path.join(self._data_dir, "test1.csv")
        test_csv2 = os.path.join(self._data_dir, "test2.csv")
        # create dummy csv
        l = ["spam"]
        with open(test_csv1, "w") as f:
            w = csv.writer(f)
            w.writerows(l)
        with open(test_csv2, "w") as f:
            w = csv.writer(f)
            w.writerows(l)
        setattr(instance, "io", "input")
        setattr(instance, "src_path", test_csv)
        with pytest.raises(CliboaException) as execinfo:
            instance.execute()
        shutil.rmtree(self._data_dir)
        assert "Input file must" in str(execinfo.value)

    def test_execute_ng_no_files(self):
        os.makedirs(self._data_dir)
        instance = CsvRead()
        test_csv = os.path.join(self._data_dir, "test.csv")
        setattr(instance, "io", "input")
        setattr(instance, "src_path", test_csv)
        with pytest.raises(FileNotFound) as execinfo:
            instance.execute()
        shutil.rmtree(self._data_dir)
        assert "The specified csv file not found" in str(execinfo.value)

    def test_execute_ok(self):
        os.makedirs(self._data_dir)
        instance = CsvRead()
        test_csv = os.path.join(self._data_dir, "test.csv")
        csv_list = [["key", "data"], ["1", "spam"]]
        with open(test_csv, "w") as f:
            w = csv.writer(f)
            w.writerows(csv_list)
        setattr(instance, "io", "input")
        setattr(instance, "src_path", test_csv)
        instance.execute()
        shutil.rmtree(self._data_dir)
        #s = StorageIO()
        fetch_result = []
        pprint(instance.__dict__)
        with open(instance.__dict__["_s"].cache_file, "r", encoding="utf-8") as i:
            fetch_result.append(i.readline())
        instance.__dict__["_s"].remove()
        assert fetch_result == ["{'key': '1', 'data': 'spam'}\n"]

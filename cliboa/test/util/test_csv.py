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
import csv
import os
import shutil

from cliboa.conf import env
from cliboa.util.csv import Csv


class TestCsv(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_extract_columns_with_names(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [["key", "data"], ["1", "spam"]]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()
        output_file = os.path.join(self._data_dir, "output.csv")
        try:
            remain_columns = ["key"]
            Csv.extract_columns_with_names(test_csv, output_file, remain_columns)
            with open(test_csv, "r") as o:
                reader = csv.DictReader(o)
                for r in reader:
                    assert r["key"] == test_csv_data[1][0]
        finally:
            shutil.rmtree(self._data_dir)

    def test_extract_columns_with_numbers_with_headers(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [
            ["header1", "header2"],
            ["1", "spam1"],
            ["2", "spam2"],
            ["3", "spam3"],
        ]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()
        try:
            output_file = os.path.join(self._data_dir, "output.csv")
            remain_column_numbers = [1]
            Csv.extract_columns_with_numbers(
                test_csv, output_file, remain_column_numbers
            )
            with open(test_csv, "r") as o:
                reader = csv.DictReader(o)
                for r in reader:
                    assert r[test_csv_data[0][0]] in [
                        test_csv_data[1][0],
                        test_csv_data[2][0],
                        test_csv_data[3][0],
                    ]
        finally:
            shutil.rmtree(self._data_dir)

    def test_extract_columns_with_numbers_with_no_headers(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [
            ["1", "spam1", "hoge1"],
            ["2", "spam2", "hoge2"],
            ["3", "spam3", "hoge3"],
        ]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()
        try:
            output_file = os.path.join(self._data_dir, "output.csv")
            remain_column_numbers = [1, 3]
            Csv.extract_columns_with_numbers(
                test_csv, output_file, remain_column_numbers
            )
            with open(test_csv, "r") as o:
                reader = csv.reader(o)
                for r in reader:
                    assert r[0] in [
                        test_csv_data[0][0],
                        test_csv_data[1][0],
                        test_csv_data[2][0],
                    ]
                    assert r[1] in [
                        test_csv_data[0][2],
                        test_csv_data[1][2],
                        test_csv_data[2][2],
                    ]
        finally:
            shutil.rmtree(self._data_dir)

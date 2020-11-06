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
from cliboa.scenario.transform.csv import CsvColumnExtract
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestCsvColumnExtract(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_execute_ok_with_column_names(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [["key", "data"], ["1", "spam"]]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        remain_columns = ["key"]
        Helper.set_property(instance, "columns", remain_columns)
        try:
            instance.execute()
            output_file = os.path.join(self._data_dir, "test.csv")
            with open(output_file, "r") as o:
                reader = csv.DictReader(o)
                for r in reader:
                    assert r["key"] == test_csv_data[1][0]
        finally:
            shutil.rmtree(self._data_dir)

    def test_execute_ok_with_remain_column_numbers(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [["1", "spam"], ["2", "spam"]]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        remain_column_number = 1
        Helper.set_property(instance, "column_numbers", remain_column_number)
        try:
            instance.execute()
            output_file = os.path.join(self._data_dir, "test.csv")
            with open(output_file, "r") as o:
                reader = csv.DictReader(o)
                for r in reader:
                    assert r[test_csv_data[0][0]] == test_csv_data[1][0]
        finally:
            shutil.rmtree(self._data_dir)

    def test_execute_ok_with__column_numbers(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [
            ["1", "spam", "hoge"],
            ["2", "spam2", "hoge2"],
            ["3", "spam3", "hoge3"],
        ]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        remain_column_numbers = "1,3"
        Helper.set_property(instance, "column_numbers", remain_column_numbers)
        try:
            instance.execute()
            output_file = os.path.join(self._data_dir, "test.csv")
            with open(output_file, "r") as o:
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

    def test_execute_ok_with_one_column_number(self):
        # create test csv
        os.makedirs(self._data_dir, exist_ok=True)
        test_csv = os.path.join(self._data_dir, "test.csv")
        test_csv_data = [
            ["1", "spam", "hoge"],
            ["2", "spam2", "hoge2"],
            ["3", "spam3", "hoge3"],
        ]
        with open(test_csv, "w") as t:
            writer = csv.writer(t)
            writer.writerows(test_csv_data)
            t.flush()

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        remain_column_numbers = 3
        Helper.set_property(instance, "column_numbers", remain_column_numbers)
        try:
            instance.execute()
            output_file = os.path.join(self._data_dir, "test.csv")
            with open(output_file, "r") as o:
                reader = csv.reader(o)
                for r in reader:
                    assert r[0] in [
                        test_csv_data[0][2],
                        test_csv_data[1][2],
                        test_csv_data[2][2],
                    ]
        finally:
            shutil.rmtree(self._data_dir)

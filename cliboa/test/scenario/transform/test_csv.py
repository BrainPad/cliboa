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
import jsonlines
import os
import shutil
import pytest

from glob import glob
from cliboa.conf import env
from cliboa.scenario.transform.csv import (
    CsvColumnConcat,
    CsvColumnExtract,
    ColumnLengthAdjust,
    CsvConcat,
    CsvConvert,
    CsvHeaderConvert,
    CsvFormatChange,
    CsvMerge,
    CsvSort,
    CsvToJsonl,
)
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import InvalidCount, InvalidParameter
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestCsvTransform(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        self._result_dir = os.path.join(env.BASE_DIR, "data", "result")
        os.makedirs(self._data_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def _create_csv(self, data, fname="test.csv"):
        src = os.path.join(self._data_dir, fname)
        with open(src, mode="w", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row)
        return src


class TestCsvColumnExtract(TestCsvTransform):
    def test_execute_ok_with_column_names(self):
        # create test csv
        test_csv_data = [["key", "data"], ["1", "spam"]]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        remain_columns = ["key"]
        Helper.set_property(instance, "columns", remain_columns)
        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r["key"] == test_csv_data[1][0]

    def test_execute_ok_with_remain_column_numbers(self):
        # create test csv
        test_csv_data = [["1", "spam"], ["2", "spam"]]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        remain_column_number = 1
        Helper.set_property(instance, "column_numbers", remain_column_number)

        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r[test_csv_data[0][0]] == test_csv_data[1][0]

    def test_execute_ok_with__column_numbers(self):
        # create test csv
        test_csv_data = [
            ["1", "spam", "hoge"],
            ["2", "spam2", "hoge2"],
            ["3", "spam3", "hoge3"],
        ]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        remain_column_numbers = "1,3"
        Helper.set_property(instance, "column_numbers", remain_column_numbers)

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

    def test_execute_ok_with_one_column_number(self):
        # create test csv
        test_csv_data = [
            ["1", "spam", "hoge"],
            ["2", "spam2", "hoge2"],
            ["3", "spam3", "hoge3"],
        ]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnExtract()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        remain_column_numbers = 3
        Helper.set_property(instance, "column_numbers", remain_column_numbers)

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


class TestCsvColumnConcat(TestCsvTransform):
    def test_execute_ok(self):
        # create test csv
        test_csv_data = [["key", "data"], ["1", "spam"]]
        concat_data = ["key_data", "1spam"]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        concat_columns = ["key", "data"]
        Helper.set_property(instance, "columns", concat_columns)
        Helper.set_property(instance, "dest_column_name", "key_data")

        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r["key_data"] == concat_data[1]

    def test_execute_ok_with_separator(self):
        # create test csv
        test_csv_data = [["key", "data"], ["1", "spam"]]
        concat_data = ["key_data", "1 spam"]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        concat_columns = ["key", "data"]
        Helper.set_property(instance, "columns", concat_columns)
        Helper.set_property(instance, "dest_column_name", "key_data")
        Helper.set_property(instance, "sep", " ")

        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r["key_data"] == concat_data[1]

    def test_execute_three_or_more_column_concat_ok(self):
        # create test csv
        test_csv_data = [["key", "data", "data1"], ["1", "spam", "spam1"]]
        concat_data = ["key_data_data1", "1 spam spam1"]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        concat_columns = ["key", "data", "data1"]
        Helper.set_property(instance, "columns", concat_columns)
        Helper.set_property(instance, "dest_column_name", "key_data_data1")
        Helper.set_property(instance, "sep", " ")

        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r["key_data_data1"] == concat_data[1]

    def test_execute_ok_with_non_target_column_remain(self):
        # create test csv
        test_csv_data = [["key", "data", "data1"], ["1", "spam", "spam1"]]
        concat_data = [["key_data", "data1"], ["1spam", "spam1"]]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        concat_columns = ["key", "data"]
        Helper.set_property(instance, "columns", concat_columns)
        Helper.set_property(instance, "dest_column_name", "key_data")

        instance.execute()
        output_file = os.path.join(self._data_dir, "test.csv")
        with open(output_file, "r") as o:
            reader = csv.DictReader(o)
            for r in reader:
                assert r["key_data"] == concat_data[1][0]
                assert r["data1"] == concat_data[1][1]

    def test_execute_ng_with_specify_not_exist_column(self):
        # create test csv
        test_csv_data = [["key", "data"], ["1", "spam"]]
        self._create_csv(test_csv_data)

        # set the essential attributes
        instance = CsvColumnConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        concat_columns = ["key", "test"]
        Helper.set_property(instance, "columns", concat_columns)
        Helper.set_property(instance, "dest_column_name", "key_data")

        with pytest.raises(KeyError) as e:
            instance.execute()
        assert "'test'" == str(e.value)


class TestColumnLengthAdjust(TestCsvTransform):
    # TODO Old version test.
    def test_ok_old(self):
        test_csv_data = [["key", "data"], ["1", "1234567890"]]
        self._create_csv(test_csv_data)

        instance = ColumnLengthAdjust()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "adjust", {"data": 5})
        des_path = os.path.join(self._result_dir, "test.csv")
        Helper.set_property(instance, "dest_path", des_path)

        instance.execute()

        with open(des_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                assert "12345" == row.get("data")

    # TODO Old version test.
    def test_ng_plural_files_old(self):
        test_csv_data = [["key", "data"], ["1", "1234567890"]]
        self._create_csv(test_csv_data, fname="test1.csv")
        self._create_csv(test_csv_data, fname="test2.csv")

        instance = ColumnLengthAdjust()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.csv")
        Helper.set_property(instance, "adjust", {"data": 5})
        des_path = os.path.join(self._result_dir, "test.csv")
        Helper.set_property(instance, "dest_path", des_path)

        with pytest.raises(Exception) as execinfo:
            instance.execute()
        assert "Input file must be only one." == str(execinfo.value)

    def test_ok(self):
        test_csv_data = [["key", "data"], ["1", "1234567890"]]
        file1 = self._create_csv(test_csv_data, fname="test1.csv")
        file2 = self._create_csv(test_csv_data, fname="test2.csv")

        instance = ColumnLengthAdjust()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.*.csv")
        Helper.set_property(instance, "adjust", {"data": 5})
        instance.execute()

        for file in [file1, file2]:
            with open(file, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    assert "12345" == row.get("data")


class TestCsvHeaderConvert(TestCsvTransform):
    # TODO Old version test.
    def test_execute_ok_old(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvHeaderConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test_new.csv")
        Helper.set_property(
            instance, "headers", [{"key": "new_key"}, {"data": "new_data"}]
        )
        instance.execute()

        test_new_csv = os.path.join(self._data_dir, "test_new.csv")
        with open(test_new_csv, "r") as t:
            reader = csv.reader(t)
            line = next(reader)
        assert line == ["new_key", "new_data"]

    # TODO Old version test.
    def test_execute_ok_2_old(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvHeaderConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test_new.csv")
        Helper.set_property(instance, "headers", [{"key": "new_key"}])
        instance.execute()

        test_new_csv = os.path.join(self._data_dir, "test_new.csv")
        with open(test_new_csv, "r") as t:
            reader = csv.reader(t)
            line = next(reader)
        assert line == ["new_key", "data"]

    # TODO Old version test.
    def test_execute_ng_no_src_file(self):
        with pytest.raises(InvalidCount) as execinfo:
            # set the essential attributes
            instance = CsvHeaderConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test_new.csv")
            Helper.set_property(
                instance, "headers", [{"key": "new_key"}, {"data": "new_data"}]
            )
            instance.execute()

        shutil.rmtree(self._data_dir)
        assert "not exist" in str(execinfo.value)

    # TODO Old version test.
    def test_execute_ng_no_multiple_files(self):
        # create test files
        test1_csv = os.path.join(self._data_dir, "test1.csv")
        test2_csv = os.path.join(self._data_dir, "test2.csv")
        open(test1_csv, "w").close
        open(test2_csv, "w").close

        with pytest.raises(InvalidCount) as execinfo:
            # set the essential attributes
            instance = CsvHeaderConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test(.*)\.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test_new.csv")
            Helper.set_property(
                instance, "headers", [{"key": "new_key"}, {"data": "new_data"}]
            )
            instance.execute()

        assert "only one" in str(execinfo.value)

    def test_execute_ok(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        file1 = self._create_csv(csv_list, fname="test1.csv")
        file2 = self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvHeaderConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(
            instance, "headers", [{"key": "new_key"}, {"data": "new_data"}]
        )
        instance.execute()

        for file in [file1, file2]:
            with open(file, "r") as t:
                reader = csv.reader(t)
                line = next(reader)
            assert line == ["new_key", "new_data"]

    def test_execute_ok_2(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        file = self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvHeaderConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "headers", [{"key": "new_key"}])
        instance.execute()

        with open(file, "r") as t:
            reader = csv.reader(t)
            line = next(reader)
        assert line == ["new_key", "data"]


class TestCsvFormatChange(TestCsvTransform):
    # TODO Old version test.
    def test_execute_ok_old(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvFormatChange()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "before_format", "csv")
        Helper.set_property(instance, "before_enc", "utf-8")
        Helper.set_property(instance, "after_format", "tsv")
        Helper.set_property(instance, "after_enc", "utf-8")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.tsv")
        instance.execute()

        test_new_csv = os.path.join(self._data_dir, "test.tsv")
        with open(test_new_csv, "r") as t:
            reader = csv.DictReader(t, delimiter="\t")
            for i, row in enumerate(reader):
                if i == 0:
                    assert "1" == row.get("key")
                elif i == 1:
                    assert "2" == row.get("key")
                elif i == 2:
                    assert "3" == row.get("key")

    # TODO Old version test.
    def test_execute_ng_plural_files_old(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list, fname="test1.csv")
        self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvFormatChange()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "before_format", "csv")
        Helper.set_property(instance, "before_enc", "utf-8")
        Helper.set_property(instance, "after_format", "tsv")
        Helper.set_property(instance, "after_enc", "utf-8")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.tsv")
        with pytest.raises(Exception) as execinfo:
            instance.execute()
        assert "Input file must be only one." == str(execinfo.value)

    def test_execute_ok(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        file1 = self._create_csv(csv_list, fname="test1.csv")
        file2 = self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvFormatChange()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "before_format", "csv")
        Helper.set_property(instance, "before_enc", "utf-8")
        Helper.set_property(instance, "after_format", "tsv")
        Helper.set_property(instance, "after_enc", "utf-8")
        instance.execute()

        for file in [file1, file2]:
            root, _ = os.path.splitext(file)
            with open(root + ".tsv", "r") as t:
                reader = csv.DictReader(t, delimiter="\t")
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("key")
                    elif i == 1:
                        assert "2" == row.get("key")
                    elif i == 2:
                        assert "3" == row.get("key")


class TestCsvMerge(TestCsvTransform):
    # TODO Old version test.
    def test_execute_ok_old(self):
        # create test file
        csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [
            ["key", "address"],
            ["1", "spam"],
            ["2", "spam"],
            ["3", "spam"],
        ]
        self._create_csv(csv_list2, fname="test2.csv")

        # set the essential attributes
        instance = CsvMerge()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src1_pattern", r"test1\.csv")
        Helper.set_property(instance, "src2_pattern", r"test2\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.csv")
        instance.execute()

        exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        assert "test.csv" in exists_csv[0]

    # TODO Old version test.
    def test_execute_ok_with_unnamed_old(self):
        # create test file
        csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [
            ["key", "Unnamed: 0"],
            ["1", "spam"],
            ["2", "spam"],
            ["3", "spam"],
        ]
        self._create_csv(csv_list2, fname="test2.csv")

        # set the essential attributes
        instance = CsvMerge()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src1_pattern", r"test1\.csv")
        Helper.set_property(instance, "src2_pattern", r"test2\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.csv")
        instance.execute()

        exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        assert "test.csv" in exists_csv[0]

    # TODO Old version test.
    def test_excute_ng_multiple_target1_old(self):
        with pytest.raises(InvalidCount) as execinfo:
            # create test file
            target1_file = os.path.join(self._data_dir, "test11.csv")
            open(target1_file, "w").close()
            target1_file = os.path.join(self._data_dir, "test111.csv")
            open(target1_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test2.csv")
            open(target2_file, "w").close()

            # set the essential attributes
            instance = CsvMerge()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src1_pattern", "test1(.*).csv")
            Helper.set_property(instance, "src2_pattern", "test2.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()
        assert "must be only one" in str(execinfo.value)

    # TODO Old version test.
    def test_excute_ng_multiple_target2_old(self):
        with pytest.raises(InvalidCount) as execinfo:
            # create test file
            target1_file = os.path.join(self._data_dir, "test1.csv")
            open(target1_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test22.csv")
            open(target2_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test222.csv")
            open(target2_file, "w").close()

            # set the essential attributes
            instance = CsvMerge()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src1_pattern", "test1.csv")
            Helper.set_property(instance, "src2_pattern", "test2(.*).csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()
        assert "must be only one" in str(execinfo.value)

    def test_execute_ok(self):
        # create test file
        csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [
            ["key", "address"],
            ["1", "spam"],
            ["2", "spam"],
            ["3", "spam"],
        ]
        self._create_csv(csv_list2, fname="test2.csv")

        # set the essential attributes
        instance = CsvMerge()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src1_pattern", r"test1\.csv")
        Helper.set_property(instance, "src2_pattern", r"test2\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.csv")
        instance.execute()

        exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        assert "test.csv" in exists_csv[0]

    def test_execute_ok_with_unnamed(self):
        # create test file
        csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [
            ["key", "Unnamed: 0"],
            ["1", "spam"],
            ["2", "spam"],
            ["3", "spam"],
        ]
        self._create_csv(csv_list2, fname="test2.csv")

        # set the essential attributes
        instance = CsvMerge()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src1_pattern", r"test1\.csv")
        Helper.set_property(instance, "src2_pattern", r"test2\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.csv")
        instance.execute()

        exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        assert "test.csv" in exists_csv[0]

    def test_excute_ng_multiple_target1(self):
        with pytest.raises(InvalidCount) as execinfo:
            # create test file
            target1_file = os.path.join(self._data_dir, "test11.csv")
            open(target1_file, "w").close()
            target1_file = os.path.join(self._data_dir, "test111.csv")
            open(target1_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test2.csv")
            open(target2_file, "w").close()

            # set the essential attributes
            instance = CsvMerge()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src1_pattern", "test1(.*).csv")
            Helper.set_property(instance, "src2_pattern", "test2.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.csv")
            instance.execute()
        assert "must be only one" in str(execinfo.value)

    def test_excute_ng_multiple_target2(self):
        with pytest.raises(InvalidCount) as execinfo:
            # create test file
            target1_file = os.path.join(self._data_dir, "test1.csv")
            open(target1_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test22.csv")
            open(target2_file, "w").close()
            target2_file = os.path.join(self._data_dir, "test222.csv")
            open(target2_file, "w").close()

            # set the essential attributes
            instance = CsvMerge()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src1_pattern", "test1.csv")
            Helper.set_property(instance, "src2_pattern", "test2(.*).csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.csv")
            instance.execute()
        assert "must be only one" in str(execinfo.value)


class TestCsvConcat(TestCsvTransform):
    # TODO Old version test.
    def test_execute_ok1_old(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)

            # create test file
            csv_list1 = [["key", "data"], ["c1", "001"], ["c2", "0.01"], ["c3", "spam"]]
            with open(os.path.join(self._data_dir, "test1.csv"), "w") as t1:
                writer = csv.writer(t1)
                writer.writerows(csv_list1)

            csv_list2 = [["key", "data"], ["d1", "1,23"], ["d2", "ABC"], ["d3", "spam"]]
            with open(os.path.join(self._data_dir, "test2.csv"), "w") as t2:
                writer = csv.writer(t2)
                writer.writerows(csv_list2)

            csv_list3 = [["key", "data"], ["c1", "000"], ["c2", "ABC"], ["c3", "spam"]]
            with open(os.path.join(self._data_dir, "test3.csv"), "w") as t3:
                writer = csv.writer(t3)
                writer.writerows(csv_list3)

            # set the essential attributes
            instance = CsvConcat()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(
                instance, "src_filenames", ["test1.csv", "test2.csv", "test3.csv"]
            )
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()

            with open(os.path.join(self._data_dir, "test.csv")) as t:
                reader = csv.reader(t)
                concatenated_list = [row for row in reader]
        finally:
            shutil.rmtree(self._data_dir)
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "001"],
            ["c2", "0.01"],
            ["c3", "spam"],
            ["d1", "1,23"],
            ["d2", "ABC"],
            ["d3", "spam"],
            ["c1", "000"],
            ["c2", "ABC"],
            ["c3", "spam"],
        ]

    # TODO Old version test.
    def test_execute_ok2_old(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)

            # create test file
            csv_list1 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
            with open(os.path.join(self._data_dir, "test1.csv"), "w") as t1:
                writer = csv.writer(t1)
                writer.writerows(csv_list1)

            csv_list2 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
            with open(os.path.join(self._data_dir, "test2.csv"), "w") as t2:
                writer = csv.writer(t2)
                writer.writerows(csv_list2)

            # set the essential attributes
            instance = CsvConcat()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()

            with open(os.path.join(self._data_dir, "test.csv")) as t:
                reader = csv.reader(t)
                concatenated_list = [row for row in reader]
        finally:
            shutil.rmtree(self._data_dir)
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "spam"],
            ["c2", "spam"],
            ["c1", "spam"],
            ["c2", "spam"],
        ]

    # TODO Old version test.
    def test_execute_ok3_old3(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)

            # create test file
            csv_list1 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
            with open(os.path.join(self._data_dir, "test1.csv"), "w") as t1:
                writer = csv.writer(t1)
                writer.writerows(csv_list1)

            # set the essential attributes
            instance = CsvConcat()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()

            with open(os.path.join(self._data_dir, "test.csv")) as t:
                reader = csv.reader(t)
                concatenated_list = [row for row in reader]
        finally:
            shutil.rmtree(self._data_dir)
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "spam"],
            ["c2", "spam"],
        ]

    # TODO Old version test.
    def test_excute_ng_multiple_target_old(self):
        with pytest.raises(InvalidParameter) as execinfo:
            # set the essential attributes
            instance = CsvConcat()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "src_filenames", ["test1.csv", "test2.csv"])
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()
        assert "Cannot specify both 'src_pattern' and 'src_filenames'." in str(
            execinfo.value
        )

    def test_execute_ok1(self):
        # create test file
        csv_list1 = [["key", "data"], ["c1", "001"], ["c2", "0.01"], ["c3", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [["key", "data"], ["d1", "1,23"], ["d2", "ABC"], ["d3", "spam"]]
        self._create_csv(csv_list2, fname="test2.csv")

        csv_list3 = [["key", "data"], ["c1", "000"], ["c2", "ABC"], ["c3", "spam"]]
        self._create_csv(csv_list3, fname="test3.csv")

        # set the essential attributes
        instance = CsvConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(
            instance, "src_filenames", ["test1.csv", "test2.csv", "test3.csv"]
        )
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.csv")
        instance.execute()

        with open(os.path.join(self._data_dir, "test.csv")) as t:
            reader = csv.reader(t)
            concatenated_list = [row for row in reader]
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "001"],
            ["c2", "0.01"],
            ["c3", "spam"],
            ["d1", "1,23"],
            ["d2", "ABC"],
            ["d3", "spam"],
            ["c1", "000"],
            ["c2", "ABC"],
            ["c3", "spam"],
        ]

    def test_execute_ok2(self):
        # create test file
        csv_list1 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        csv_list2 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
        self._create_csv(csv_list2, fname="test2.csv")

        # set the essential attributes
        instance = CsvConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_name", "test.csv")
        instance.execute()

        with open(os.path.join(self._data_dir, "test.csv")) as t:
            reader = csv.reader(t)
            concatenated_list = [row for row in reader]
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "spam"],
            ["c2", "spam"],
            ["c1", "spam"],
            ["c2", "spam"],
        ]

    def test_execute_ok3(self):
        # create test file
        csv_list1 = [["key", "data"], ["c1", "spam"], ["c2", "spam"]]
        self._create_csv(csv_list1, fname="test1.csv")

        # set the essential attributes
        instance = CsvConcat()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "dest_dir", self._data_dir)
        Helper.set_property(instance, "dest_pattern", "test.csv")
        instance.execute()

        with open(os.path.join(self._data_dir, "test.csv")) as t:
            reader = csv.reader(t)
            concatenated_list = [row for row in reader]
        assert concatenated_list == [
            ["key", "data"],
            ["c1", "spam"],
            ["c2", "spam"],
        ]

    def test_excute_ng_multiple_target(self):
        with pytest.raises(InvalidParameter) as execinfo:
            # set the essential attributes
            instance = CsvConcat()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "src_filenames", ["test1.csv", "test2.csv"])
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_name", "test.csv")
            instance.execute()
        assert "Cannot specify both 'src_pattern' and 'src_filenames'." in str(
            execinfo.value
        )


class TestCsvConvert(TestCsvTransform):
    def test_convert_header(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        test_csv = self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(
            instance, "headers", [{"key": "new_key"}, {"data": "new_data"}]
        )
        instance.execute()

        with open(test_csv, "r") as t:
            reader = csv.reader(t)
            line = next(reader)
        assert line == ["new_key", "new_data"]

    def test_convert_entire(self):
        # create test file
        csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
        self._create_csv(csv_list)

        # set the essential attributes
        instance = CsvConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "headers", [{"key": "new_key"}])
        Helper.set_property(instance, "quote", "QUOTE_ALL")
        Helper.set_property(instance, "after_format", "tsv")
        Helper.set_property(instance, "after_enc", "utf-8")
        Helper.set_property(instance, "after_nl", "CR")
        instance.execute()

        with open(os.path.join(self._data_dir, "test.tsv"), "r", newline="") as t:
            line = t.readline()
            idx = 0
            while line:
                if idx == 0:
                    assert '"new_key"\t"data"' in line
                else:
                    assert '"%s"\t"spam"' % idx in line
                assert line.endswith("\r")

                line = t.readline()
                idx += 1


class TestCsvSort(TestCsvTransform):
    def test_sort(self):
        # create test file
        csv_list = [["key", "data"], ["1", "A"], ["3", "C"], ["2", "B"], ["3", "C"]]
        self._create_csv(csv_list, fname="test1.csv")
        self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvSort()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        Helper.set_property(instance, "order", ["key"])
        Helper.set_property(instance, "quote", "QUOTE_ALL")
        instance.execute()

        files = glob(os.path.join(self._result_dir, "test*.csv"))
        assert 2 == len(files)
        for file in files:
            with open(file, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                record_count = 0
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("key")
                    elif i == 1:
                        assert "2" == row.get("key")
                    elif i == 2 or i == 3:
                        assert "3" == row.get("key")
                    record_count += 1
                assert record_count == 4

    def test_sort_no_duplicate(self):
        # create test file
        csv_list = [["key", "data"], ["1", "A"], ["3", "C"], ["2", "B"], ["3", "C"]]
        self._create_csv(csv_list, fname="test1.csv")
        self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvSort()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        Helper.set_property(instance, "order", ["key"])
        Helper.set_property(instance, "no_duplicate", True)
        Helper.set_property(instance, "quote", "QUOTE_ALL")
        instance.execute()

        files = glob(os.path.join(self._result_dir, "test*.csv"))
        assert 2 == len(files)
        for file in files:
            with open(file, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                record_count = 0
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("key")
                    elif i == 1:
                        assert "2" == row.get("key")
                    elif i == 2 or i == 3:
                        assert "3" == row.get("key")
                    record_count += 1
                assert record_count == 3


class TestCsvToJsonl(TestCsvTransform):
    def test_convert(self):
        # create test file
        csv_list = [["key", "data"], ["1", "A"], ["2", "B"], ["3", "C"]]
        self._create_csv(csv_list, fname="test1.csv")
        self._create_csv(csv_list, fname="test2.csv")

        # set the essential attributes
        instance = CsvToJsonl()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.csv")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        instance.execute()

        files = glob(os.path.join(self._result_dir, "test*.jsonl"))
        assert 2 == len(files)
        for file in files:
            with jsonlines.open(file) as reader:
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("key")
                    elif i == 1:
                        assert "2" == row.get("key")
                    elif i == 2:
                        assert "3" == row.get("key")

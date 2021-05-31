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
    CsvColumnExtract,
    CsvConcat,
    CsvConvert,
    CsvHeaderConvert,
    CsvMerge,
    CsvSort,
    CsvToJsonl,
)
from cliboa.util.exception import InvalidCount, InvalidParameter
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestFileTransform(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")


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


class TestCsvHeaderConvert(TestFileTransform):
    def test_execute_ok(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv = os.path.join(self._data_dir, "test.csv")
            with open(test_csv, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

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
        finally:
            shutil.rmtree(self._data_dir)
        assert line == ["new_key", "new_data"]

    def test_execute_ok_2(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv = os.path.join(self._data_dir, "test.csv")
            with open(test_csv, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

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
        finally:
            shutil.rmtree(self._data_dir)
        assert line == ["new_key", "data"]

    def test_execute_ng_no_src_file(self):
        os.makedirs(self._data_dir, exist_ok=True)
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

    def test_execute_ng_no_multiple_files(self):
        # create test files
        os.makedirs(self._data_dir, exist_ok=True)
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

        shutil.rmtree(self._data_dir)
        assert "only one" in str(execinfo.value)


class TestCsvMerge(TestFileTransform):
    def test_execute_ok(self):
        try:
            # create test file
            csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv1 = os.path.join(self._data_dir, "test1.csv")
            with open(test_csv1, "w") as t1:
                writer = csv.writer(t1)
                writer.writerows(csv_list1)

            csv_list2 = [
                ["key", "address"],
                ["1", "spam"],
                ["2", "spam"],
                ["3", "spam"],
            ]
            test_csv2 = os.path.join(self._data_dir, "test2.csv")
            with open(test_csv2, "w") as t2:
                writer = csv.writer(t2)
                writer.writerows(csv_list2)

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
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]

    def test_execute_ok_with_unnamed(self):
        try:
            # create test file
            csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv1 = os.path.join(self._data_dir, "test1.csv")
            with open(test_csv1, "w") as t1:
                writer = csv.writer(t1)
                writer.writerows(csv_list1)

            csv_list2 = [
                ["key", "Unnamed: 0"],
                ["1", "spam"],
                ["2", "spam"],
                ["3", "spam"],
            ]
            test_csv2 = os.path.join(self._data_dir, "test2.csv")
            with open(test_csv2, "w") as t2:
                writer = csv.writer(t2)
                writer.writerows(csv_list2)

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
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]

    def test_excute_ng_multiple_target1(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir, exist_ok=True)
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
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)

    def test_excute_ng_multiple_target2(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir, exist_ok=True)
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
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)


class TestCsvConcat(TestFileTransform):
    def test_execute_ok1(self):
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

    def test_execute_ok2(self):
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

    def test_execute_ok3(self):
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

    def test_excute_ng_multiple_target(self):
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


class TestCsvConvert(TestFileTransform):
    def test_convert_header(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv = os.path.join(self._data_dir, "test.csv")
            with open(test_csv, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

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
        finally:
            shutil.rmtree(self._data_dir)
        assert line == ["new_key", "new_data"]

    def test_convert_entire(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir, exist_ok=True)
            test_csv = os.path.join(self._data_dir, "test.csv")
            with open(test_csv, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            # set the essential attributes
            instance = CsvConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.csv")
            Helper.set_property(instance, "headers", [{"key": "new_key"}])
            Helper.set_property(instance, "quote", "QUOTE_ALL")
            Helper.set_property(instance, "after_format", "tsv")
            instance.execute()

            with open(os.path.join(self._data_dir, "test.tsv"), "r") as t:
                for i in range(len(csv_list)):
                    line = t.readline()
                    if i == 0:
                        assert line == '"new_key"\t"data"\n'
                    else:
                        assert line == '"%s"\t"%s"\n' % (csv_list[i][0], csv_list[i][1])
        finally:
            shutil.rmtree(self._data_dir)


class TestCsvSort(TestFileTransform):
    def test_sort(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "A"], ["3", "C"], ["2", "B"]]
            result_dir = os.path.join(self._data_dir, "result")
            os.makedirs(result_dir, exist_ok=True)

            test_csv1 = os.path.join(self._data_dir, "test1.csv")
            with open(test_csv1, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            test_csv2 = os.path.join(self._data_dir, "test2.csv")
            with open(test_csv2, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            # set the essential attributes
            instance = CsvSort()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "dest_dir", result_dir)
            Helper.set_property(instance, "order", ["key"])
            instance.execute()

            files = glob(os.path.join(result_dir, "test*.csv"))
            assert 2 == len(files)
            for file in files:
                with open(file, mode='r', encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i == 0:
                            assert "1" == row.get("key")
                        elif i == 1:
                            assert "2" == row.get("key")
                        elif i == 2:
                            assert "3" == row.get("key")
        finally:
            shutil.rmtree(self._data_dir)


class TestCsvToJsonl(TestFileTransform):
    def test_convert(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "A"], ["2", "B"], ["3", "C"]]
            result_dir = os.path.join(self._data_dir, "result")
            os.makedirs(result_dir, exist_ok=True)

            test_csv1 = os.path.join(self._data_dir, "test1.csv")
            with open(test_csv1, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            test_csv2 = os.path.join(self._data_dir, "test2.csv")
            with open(test_csv2, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            # set the essential attributes
            instance = CsvToJsonl()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test.*\.csv")
            Helper.set_property(instance, "dest_dir", result_dir)
            instance.execute()

            files = glob(os.path.join(result_dir, "test*.jsonl"))
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
        finally:
            shutil.rmtree(self._data_dir)

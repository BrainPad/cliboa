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
import pytest
import shutil
import xlsxwriter
from glob import glob
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.transform.file import ExcelConvert, CsvMerge, CsvHeaderConvert
from cliboa.util.exception import InvalidFormat, InvalidCount
from cliboa.util.lisboa_log import LisboaLog


class TestFileTransform(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")


class TestExcelConvert(TestFileTransform):
    def test_execute_ng_invalid_extension(self):
        with pytest.raises(InvalidFormat) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir)
                excel_file = os.path.join(self._data_dir, "test.xlxs")
                open(excel_file, "w").close()
                excel_file2 = os.path.join(self._data_dir, "test.xlxs.bk")
                open(excel_file2, "w").close()

                # set the essential attributes
                instance = ExcelConvert()
                instance.logger = LisboaLog.get_logger(__name__)
                setattr(instance, "src_dir", self._data_dir)
                setattr(instance, "src_pattern", "test\.xlxs")
                setattr(instance, "dest_dir", self._data_dir)
                setattr(instance, "dest_pattern", "test.xlxs")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "not supported" in str(execinfo.value)

    def test_excute_ng_multiple_src(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir)
                excel_file = os.path.join(self._data_dir, "test1.xlxs")
                open(excel_file, "w").close()
                excel_file2 = os.path.join(self._data_dir, "test2.xlxs")
                open(excel_file2, "w").close()

                # set the essential attributes
                instance = ExcelConvert()
                instance.logger = LisboaLog.get_logger(__name__)
                setattr(instance, "src_dir", self._data_dir)
                setattr(instance, "src_pattern", "test(.*)\.xlxs")
                setattr(instance, "dest_dir", self._data_dir)
                setattr(instance, "dest_pattern", "test(.*).xlxs")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)

    def test_execute_ok(self):
        try:
            # create test file
            os.makedirs(self._data_dir)
            excel_file = os.path.join(self._data_dir, "test.xlxs")
            workbook = xlsxwriter.Workbook(excel_file)
            workbook.close()

            # set the essential attributes
            instance = ExcelConvert()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src_pattern", "test\.xlxs")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test.csv")
            instance.execute()

            exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]


class TestCsvMerge(TestFileTransform):
    def test_execute_ok(self):
        try:
            # create test file
            csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir)
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
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src1_pattern", "test1\.csv")
            setattr(instance, "src2_pattern", "test2\.csv")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test.csv")
            instance.execute()

            exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]

    def test_execute_ok_with_unnamed(self):
        try:
            # create test file
            csv_list1 = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir)
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
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src1_pattern", "test1\.csv")
            setattr(instance, "src2_pattern", "test2\.csv")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test.csv")
            instance.execute()

            exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]

    def test_excute_ng_multiple_target1(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir)
                target1_file = os.path.join(self._data_dir, "test11.csv")
                open(target1_file, "w").close()
                target1_file = os.path.join(self._data_dir, "test111.csv")
                open(target1_file, "w").close()
                target2_file = os.path.join(self._data_dir, "test2.csv")
                open(target2_file, "w").close()

                # set the essential attributes
                instance = CsvMerge()
                instance.logger = LisboaLog.get_logger(__name__)
                setattr(instance, "src_dir", self._data_dir)
                setattr(instance, "src1_pattern", "test1(.*).csv")
                setattr(instance, "src2_pattern", "test2.csv")
                setattr(instance, "dest_dir", self._data_dir)
                setattr(instance, "dest_pattern", "test.csv")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)

    def test_excute_ng_multiple_target2(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir)
                target1_file = os.path.join(self._data_dir, "test1.csv")
                open(target1_file, "w").close()
                target2_file = os.path.join(self._data_dir, "test22.csv")
                open(target2_file, "w").close()
                target2_file = os.path.join(self._data_dir, "test222.csv")
                open(target2_file, "w").close()

                # set the essential attributes
                instance = CsvMerge()
                instance.logger = LisboaLog.get_logger(__name__)
                setattr(instance, "src_dir", self._data_dir)
                setattr(instance, "src1_pattern", "test1.csv")
                setattr(instance, "src2_pattern", "test2(.*).csv")
                setattr(instance, "dest_dir", self._data_dir)
                setattr(instance, "dest_pattern", "test.csv")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)


class TestCsvHeaderConvert(TestFileTransform):
    def test_execute_ok(self):
        try:
            # create test file
            csv_list = [["key", "data"], ["1", "spam"], ["2", "spam"], ["3", "spam"]]
            os.makedirs(self._data_dir)
            test_csv = os.path.join(self._data_dir, "test.csv")
            with open(test_csv, "w") as t:
                writer = csv.writer(t)
                writer.writerows(csv_list)

            # set the essential attributes
            instance = CsvHeaderConvert()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src_pattern", "test\.csv")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test_new.csv")
            setattr(instance, "headers", [{"key": "new_key"}, {"data": "new_data"}])
            instance.execute()

            test_new_csv = os.path.join(self._data_dir, "test_new.csv")
            with open(test_new_csv, "r") as t:
                reader = csv.reader(t)
                l = next(reader)
        finally:
            shutil.rmtree(self._data_dir)
        assert l == ["new_key", "new_data"]

    def test_execute_ng_no_src_file(self):
        os.makedirs(self._data_dir)
        with pytest.raises(InvalidCount) as execinfo:
            # set the essential attributes
            instance = CsvHeaderConvert()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src_pattern", "test\.csv")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test_new.csv")
            setattr(instance, "headers", [{"key": "new_key"}, {"data": "new_data"}])
            instance.execute()

        shutil.rmtree(self._data_dir)
        assert "not exist" in str(execinfo.value)

    def test_execute_ng_no_multiple_files(self):
        # create test files
        os.makedirs(self._data_dir)
        test1_csv = os.path.join(self._data_dir, "test1.csv")
        test2_csv = os.path.join(self._data_dir, "test2.csv")
        open(test1_csv, "w").close
        open(test2_csv, "w").close

        with pytest.raises(InvalidCount) as execinfo:
            # set the essential attributes
            instance = CsvHeaderConvert()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "src_dir", self._data_dir)
            setattr(instance, "src_pattern", "test(.*)\.csv")
            setattr(instance, "dest_dir", self._data_dir)
            setattr(instance, "dest_pattern", "test_new.csv")
            setattr(instance, "headers", [{"key": "new_key"}, {"data": "new_data"}])
            instance.execute()

        shutil.rmtree(self._data_dir)
        assert "only one" in str(execinfo.value)

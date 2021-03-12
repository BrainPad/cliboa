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
import shutil
from glob import glob

import pytest
import xlsxwriter

from cliboa.conf import env
from cliboa.scenario.transform.file import ExcelConvert, FileConvert
from cliboa.util.exception import InvalidCount, InvalidFormat
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestFileTransform(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")


class TestExcelConvert(TestFileTransform):
    def test_execute_ng_invalid_extension(self):
        with pytest.raises(InvalidFormat) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir, exist_ok=True)
                excel_file = os.path.join(self._data_dir, "test.xlxs")
                open(excel_file, "w").close()
                excel_file2 = os.path.join(self._data_dir, "test.xlxs.bk")
                open(excel_file2, "w").close()

                # set the essential attributes
                instance = ExcelConvert()
                Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
                Helper.set_property(instance, "src_dir", self._data_dir)
                Helper.set_property(instance, "src_pattern", "test.xlxs")
                Helper.set_property(instance, "dest_dir", self._data_dir)
                Helper.set_property(instance, "dest_pattern", "test.xlxs")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "not supported" in str(execinfo.value)

    def test_excute_ng_multiple_src(self):
        with pytest.raises(InvalidCount) as execinfo:
            try:
                # create test file
                os.makedirs(self._data_dir, exist_ok=True)
                excel_file = os.path.join(self._data_dir, "test1.xlxs")
                open(excel_file, "w").close()
                excel_file2 = os.path.join(self._data_dir, "test2.xlxs")
                open(excel_file2, "w").close()

                # set the essential attributes
                instance = ExcelConvert()
                Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
                Helper.set_property(instance, "src_dir", self._data_dir)
                Helper.set_property(instance, "src_pattern", r"test(.*)\.xlxs")
                Helper.set_property(instance, "dest_dir", self._data_dir)
                Helper.set_property(instance, "dest_pattern", r"test(.*).xlxs")
                instance.execute()
            finally:
                shutil.rmtree(self._data_dir)
        assert "must be only one" in str(execinfo.value)

    def test_execute_ok(self):
        try:
            # create test file
            os.makedirs(self._data_dir, exist_ok=True)
            excel_file = os.path.join(self._data_dir, "test.xlxs")
            workbook = xlsxwriter.Workbook(excel_file)
            workbook.close()

            # set the essential attributes
            instance = ExcelConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.xlxs")
            Helper.set_property(instance, "dest_dir", self._data_dir)
            Helper.set_property(instance, "dest_pattern", "test.csv")
            instance.execute()

            exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        finally:
            shutil.rmtree(self._data_dir)
        assert "test.csv" in exists_csv[0]


class TestFileConvert(TestFileTransform):
    def test_execute(self):
        STR_UTF8 = "いろはにほへと"
        try:
            # create test file
            os.makedirs(self._data_dir, exist_ok=True)
            test_file = os.path.join(self._data_dir, "test.txt")

            with open(test_file, "w") as t:
                t.write(STR_UTF8)

            # set the essential attributes
            instance = FileConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.txt")
            Helper.set_property(instance, "encoding_from", "utf-8")
            Helper.set_property(instance, "encoding_to", "utf-16")
            instance.execute()

            with open(test_file, errors="ignore") as t:
                str_utf16 = t.read()

            assert str_utf16 != STR_UTF8

            # set the essential attributes
            instance = FileConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.txt")
            Helper.set_property(instance, "encoding_from", "utf-16")
            Helper.set_property(instance, "encoding_to", "utf-8")
            instance.execute()

            with open(test_file) as t:
                str_utf8 = t.read()

            assert str_utf8 == STR_UTF8

        finally:
            shutil.rmtree(self._data_dir)

    def test_execute_encode_error_ignore(self):
        STR_UTF8 = "いろはにほへと☺"
        try:
            # create test file
            os.makedirs(self._data_dir, exist_ok=True)
            test_file = os.path.join(self._data_dir, "test.txt")

            with open(test_file, "w") as t:
                t.write(STR_UTF8)

            # set the essential attributes
            instance = FileConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.txt")
            Helper.set_property(instance, "encoding_from", "utf-8")
            Helper.set_property(instance, "encoding_to", "shift_jis")
            Helper.set_property(instance, "errors", "ignore")
            instance.execute()

            with open(test_file, encoding="shift_jis", errors="ignore") as t:
                str_output = t.read()

            assert str_output == "いろはにほへと"

        finally:
            shutil.rmtree(self._data_dir)

    def test_execute_encode_error_strict(self):
        STR_UTF8 = "いろはにほへと☺"
        try:
            # create test file
            os.makedirs(self._data_dir, exist_ok=True)
            test_file = os.path.join(self._data_dir, "test.txt")

            with open(test_file, "w") as t:
                t.write(STR_UTF8)

            # set the essential attributes
            instance = FileConvert()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"test\.txt")
            Helper.set_property(instance, "encoding_from", "utf-8")
            Helper.set_property(instance, "encoding_to", "shift_jis")
            Helper.set_property(instance, "errors", "strict")
            with pytest.raises(UnicodeEncodeError):
                instance.execute()

        finally:
            shutil.rmtree(self._data_dir)

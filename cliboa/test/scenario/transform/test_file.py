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
import bz2
import csv
import gzip
import os
import pytest
import shutil
import tarfile
import xlsxwriter
import zipfile

from glob import glob
from cliboa.conf import env
from cliboa.scenario.transform.file import (
    DateFormatConvert,
    ExcelConvert,
    FileArchive,
    FileBaseTransform,
    FileCompress,
    FileConvert,
    FileDecompress,
    FileDivide,
    FileRename,
)
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import (
    CliboaException,
    FileNotFound,
    InvalidCount,
    InvalidFormat,
    InvalidParameter,
)
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestFileTransform(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        self._out_dir = os.path.join(env.BASE_DIR, "data", "out")
        os.makedirs(self._data_dir, exist_ok=True)
        os.makedirs(self._out_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def _create_files(self):
        file1 = os.path.join(self._data_dir, "test1.txt")
        with open(file1, mode="w", encoding="utf-8") as f:
            f.write("This is test 1")

        file2 = os.path.join(self._data_dir, "test2.txt")
        with open(file2, mode="w", encoding="utf-8") as f:
            f.write("This is test 2")
        return [file1, file2]


class TestFileTransformFunctions(TestFileTransform):
    def test_execute(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", "in")
        Helper.set_property(instance, "src_pattern", r".*")
        Helper.set_property(instance, "dest_dir", "out")
        Helper.set_property(instance, "dest_name", "result.txt")
        Helper.set_property(instance, "encoding", "utf-8")
        Helper.set_property(instance, "nonfile_error", True)
        instance.execute()

    def test_io_files_unset_directory(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for fi, fo in instance.io_files(files):
            pass
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

    def test_io_files_same_directory(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "dest_dir", self._data_dir)
        files = self._create_files()
        for fi, fo in instance.io_files(files):
            pass
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

    def test_io_files_different_directory(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "dest_dir", self._out_dir)
        files = self._create_files()
        for fi, fo in instance.io_files(files):
            pass
        assert os.path.exists(os.path.join(self._out_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._out_dir, "test2.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

    def test_io_files_secret_file_name(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "dest_dir", self._out_dir)
        files = []
        for file in self._create_files():
            root, name = os.path.split(file)
            renamed_file = os.path.join(root, "." + name)
            os.rename(file, renamed_file)
            files.append(renamed_file)
        for fi, fo in instance.io_files(files):
            pass
        assert os.path.exists(os.path.join(self._out_dir, ".test1.txt")) is True
        assert os.path.exists(os.path.join(self._out_dir, ".test2.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, ".test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, ".test2.txt")) is True

    def test_io_files_specify_ext(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for fi, fo in instance.io_files(files, ext="csv"):
            pass
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test1.csv")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.csv")) is True

    def test_io_files_specify_ext_2(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for fi, fo in instance.io_files(files, ext=".csv"):
            pass
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test1.csv")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.csv")) is True

    def test_io_writers_write_text(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for reader, writer in instance.io_writers(files):
            writer.write(reader.read())
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

        with open(os.path.join(self._data_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(os.path.join(self._data_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_io_writers_write_binary(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for reader, writer in instance.io_writers(files, mode="b"):
            writer.write(reader.read())
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

        with open(os.path.join(self._data_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(os.path.join(self._data_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_io_writers_overwrite_text(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        for reader, writer in instance.io_writers(files):
            writer.write(reader.read())
            writer.write("--OK--")
        assert os.path.exists(os.path.join(self._data_dir, "test1.txt")) is True
        assert os.path.exists(os.path.join(self._data_dir, "test2.txt")) is True

        with open(os.path.join(self._data_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1--OK--" == f.read()
        with open(os.path.join(self._data_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2--OK--" == f.read()

    def test_io_writers_invalid_parameter(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        files = self._create_files()
        with pytest.raises(InvalidParameter) as execinfo:
            for reader, writer in instance.io_writers(files, mode="s"):
                pass
        assert "Unknown mode. One of the following is allowed [t, b]" == str(
            execinfo.value
        )

    def test_file_check_exist(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        ret = instance.check_file_existence(["test.txt"])
        assert ret is None

    def test_file_check_nofile_error(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "nonfile_error", True)
        with pytest.raises(FileNotFound) as execinfo:
            instance.check_file_existence([])
        assert "No files are found." == str(execinfo.value)

    def test_file_check_nofile_noerror(self):
        instance = FileBaseTransform()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "nonfile_error", False)
        ret = instance.check_file_existence([])
        assert ret is None


class TestFileDecompress(TestFileTransform):
    def test_zip(self):
        files = self._create_files()
        for file in files:
            with zipfile.ZipFile(
                os.path.join(self._data_dir, (os.path.basename(file) + ".zip")),
                "w",
                zipfile.ZIP_DEFLATED,
            ) as o:
                o.write(file, arcname=os.path.basename(file))

        instance = FileDecompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt\.zip")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        instance.execute()

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        assert os.path.exists(decompressed_file_1)
        assert os.path.exists(decompressed_file_2)
        with open(decompressed_file_1, encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(decompressed_file_2, encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_gz(self):
        files = self._create_files()
        for file in files:
            com_path = os.path.join(self._data_dir, (os.path.basename(file) + ".gz"))
            with open(file, "rb") as i, gzip.open(com_path, "wb") as o:
                while True:
                    buf = i.read()
                    if buf == b"":
                        break
                    o.write(buf)

        instance = FileDecompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt\.gz")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "chunk_size", 1024)
        instance.execute()

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        assert os.path.exists(decompressed_file_1)
        assert os.path.exists(decompressed_file_2)
        with open(decompressed_file_1, encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(decompressed_file_2, encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_bz2(self):
        files = self._create_files()
        for file in files:
            com_path = os.path.join(self._data_dir, (os.path.basename(file) + ".bz2"))
            with open(file, "rb") as i, bz2.open(com_path, "wb") as o:
                while True:
                    buf = i.read()
                    if buf == b"":
                        break
                    o.write(buf)

        instance = FileDecompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt\.bz2")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "chunk_size", 1024)
        instance.execute()

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        assert os.path.exists(decompressed_file_1)
        assert os.path.exists(decompressed_file_2)
        with open(decompressed_file_1, encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(decompressed_file_2, encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_tar(self):
        self._create_files()
        with tarfile.open(os.path.join(self._data_dir, "test.tar"), mode="w") as f:
            f.add(self._data_dir, arcname="/")

        instance = FileDecompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.tar")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        instance.execute()

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        assert os.path.exists(decompressed_file_1)
        assert os.path.exists(decompressed_file_2)
        with open(decompressed_file_1, encoding="utf-8") as f:
            assert "This is test 1" == f.read()
        with open(decompressed_file_2, encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_unsupported_type(self):
        file1 = os.path.join(self._data_dir, "test.rar")
        with open(file1, mode="w", encoding="utf-8") as f:
            f.write("This is test 1")

        instance = FileDecompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.rar")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        with pytest.raises(CliboaException) as execinfo:
            instance.execute()
        assert "Unmatched any available decompress type" in str(execinfo.value)


class TestFileCompress(TestFileTransform):
    def test_zip(self):
        self._create_files()

        instance = FileCompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "format", "zip")
        Helper.set_property(instance, "chunk_size", 1024)
        instance.execute()

        compressed_file_1 = os.path.join(self._out_dir, "test1.txt.zip")
        compressed_file_2 = os.path.join(self._out_dir, "test2.txt.zip")
        assert os.path.exists(compressed_file_1)
        assert os.path.exists(compressed_file_2)

        with zipfile.ZipFile(compressed_file_1) as zp:
            zp.extractall(self._out_dir)
        with open(os.path.join(self._out_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1" == f.read()

        with zipfile.ZipFile(compressed_file_2) as zp:
            zp.extractall(self._out_dir)
        with open(os.path.join(self._out_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_gz(self):
        self._create_files()

        instance = FileCompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "format", "gz")
        instance.execute()

        compressed_file_1 = os.path.join(self._out_dir, "test1.txt.gz")
        compressed_file_2 = os.path.join(self._out_dir, "test2.txt.gz")
        assert os.path.exists(compressed_file_1)
        assert os.path.exists(compressed_file_2)

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        with gzip.open(compressed_file_1, "rb") as i, open(
            decompressed_file_1, "wb"
        ) as o:
            while True:
                buf = i.read()
                if buf == b"":
                    break
                o.write(buf)
        with open(os.path.join(self._out_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1" == f.read()

        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        with gzip.open(compressed_file_2, "rb") as i, open(
            decompressed_file_2, "wb"
        ) as o:
            while True:
                buf = i.read()
                if buf == b"":
                    break
                o.write(buf)
        with open(os.path.join(self._out_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2" == f.read()

    def test_bz2(self):
        self._create_files()

        instance = FileCompress()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "format", "bz2")
        instance.execute()

        compressed_file_1 = os.path.join(self._out_dir, "test1.txt.bz2")
        compressed_file_2 = os.path.join(self._out_dir, "test2.txt.bz2")
        assert os.path.exists(compressed_file_1)
        assert os.path.exists(compressed_file_2)

        decompressed_file_1 = os.path.join(self._out_dir, "test1.txt")
        with bz2.open(compressed_file_1, mode="rb") as i, open(
            decompressed_file_1, mode="wb"
        ) as o:
            while True:
                buf = i.read()
                if buf == b"":
                    break
                o.write(buf)
        with open(os.path.join(self._out_dir, "test1.txt"), encoding="utf-8") as f:
            assert "This is test 1" == f.read()

        decompressed_file_2 = os.path.join(self._out_dir, "test2.txt")
        with bz2.open(compressed_file_2, mode="rb") as i, open(
            decompressed_file_2, mode="wb"
        ) as o:
            while True:
                buf = i.read()
                if buf == b"":
                    break
                o.write(buf)
        with open(os.path.join(self._out_dir, "test2.txt"), encoding="utf-8") as f:
            assert "This is test 2" == f.read()


class TestDateFormatConvert(TestFileTransform):
    # TODO Old version test.
    def test_convert_ok_old(self):
        src = os.path.join(self._data_dir, "test.csv")
        obj = [
            {"No": "1", "date": "2021/01/01 12:00:00"},
        ]
        with open(src, mode="w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, list(obj[0].keys()), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for r in obj:
                writer.writerow(r)

        instance = DateFormatConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "columns", ["date"])
        Helper.set_property(instance, "formatter", "%Y-%m-%d %H:%M")
        dest_path = os.path.join(self._out_dir, "test.csv")
        Helper.set_property(instance, "dest_path", dest_path)
        instance.execute()

        with open(dest_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                assert "2021-01-01 12:00" == row.get("date")

    def test_convert_ok(self):
        src = os.path.join(self._data_dir, "test.csv")
        obj = [
            {"No": "1", "date": "2021/01/01 12:00:00"},
        ]
        with open(src, mode="w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, list(obj[0].keys()), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for r in obj:
                writer.writerow(r)

        instance = DateFormatConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.csv")
        Helper.set_property(instance, "columns", ["date"])
        Helper.set_property(instance, "formatter", "%Y-%m-%d %H:%M")
        instance.execute()

        with open(src, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                assert "2021-01-01 12:00" == row.get("date")


class TestExcelConvert(TestFileTransform):
    # TODO Old version test.
    def test_execute_ng_invalid_extension_old(self):
        with pytest.raises(InvalidFormat) as execinfo:
            # create test file
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
        assert "not supported" in str(execinfo.value)

    # TODO Old version test.
    def test_excute_ng_multiple_src_old(self):
        with pytest.raises(InvalidCount) as execinfo:
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

        assert "must be only one" in str(execinfo.value)

    # TODO Old version test.
    def test_execute_ok_old(self):
        # create test file
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
        assert "test.csv" in exists_csv[0]

    def test_convert_ok(self):
        excel_file = os.path.join(self._data_dir, "test.xlxs")
        workbook = xlsxwriter.Workbook(excel_file)
        workbook.close()

        # set the essential attributes
        instance = ExcelConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.xlxs")
        instance.execute()

        exists_csv = glob(os.path.join(self._data_dir, "test.csv"))
        assert "test.csv" in exists_csv[0]


class TestFileDivide(TestFileTransform):
    # TODO Old version test.
    def test_execute_old(self):
        file1 = os.path.join(self._data_dir, "test.txt")
        with open(file1, mode="w", encoding="utf-8") as f:
            for i in range(100):
                f.write("%s\n" % str(i))

        instance = FileDivide()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "dest_pattern", "out.txt")
        Helper.set_property(instance, "divide_rows", 10)
        instance.execute()

        row_index = 0
        for i in range(1, 11):
            file = os.path.join(self._out_dir, "out.%s.txt" % i)
            assert os.path.exists(file)
            with open(file, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if line:
                        assert str(row_index) == line.splitlines()[0]
                        row_index += 1
                    else:
                        break

    def test_execute_ok(self):
        file1 = os.path.join(self._data_dir, "test.txt")
        with open(file1, mode="w", encoding="utf-8") as f:
            f.write("idx\n")
            for i in range(100):
                f.write("%s\n" % str(i))

        instance = FileDivide()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "divide_rows", 10)
        Helper.set_property(instance, "header", True)
        instance.execute()

        row_index = 0
        for i in range(1, 11):
            file = os.path.join(self._out_dir, "test.%s.txt" % i)
            assert os.path.exists(file)
            with open(file, "r", encoding="utf-8", newline="") as f:
                line = f.readline()
                assert line == "idx\n"
                while line:
                    line = f.readline()
                    if line:
                        assert str(row_index) == line.splitlines()[0]
                        row_index += 1

    def test_execute_ok_2(self):
        file1 = os.path.join(self._data_dir, ".test.txt")
        with open(file1, mode="w", encoding="utf-8") as f:
            for i in range(100):
                f.write("%s\n" % str(i))

        instance = FileDivide()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"\.test\.txt")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "divide_rows", 10)
        instance.execute()

        row_index = 0
        for i in range(1, 11):
            file = os.path.join(self._out_dir, ".test.%s.txt" % i)
            assert os.path.exists(file)
            with open(file, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if line:
                        assert str(row_index) == line.splitlines()[0]
                        row_index += 1
                    else:
                        break

    def test_execute_ok_3(self):
        file1 = os.path.join(self._data_dir, "test.txt.12345")
        with open(file1, mode="w", encoding="utf-8") as f:
            for i in range(100):
                f.write("%s\n" % str(i))

        instance = FileDivide()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt.12345")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "divide_rows", 10)
        instance.execute()

        row_index = 0
        for i in range(1, 11):
            file = os.path.join(self._out_dir, "test.%s.txt.12345" % i)
            assert os.path.exists(file)
            with open(file, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if line:
                        assert str(row_index) == line.splitlines()[0]
                        row_index += 1
                    else:
                        break

    def test_execute_ok_4(self):
        file1 = os.path.join(self._data_dir, "test")
        with open(file1, mode="w", encoding="utf-8") as f:
            for i in range(100):
                f.write("%s\n" % str(i))

        instance = FileDivide()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test")
        Helper.set_property(instance, "dest_dir", self._out_dir)
        Helper.set_property(instance, "divide_rows", 10)
        instance.execute()

        row_index = 0
        for i in range(1, 11):
            file = os.path.join(self._out_dir, "test.%s" % i)
            assert os.path.exists(file)
            with open(file, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if line:
                        assert str(row_index) == line.splitlines()[0]
                        row_index += 1
                    else:
                        break


class TestFileRename(TestFileTransform):
    def test_execute_ok(self):
        self._create_files()

        instance = FileRename()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt")
        Helper.set_property(instance, "prefix", "PRE-")
        Helper.set_property(instance, "suffix", "-SUF")
        instance.execute()

        assert os.path.exists(os.path.join(self._data_dir, "PRE-test1-SUF.txt"))
        assert os.path.exists(os.path.join(self._data_dir, "PRE-test2-SUF.txt"))

    def test_execute_ok_2(self):
        files = self._create_files()
        for file in files:
            root, name = os.path.split(file)
            os.rename(file, os.path.join(root, "." + name))

        instance = FileRename()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"\.test.*\.txt")
        Helper.set_property(instance, "prefix", "PRE-")
        Helper.set_property(instance, "suffix", "-SUF")
        instance.execute()

        assert os.path.exists(os.path.join(self._data_dir, "PRE-.test1-SUF.txt"))
        assert os.path.exists(os.path.join(self._data_dir, "PRE-.test2-SUF.txt"))

    def test_execute_ok_3(self):
        self._create_files()
        files = self._create_files()
        for file in files:
            root, name = os.path.split(file)
            os.rename(file, os.path.join(root, name + ".12345"))

        instance = FileRename()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*\.txt\.12345")
        Helper.set_property(instance, "prefix", "PRE-")
        Helper.set_property(instance, "suffix", "-SUF")
        instance.execute()

        assert os.path.exists(os.path.join(self._data_dir, "PRE-test1-SUF.txt.12345"))
        assert os.path.exists(os.path.join(self._data_dir, "PRE-test2-SUF.txt.12345"))

    def test_execute_ok_4(self):
        self._create_files()
        files = self._create_files()
        for file in files:
            root, name = os.path.split(file)
            os.rename(file, os.path.join(root, os.path.splitext(name)[0]))

        instance = FileRename()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test.*")
        Helper.set_property(instance, "prefix", "PRE-")
        Helper.set_property(instance, "suffix", "-SUF")
        instance.execute()

        assert os.path.exists(os.path.join(self._data_dir, "PRE-test1-SUF"))
        assert os.path.exists(os.path.join(self._data_dir, "PRE-test2-SUF"))


class TestFileConvert(TestFileTransform):
    def test_execute(self):
        STR_UTF8 = "いろはにほへと"
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w", encoding="utf-8") as t:
            t.write(STR_UTF8)

        # set the essential attributes
        instance = FileConvert()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "encoding_from", "utf-8")
        Helper.set_property(instance, "encoding_to", "utf-16")
        instance.execute()

        with open(test_file, mode="rb") as t:
            ret = t.read()
        with pytest.raises(UnicodeDecodeError):
            ret.decode("utf-8")
        try:
            ret.decode("utf-16")
        except Exception:
            pytest.fail("Converted file was not utf-16 encoded.")

    def test_execute_encode_error_ignore(self):
        STR_UTF8 = "いろはにほへと髙"

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

    def test_execute_encode_error_strict(self):
        STR_UTF8 = "いろはにほへと髙"

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


class TestFileArchive(TestFileTransform):
    # TODO Old version test.
    def test_compress_tar_old(self):
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "tar")
        Helper.set_property(instance, "dest_pattern", "foo")
        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.tar"))
        assert 1 == len(files)
        assert "foo.tar" == os.path.basename(files[0])

    # TODO Old version test.
    def test_compress_zip_old(self):
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "zip")
        Helper.set_property(instance, "dest_pattern", "foo")
        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.zip"))
        assert 1 == len(files)
        assert "foo.zip" == os.path.basename(files[0])

    # TODO Old version test.
    def test_compress_with_path_old(self):
        result_dir = os.path.join(self._data_dir, "out")
        os.makedirs(result_dir, exist_ok=True)
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "zip")
        Helper.set_property(instance, "dest_dir", result_dir)
        Helper.set_property(instance, "dest_pattern", "foo")
        instance.execute()

        files = glob(os.path.join(result_dir, "foo.zip"))
        assert 1 == len(files)
        assert "foo.zip" == os.path.basename(files[0])

    def test_compress_tar(self):
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "tar")
        Helper.set_property(instance, "dest_name", "foo")
        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.tar"))
        assert 1 == len(files)
        assert "foo.tar" == os.path.basename(files[0])

    def test_compress_zip(self):
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "zip")
        Helper.set_property(instance, "dest_name", "foo")
        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.zip"))
        assert 1 == len(files)
        assert "foo.zip" == os.path.basename(files[0])

    def test_compress_with_path(self):
        result_dir = os.path.join(self._data_dir, "out")
        os.makedirs(result_dir, exist_ok=True)
        test_file = os.path.join(self._data_dir, "test.txt")

        with open(test_file, "w") as t:
            t.write("ABCDEF")

        # set the essential attributes
        instance = FileArchive()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", r"test\.txt")
        Helper.set_property(instance, "format", "zip")
        Helper.set_property(instance, "dest_dir", result_dir)
        Helper.set_property(instance, "dest_name", "foo")
        instance.execute()

        files = glob(os.path.join(result_dir, "foo.zip"))
        assert 1 == len(files)
        assert "foo.zip" == os.path.basename(files[0])

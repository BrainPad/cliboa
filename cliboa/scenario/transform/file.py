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
import re
import shutil
import tarfile
import tempfile
import zipfile

import pandas

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.scenario.extras import ExceptionHandler
from cliboa.util.date import DateUtil
from cliboa.util.exception import CliboaException, FileNotFound, InvalidParameter
from cliboa.util.file import File


class FileBaseTransform(BaseStep, ExceptionHandler):
    """
    Base class of file transform classes

    Basically transform class of Cliboa is that find files,
    do something, and output transformed files.
    When output files are created, name of the files will be the same name with the input files
    (original input files will be changed to the transformed files).
    If you would not like to remove the original files,
    give a path to the "dest_dir" for output directory.

    Note:
    Output files are not always be the same name with the input file names.
    See documentation for individual classes for details.
    """

    def __init__(self):
        super().__init__()
        self._src_dir = ""
        self._src_pattern = ""
        self._dest_dir = None
        self._dest_name = None
        self._encoding = "utf-8"
        self._nonfile_error = False
        self._force_continue = False

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def dest_name(self, dest_name):
        self._dest_name = dest_name

    def encoding(self, encoding):
        self._encoding = encoding

    def nonfile_error(self, nonfile_error):
        self._nonfile_error = nonfile_error

    def force_continue(self, force_continue):
        self._force_continue = force_continue

    def execute(self, *args):
        pass

    def check_file_existence(self, files):
        """
        Check whether files exist.
        If no files, the scenario will continue or an error is raised,
        depends on parameter[nonfile_error].
        """
        if len(files) == 0:
            if self._nonfile_error is True:
                raise FileNotFound("No files are found.")
            else:
                self._logger.info("No files are found. Nothing to do.")
                return
        self._logger.info("Files found %s" % files)

    def check_output_path(self, input_path, ext):
        root, name = os.path.split(input_path)

        if ext:
            if ext.startswith("."):
                output_name = os.path.splitext(name)[0] + ext
            else:
                output_name = os.path.splitext(name)[0] + "." + ext
        else:
            output_name = name

        if self._dest_dir:
            output_dir = self._dest_dir
        else:
            output_dir = root

        output_path = os.path.join(output_dir, output_name)

        fd, temp_file = tempfile.mkstemp()
        os.close(fd)
        return output_path, temp_file

    def overwrite_output_path(self, input_path, output_path, temp_file):
        if input_path == output_path:
            os.remove(input_path)
        shutil.move(temp_file, output_path)

    def io_files(self, iterable, ext=None, func=None):
        """
        Iteration which returns input and output path.
        If the parameter "dest_dir" was given, the output file will be created under
        the given directory and returns input and output path.
        If not, the output file will be created to the same directory to the input file.

        Arguments:
            iterable (list): Input file list
            ext=None (str): Set an extension for output file,
                            if input and output extension would like to be changed.
                            "." is not necessary.
        """
        for input_path in iterable:
            output_path, temp_file = self.check_output_path(input_path, ext)

            try:
                func(input_path, temp_file)
            except Exception as e:
                if self._force_continue is True:
                    self.handle_error(e, input_path)
                else:
                    raise e

            self.overwrite_output_path(input_path, output_path, temp_file)

    def io_writers(self, iterable, mode="t", encoding="utf-8", ext=None):
        """
        Iteration which returns input and output writer.
        If the parameter "dest_dir" was given, the output file will be created under
        the given directory and returns input and output writer.
        If not, the output file will be created to the same directory to the input file.

        Arguments:
            iterable (list): Input file list
            mode="t" (str): Mode to open input files, opening file with text mode by default.
            encoding="utf-8" (str): File encoding. It will be ignored when mode is "b"
            ext=None (str): Set an extension for output file,
                            if input and output extension would like to be changed.
                            "." is not necessary.
        """
        if not "t" == mode and not "b" == mode:
            raise InvalidParameter("Unknown mode. One of the following is allowed [t, b]")

        if "b" in mode:
            encoding = None

        for input_path in iterable:
            output_path, temp_file = self.check_output_path(input_path, ext)

            if mode == "t":
                with open(input_path, mode="r", encoding=encoding, newline="") as i, open(
                    temp_file, mode="w", encoding=encoding, newline=""
                ) as o:
                    yield i, o
            elif mode == "b":
                with open(input_path, mode="rb") as i, open(temp_file, mode="wb") as o:
                    yield i, o

            self.overwrite_output_path(input_path, output_path, temp_file)


class FileDecompress(FileBaseTransform):
    """
    Decompress the specified file
    """

    def __init__(self):
        super().__init__()
        self._chunk_size = None
        self._password = None

    def chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    def password(self, password):
        self._password = password

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for f in files:
            _, ext = os.path.splitext(f)
            if ext == ".zip":
                self._logger.info("Decompress zip file %s" % f)
                if self._password is not None:
                    pwd = self._password.encode(self._encoding)
                else:
                    pwd = self._password
                with zipfile.ZipFile(f) as zp:
                    zp.extractall(
                        self._dest_dir if self._dest_dir is not None else self._src_dir, pwd=pwd
                    ),
            elif ext == ".tar":
                self._logger.info("Decompress tar file %s" % f)
                with tarfile.open(f, "r:*") as tf:
                    tf.extractall(self._dest_dir if self._dest_dir is not None else self._src_dir)
            elif ext == ".bz2":
                self._logger.info("Decompress bz2 file %s" % f)
                dcom_name = os.path.splitext(os.path.basename(f))[0]
                decom_path = (
                    os.path.join(self._dest_dir, dcom_name)
                    if self._dest_dir is not None
                    else os.path.join(self._src_dir, dcom_name)
                )
                with bz2.open(f, mode="rb") as i, open(decom_path, mode="wb") as o:
                    while True:
                        buf = i.read(self._chunk_size)
                        if buf == b"":
                            break
                        o.write(buf)
            elif ext == ".gz":
                self._logger.info("Decompress gz file %s" % f)
                dcom_name = os.path.splitext(os.path.basename(f))[0]
                decom_path = (
                    os.path.join(self._dest_dir, dcom_name)
                    if self._dest_dir is not None
                    else os.path.join(self._src_dir, dcom_name)
                )
                with gzip.open(f, "rb") as i, open(decom_path, "wb") as o:
                    while True:
                        buf = i.read(self._chunk_size)
                        if buf == b"":
                            break
                        o.write(buf)
            else:
                raise CliboaException("Unmatched any available decompress type %s" % f)


class FileCompress(FileBaseTransform):
    """
    Compress files
    """

    def __init__(self):
        super().__init__()
        self._format = None
        self._chunk_size = None

    def format(self, format):
        self._format = format.lower()

    def chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._format]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        dir = self._dest_dir if self._dest_dir is not None else self._src_dir
        for f in files:
            if self._format == "zip":
                self._logger.info("Compress file %s to zip." % f)
                with zipfile.ZipFile(
                    os.path.join(dir, (os.path.basename(f) + ".zip")),
                    "w",
                    zipfile.ZIP_DEFLATED,
                ) as o:
                    o.write(f, arcname=os.path.basename(f))
            elif self._format in ("gz", "gzip"):
                self._logger.info("Compress file %s to gzip." % f)
                com_path = os.path.join(dir, (os.path.basename(f) + ".gz"))
                with open(f, "rb") as i, gzip.open(com_path, "wb") as o:
                    while True:
                        buf = i.read(self._chunk_size)
                        if buf == b"":
                            break
                        o.write(buf)
            elif self._format in ("bz2", "bzip2"):
                self._logger.info("Compress file %s to bzip2." % f)
                com_path = os.path.join(dir, (os.path.basename(f) + ".bz2"))
                with open(f, "rb") as i, bz2.open(com_path, "wb") as o:
                    while True:
                        buf = i.read(self._chunk_size)
                        if buf == b"":
                            break
                        o.write(buf)


class DateFormatConvert(FileBaseTransform):
    """
    Convert csv (tsv) date field columns to another date field format columns
    """

    def __init__(self):
        super().__init__()
        self._columns = []
        self._formatter = None

    def columns(self, columns):
        self._columns = columns

    def formatter(self, formatter):
        self._formatter = formatter

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._columns, self._formatter],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        _, ext = os.path.splitext(files[0])
        if ext == ".csv":
            delimiter = ","
        elif ext == ".tsv":
            delimiter = "\t"

        for ins, ous in super().io_writers(files, encoding=self._encoding):
            reader = csv.DictReader(ins, delimiter=delimiter)
            writer = csv.DictWriter(ous, reader.fieldnames)
            writer.writeheader()
            date_util = DateUtil()
            for row in reader:
                for column in self._columns:
                    r = row.get(column)
                    if not r:
                        continue
                    row[column] = date_util.convert_date_format(r, self._formatter)
                writer.writerow(row)


class ExcelConvert(FileBaseTransform):
    """
    Convert excel to other format
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        # TODO Currently only excel to csv is supported.
        super().io_files(files, ext="csv", func=self.convert)

    def convert(self, fi, fo):
        self._logger.info("Convert %s to %s" % (fi, fo))
        df = pandas.read_excel(fi)
        df.to_csv(fo, encoding=self._encoding)


class FileDivide(FileBaseTransform):
    """
    Divide a file to plural files
    """

    def __init__(self):
        super().__init__()
        self._divide_rows = None
        self._header = False

    def divide_rows(self, divide_rows):
        self._divide_rows = divide_rows

    def header(self, header):
        self._header = header

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._divide_rows],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for file in files:
            fname = os.path.basename(file)

            px = ""
            if fname.startswith("."):
                fname = fname[1:]
                px = "."

            if "." in fname:
                nameonly, ext = fname.split(".", 1)
                ext = "." + ext
            else:
                nameonly = fname
                ext = ""

            if self._header:
                with open(file, encoding=self._encoding) as i:
                    self._header_row = i.readline()

            row = self._ifile_reader(file)
            newfilename = px + nameonly + ".%s" + ext

            has_left = True
            index = 1
            while has_left:
                ofile_path = os.path.join(self._dest_dir, newfilename % str(index))
                has_left = self._ofile_generator(ofile_path, row)
                index = index + 1

    def _ifile_reader(self, filepath):
        with open(filepath, encoding=self._encoding) as i:
            if self._header is True:
                i.readline()
            for line in i:
                yield line

    def _ofile_generator(self, filepath, row):
        left = False
        written = False
        with open(filepath, mode="w", encoding=self._encoding) as o:
            if self._header is True:
                o.write(self._header_row)
            for i, line in enumerate(row):
                written = True
                o.write(line)
                if i + 1 >= self._divide_rows:
                    left = True
                    break
        if written is False:
            os.remove(filepath)
        return left


class FileRename(FileBaseTransform):
    """
    Change file names with adding either prefix or suffix.
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._suffix = ""
        self._regex_pattern = ""
        self._rep_str = ""
        self._ext = ""

    def prefix(self, prefix):
        self._prefix = prefix

    def suffix(self, suffix):
        self._suffix = suffix

    def regex_pattern(self, regex_pattern):
        self._regex_pattern = regex_pattern

    def rep_str(self, rep_str):
        self._rep_str = rep_str

    def ext(self, ext):
        self._ext = ext

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for file in files:
            dirname = os.path.dirname(file)
            basename = os.path.basename(file)

            px = ""
            if basename.startswith("."):
                basename = basename[1:]
                px = "."

            if "." in basename:
                nameonly, extension = basename.split(".", 1)
                extension = "." + extension
            else:
                nameonly = basename
                extension = ""

            if self._regex_pattern and self._rep_str:
                nameonly = re.sub(self._regex_pattern, self._rep_str, nameonly)
            elif self._regex_pattern:
                raise InvalidParameter("The converted string is not defined in yaml file: dest_str")
            elif self._rep_str:
                raise InvalidParameter(
                    "The conversion pattern is not defined in yaml file: regex_pattern"
                )

            if self._ext:
                extension = "." + self._ext

            newfilename = self._prefix + px + nameonly + self._suffix + extension
            newfilepath = os.path.join(dirname, newfilename)
            os.rename(file, newfilepath)
            self._logger.info("File name changed %s -> %s" % (file, newfilepath))


class FileConvert(FileBaseTransform):
    """
    Convert file encoding
    """

    def __init__(self):
        super().__init__()
        self._encoding_from = None
        self._encoding_to = None
        self._errors = None

    def encoding_from(self, encoding_from):
        self._encoding_from = encoding_from

    def encoding_to(self, encoding_to):
        self._encoding_to = encoding_to

    def errors(self, errors):
        self._errors = errors

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._encoding_from, self._encoding_to],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        File().convert_encoding(
            fi,
            fo,
            self._encoding_from,
            self._encoding_to,
            self._errors,
        )

        self._logger.info("Encoded file %s" % fi)


class FileArchive(FileBaseTransform):
    """
    Create archeve object.a
    """

    def __init__(self):
        super().__init__()
        self._format = None
        self._create_dir = False

    def format(self, format):
        self._format = format.lower()

    def create_dir(self, create_dir):
        self._create_dir = create_dir

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._format],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        dir = self._dest_dir if self._dest_dir is not None else self._src_dir

        valid = EssentialParameters(self.__class__.__name__, [self._dest_name])
        valid()
        dest_path = os.path.join(dir, (self._dest_name + ".%s" % self._format))

        if self._format == "tar":
            with tarfile.open(dest_path, "w") as tar:
                for file in files:
                    arcname = (
                        os.path.join(self._dest_name, os.path.basename(file))
                        if self._create_dir
                        else os.path.basename(file)
                    )
                    tar.add(file, arcname=arcname)
        elif self._format == "zip":
            with zipfile.ZipFile(dest_path, "w") as zp:
                for file in files:
                    arcname = (
                        os.path.join(self._dest_name, os.path.basename(file))
                        if self._create_dir
                        else os.path.basename(file)
                    )
                    zp.write(file, arcname=arcname)
        else:
            raise InvalidParameter("'format' must set one of the followings [tar, zip]")

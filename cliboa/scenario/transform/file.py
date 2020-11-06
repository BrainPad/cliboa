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
import codecs
import csv
import gzip
import os
import tarfile
import zipfile

import pandas

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.util.date import DateUtil
from cliboa.util.exception import CliboaException, InvalidCount, InvalidFormat
from cliboa.util.file import File
from cliboa.util.string import StringUtil


class FileBaseTransform(BaseStep):
    """
    Base class of file extract classes
    """

    def __init__(self):
        super().__init__()
        self._src_dir = ""
        self._src_pattern = ""
        self._dest_path = ""
        self._dest_dir = None
        self._dest_pattern = None
        self._encoding = "utf-8"

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def dest_dir(self, dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        self._dest_dir = dest_dir

    def dest_pattern(self, dest_pattern):
        self._dest_pattern = dest_pattern

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) != 1:
            raise Exception("Input file must be only one.")
        return files[0]


class FileDecompress(FileBaseTransform):
    """
    Decompress the specified file
    """

    def __init__(self):
        super().__init__()
        self._chunk_size = None

    def chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    def execute(self, *args):
        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Files found %s" % files)
        for f in files:
            _, ext = os.path.splitext(f)
            if ext == ".zip":
                self._logger.info("Decompress zip file %s" % f)
                with zipfile.ZipFile(f) as zp:
                    zp.extractall(
                        self._dest_dir if self._dest_dir is not None else self._src_dir
                    )
            elif ext == ".tar":
                self._logger.info("Decompress tar file %s" % f)
                with tarfile.open(f, "r:*") as tf:
                    tf.extractall(
                        self._dest_dir if self._dest_dir is not None else self._src_dir
                    )
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
        self._logger.info("Files found %s" % files)

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
        file = super().execute()
        valid = EssentialParameters(
            self.__class__.__name__, [self._columns, self._formatter]
        )
        valid()

        _, ext = os.path.splitext(file)
        if ext == ".csv":
            delimiter = ","
        elif ext == ".tsv":
            delimiter = "\t"

        with codecs.open(file, mode="r", encoding=self._encoding) as fi, codecs.open(
            self._dest_path, mode="w", encoding=self._encoding
        ) as fo:
            reader = csv.DictReader(fi, delimiter=delimiter)
            writer = csv.DictWriter(fo, reader.fieldnames)
            writer.writeheader()
            date_util = DateUtil()
            for row in reader:
                for column in self._columns:
                    r = row.get(column)
                    if not r:
                        continue
                    row[column] = date_util.convert_date_format(r, self._formatter)
                writer.writerow(row)
            fo.flush()
        self._logger.info("Finish %s" % self.__class__.__name__)


class ExcelConvert(FileBaseTransform):
    """
    Convert excel to other format
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._dest_dir, self._dest_pattern],
        )
        valid()

        # get a target file
        target_files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(target_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self._src_pattern)
            )
        elif len(target_files) > 1:
            self._logger.error("Hit target files %s" % target_files)
            raise InvalidCount("Input files must be only one.")
        self._logger.info(
            "A target file to be converted: %s" % os.path.join(target_files[0])
        )

        # convert
        _, dest_ext = os.path.splitext(self._dest_pattern)
        if dest_ext != ".csv":
            raise InvalidFormat(
                "%s is not supported format in %s. The supported format is .csv"
                % (dest_ext, self._dest_pattern)
            )

        df = pandas.read_excel(target_files[0], encoding=self._encoding)
        dest_path = os.path.join(self._dest_dir, self._dest_pattern)
        self._logger.info("Convert %s to %s" % (target_files[0], dest_path))
        df.to_csv(dest_path, encoding=self._encoding)


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
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._dest_dir,
                self._dest_pattern,
                self._divide_rows,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Files found %s" % files)

        file = files[0]
        if self._dest_pattern is None:
            fname = os.path.basename(file)
        else:
            fname = self._dest_pattern

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
        newfilename = nameonly + ".%s" + ext
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

    def prefix(self, prefix):
        self._prefix = prefix

    def suffix(self, suffix):
        self._suffix = suffix

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        for file in files:
            dirname = os.path.dirname(file)
            basename = os.path.basename(file)

            if "." in basename:
                nameonly, ext = basename.split(".", 1)
                ext = "." + ext
            else:
                nameonly = basename
                ext = ""

            newfilename = self._prefix + nameonly + self._suffix + ext
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

    def encoding_from(self, encoding_from):
        self._encoding_from = encoding_from

    def encoding_to(self, encoding_to):
        self._encoding_to = encoding_to

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._encoding_from, self._encoding_to],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        for file in files:
            basename = os.path.basename(file)

            if self._dest_dir:
                File().convert_encoding(
                    file,
                    os.path.join(self._dest_dir, basename),
                    self._encoding_from,
                    self._encoding_to,
                )
            else:
                tmpfile = os.path.join(
                    os.path.dirname(file),
                    "." + StringUtil().random_str(10) + "." + basename,
                )
                File().convert_encoding(
                    file, tmpfile, self._encoding_from, self._encoding_to
                )
                os.remove(file)
                os.rename(tmpfile, file)

            self._logger.info("Encoded file %s" % basename)

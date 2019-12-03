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
import bz2
import codecs
import csv
import gzip
import os
import pandas
import re
import shutil
import tarfile
import zipfile

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.util.date import DateUtil
from cliboa.util.exception import CliboaException, InvalidFormat, InvalidCount
from cliboa.util.file import File


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

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self._src_dir = src_dir

    @property
    def src_pattern(self):
        return self._src_pattern

    @src_pattern.setter
    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    @property
    def dest_path(self):
        return self._dest_path

    @dest_path.setter
    def dest_path(self, dest_path):
        self._dest_path = dest_path

    @property
    def dest_dir(self):
        return self._dest_dir

    @dest_dir.setter
    def dest_dir(self, dest_dir):
        if os.path.exists(dest_dir) is False:
            os.makedirs(dest_dir)
        self._dest_dir = dest_dir

    @property
    def dest_pattern(self):
        return self._dest_pattern

    @dest_pattern.setter
    def dest_pattern(self, dest_pattern):
        self._dest_pattern = dest_pattern

    @property
    def encoding(self):
        return self._encoding

    @encoding.setter
    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._dest_path]
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

        self._dest_dir = None

    @property
    def dest_dir(self):
        return self._dest_dir

    @dest_dir.setter
    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))

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
                with open(f, mode="rb") as fb:
                    rb = fb.read()
                decom_path = (
                    os.path.join(self._dest_dir, dcom_name)
                    if self._dest_dir is not None
                    else os.path.join(self._src_dir, dcom_name)
                )
                with open(decom_path, mode="wb") as fb:
                    fb.write(bz2.decompress(rb))
            elif ext == ".gz":
                self._logger.info("Decompress gz file %s" % f)
                dcom_name = os.path.splitext(os.path.basename(f))[0]
                decom_path = (
                    os.path.join(self._dest_dir, dcom_name)
                    if self._dest_dir is not None
                    else os.path.join(self._src_dir, dcom_name)
                )
                with gzip.open(f, "rb") as i, open(decom_path, "wb") as o:
                    o.write(i.read())
            else:
                raise CliboaException("Unmatched any available decompress type %s" % f)


class FileCompress(FileBaseTransform):
    """
    Compress files
    """
    def __init__(self):
        super().__init__()
        self._format = None

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, format):
        self._format = format.lower()

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._format,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Files found %s" % files)

        dir = self._dest_dir if self._dest_dir is not None else self._src_dir
        for f in files:
            if self._format == 'zip':
                self._logger.info("Compress file %s to zip." % f)
                with zipfile.ZipFile(os.path.join(dir, (os.path.basename(f) + '.zip')), 'w', zipfile.ZIP_DEFLATED) as o:
                    o.write(f, arcname=os.path.basename(f))
            elif self._format in ('gz', 'gzip'):
                with open(f, 'rb') as i:
                    self._logger.info("Compress file %s to gzip." % f)
                    with gzip.open(os.path.join(dir, (os.path.basename(f) + '.gz')), 'wb') as o:
                        shutil.copyfileobj(i, o)
            elif self._format in ('bz2', 'bzip2'):
                with open(f, 'rb') as i:
                    self._logger.info("Compress file %s to bzip2." % f)
                    with open(os.path.join(dir, (os.path.basename(f) + '.bz2')), 'wb') as o:
                        o.write(bz2.compress(i.read()))


class CsvColsExtract(FileBaseTransform):
    """
    Remove columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._columns = None

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    def execute(self, *args):
        file = super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._columns])
        valid()

        File().remove_csv_col(file, self._dest_path, self._columns)


class ColumnLengthAdjust(FileBaseTransform):
    """
    Adjust csv (tsv) column to maximum length
    """

    def __init__(self):
        super().__init__()
        self._adjust = {}

    @property
    def adjust(self):
        return self._adjust

    @adjust.setter
    def adjust(self, adjust):
        self._adjust = adjust

    def execute(self, *args):
        file = super().execute()
        if self._adjust is None:
            raise Exception(
                "The essential parameter are not specified in %s."
                % self.__class__.__name__
            )

        with codecs.open(file, mode="r", encoding=self._encoding) as fi, codecs.open(
            self._dest_path, mode="w", encoding=self._encoding
        ) as fo:
            reader = csv.DictReader(fi)
            writer = csv.DictWriter(fo, reader.fieldnames)
            writer.writeheader()

            for row in reader:
                for k, v in self._adjust.items():
                    f1 = row.get(k)
                    if len(f1) > v:
                        row[k] = f1[:v]
                writer.writerow(row)
            fo.flush()


class DateFormatConvert(FileBaseTransform):
    """
    Convert csv (tsv) date field columns to another date field format columns
    """

    def __init__(self):
        super().__init__()
        self._columns = []
        self._formatter = None

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    @property
    def formatter(self):
        return self._formatter

    @formatter.setter
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
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._dest_dir, self._dest_pattern],
        )
        valid()

        # get a target file
        target_files = File().get_target_files(self._src_dir, self._src_pattern)
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


class CsvMerge(FileBaseTransform):
    """
    Merge two csv files
    """

    def __init__(self):
        super().__init__()
        self.__src1_pattern = None
        self.__src2_pattern = None

    @property
    def src1_pattern(self):
        return self.__src1_pattern

    @src1_pattern.setter
    def src1_pattern(self, src1_pattern):
        self.__src1_pattern = src1_pattern

    @property
    def src2_pattern(self):
        return self.__src2_pattern

    @src2_pattern.setter
    def src2_pattern(self, src2_pattern):
        self.__src2_pattern = src2_pattern

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self.__src1_pattern,
                self.__src2_pattern,
                self._dest_dir,
                self._dest_pattern,
            ],
        )
        valid()

        target1_files = File().get_target_files(self._src_dir, self.__src1_pattern)
        target2_files = File().get_target_files(self._src_dir, self.__src2_pattern)
        if len(target1_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self.__src1_pattern)
            )
        elif len(target2_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self.__src2_pattern)
            )
        elif len(target1_files) > 1:
            self._logger.error("Hit target files %s" % target1_files)
            raise InvalidCount("Input files must be only one.")
        elif len(target2_files) > 1:
            self._logger.error("Hit target files %s" % target2_files)
            raise InvalidCount("Input files must be only one.")

        self._logger.info("Merge %s and %s." % (target1_files[0], target2_files[0]))
        df1 = pandas.read_csv(
            os.path.join(self._src_dir, target1_files[0]),
            dtype=str,
            encoding=self._encoding,
        )
        df2 = pandas.read_csv(
            os.path.join(self._src_dir, target2_files[0]),
            dtype=str,
            encoding=self._encoding,
        )
        df = pandas.merge(df1, df2)
        if "Unnamed: 0" in df.index:
            del df["Unnamed: 0"]
        df.to_csv(
            os.path.join(self._dest_dir, self._dest_pattern),
            encoding=self._encoding,
            index=False,
        )


class CsvHeaderConvert(FileBaseTransform):
    """
    Conver csv headers
    """

    def __init__(self):
        super().__init__()
        self.__headers = []

    @property
    def headers(self):
        return self.__headers

    @headers.setter
    def headers(self, headers):
        self.__headers = headers

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._dest_dir,
                self._dest_pattern,
                self.__headers,
            ],
        )
        valid()

        target_files = File().get_target_files(self._src_dir, self._src_pattern)
        if len(target_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self._src_pattern)
            )
        elif len(target_files) > 1:
            self._logger.error("Hit target files %s" % target_files)
            raise InvalidCount("Input files must be only one.")
        self._logger.info("A target file to be converted: %s")

        dest_path = os.path.join(self._dest_dir, self._dest_pattern)
        self._logger.info(
            "Convert header of %s. An output file is %s." % (target_files[0], dest_path)
        )
        with open(target_files[0], "r", encoding=self._encoding) as s, open(
            dest_path, "w", encoding=self._encoding
        ) as d:
            reader = csv.reader(s)
            writer = csv.writer(d, quoting=csv.QUOTE_ALL)
            headers = next(reader, None)
            new_headers = self.__replace_headers(headers)
            writer.writerow(new_headers)
            for r in reader:
                writer.writerow(r)
            d.flush()

    def __replace_headers(self, old_headers):
        """
        Replace old headers to new headers
        """
        new_headers = []
        for old_and_new_headers in self.__headers:
            for oh in old_headers:
                if old_and_new_headers.get(oh):
                    new_headers.append(old_and_new_headers[oh])
                    break
        return new_headers


class FileDivide(FileBaseTransform):
    """
    Divide a file to plural files
    """
    def __init__(self):
        super().__init__()
        self._divide_rows = None
        self._header = False

    @property
    def divide_rows(self):
        return self._divide_rows

    @divide_rows.setter
    def divide_rows(self, divide_rows):
        self._divide_rows = divide_rows

    @property
    def header(self):
        return self._header

    @header.setter
    def header(self, header):
        self._header = header

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))

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
        newfilename = nameonly + '.%s' + ext
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

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, prefix):
        self._prefix = prefix

    @property
    def suffix(self):
        return self._suffix

    @suffix.setter
    def suffix(self, suffix):
        self._suffix = suffix

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        files = File().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info('No files are found. Nothing to do.')
            return

        for file in files:
            dirname = os.path.dirname(file)
            basename = os.path.basename(file)

            if '.' in basename:
                nameonly, ext = basename.split(".", 1)
                ext = "." + ext
            else:
                nameonly = basename
                ext = ""

            newfilename = self._prefix + nameonly + self._suffix + ext
            newfilepath = os.path.join(dirname, newfilename)
            os.rename(file, newfilepath)
            self._logger.info("File name changed %s -> %s" % (file, newfilepath))

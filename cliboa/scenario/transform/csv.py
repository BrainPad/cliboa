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
import hashlib
import os
import re
from datetime import datetime

import jsonlines
import pandas

from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.core.validator import EssentialParameters
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.csv import Csv
from cliboa.util.exception import FileNotFound, InvalidCount, InvalidParameter
from cliboa.util.file import File
from cliboa.util.string import StringUtil


class CsvColumnHash(FileBaseTransform):
    """
    Hash(SHA256) specific columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._columns = []

    def columns(self, columns):
        self._columns = columns

    def _stringToHash(self, string):
        return hashlib.sha256(string.encode()).hexdigest()

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._columns],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        df = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
        )
        for c in self._columns:
            df[c] = df[c].apply(self._stringToHash)

        df.to_csv(
            fo,
            encoding=self._encoding,
            index=False,
        )


class CsvColumnExtract(FileBaseTransform):
    """
    Remove specific columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._columns = None
        self._column_numbers = None

    def columns(self, columns):
        self._columns = columns

    def column_numbers(self, column_numbers):
        self._column_numbers = column_numbers

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        if not self._columns and not self._column_numbers:
            raise InvalidParameter("Specifying either 'column' or 'column_numbers' is essential.")
        if self._columns and self._column_numbers:
            raise InvalidParameter("Cannot specify both 'column' and 'column_numbers'.")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self._columns:
            Csv.extract_columns_with_names(fi, fo, self._columns)
        elif self._column_numbers:
            if isinstance(self._column_numbers, int) is True:
                remain_column_numbers = [self._column_numbers]
            else:
                column_numbers = self._column_numbers.split(",")
                remain_column_numbers = [int(n) for n in column_numbers]
            Csv.extract_columns_with_numbers(fi, fo, remain_column_numbers)


class CsvValueExtract(FileBaseTransform):
    """
    Extract a specific column from a CSV file and then replace it with a regular expression.
    """

    def __init__(self):
        super().__init__()
        self._column_regex_pattern = None

    def column_regex_pattern(self, column_regex_pattern):
        self._column_regex_pattern = column_regex_pattern

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._column_regex_pattern]
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)
        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        with open(fi, mode="rt") as i:
            reader = csv.DictReader(i)
            with open(fo, mode="wt") as o:
                writer = csv.DictWriter(o, reader.fieldnames)
                writer.writeheader()
                for line in reader:
                    for column, regex_pattern in self._column_regex_pattern.items():
                        if re.search(regex_pattern, line[column]):
                            line[column] = re.search(regex_pattern, line[column]).group()
                        else:
                            line[column] = ""
                    writer.writerow(dict(line))


class CsvColumnConcat(FileBaseTransform):
    """
    Concat specific columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._columns = None
        self._dest_column_name = None
        self._sep = ""

    def columns(self, columns):
        self._columns = columns

    def dest_column_name(self, dest_column_name):
        self._dest_column_name = dest_column_name

    def sep(self, sep):
        self._sep = sep

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._dest_column_name],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        if len(self._columns) < 2 or type(self._columns) is not list:
            raise InvalidParameter("'columns' must 2 or more lengths")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        df = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
        )

        dest_str = None
        for c in self._columns:
            if dest_str is None:
                dest_str = df[c].astype(str)
            else:
                dest_str = dest_str + self._sep + df[c].astype(str)
            df = df.drop(columns=[c])
        df[self._dest_column_name] = dest_str

        df.to_csv(
            fo,
            encoding=self._encoding,
            index=False,
        )


class CsvMergeExclusive(FileBaseTransform):
    """
    Compare specific columns each file.
    If matched, exclude rows.
    """

    def __init__(self):
        super().__init__()
        self._src_column = None
        self._target_compare_path = None
        self._target_column = None

    def src_column(self, src_column):
        self._src_column = src_column

    def target_compare_path(self, target_compare_path):
        self._target_compare_path = target_compare_path

    def target_column(self, target_column):
        self._target_column = target_column

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._src_column,
                self._target_compare_path,
                self._target_column,
            ],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        target = super().get_target_files(
            os.path.dirname(self._target_compare_path), os.path.basename(self._target_compare_path)
        )
        self.check_file_existence(target)

        self.df_target = pandas.read_csv(self._target_compare_path)
        if self._target_column not in self.df_target:
            raise KeyError(
                "Target Compare file does not exist target column [%s]." % self._target_column
            )

        self.df_target_list = self.df_target[self._target_column].values.tolist()

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        df = pandas.read_csv(fi)
        try:
            df[self._src_column].values.tolist()
        except KeyError:
            raise KeyError("Src file does not exist target column [%s]." % self._target_column)

        df = df[~df[self._src_column].isin(self.df_target_list)]

        df.to_csv(
            fo,
            encoding=self._encoding,
            index=False,
        )


class ColumnLengthAdjust(FileBaseTransform):
    """
    Adjust csv (tsv) column to maximum length
    """

    def __init__(self):
        super().__init__()
        self._adjust = {}

    def adjust(self, adjust):
        self._adjust = adjust

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._adjust],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_writers(files, encoding=self._encoding)

        for fi, fo in super().io_writers(files, encoding=self._encoding):
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


class CsvMerge(FileBaseTransform):
    """
    Merge two csv files
    """

    def __init__(self):
        super().__init__()
        self._src1_pattern = None
        self._src2_pattern = None

    def src1_pattern(self, src1_pattern):
        self._src1_pattern = src1_pattern

    def src2_pattern(self, src2_pattern):
        self._src2_pattern = src2_pattern

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src1_pattern,
                self._src2_pattern,
                self._dest_dir,
                self._dest_name,
            ],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        target1_files = File().get_target_files(self._src_dir, self._src1_pattern)
        target2_files = File().get_target_files(self._src_dir, self._src2_pattern)
        if len(target1_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist." % os.path.join(self._src_dir, self._src1_pattern)
            )
        elif len(target2_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist." % os.path.join(self._src_dir, self._src2_pattern)
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

        dest_name = self._dest_name

        df.to_csv(
            os.path.join(self._dest_dir, dest_name),
            encoding=self._encoding,
            index=False,
        )


class CsvColumnSelect(FileBaseTransform):
    """
    Select columns in Csv file in specified order
    """

    def __init__(self):
        super().__init__()
        self._column_order = None

    def column_order(self, column_order):
        self._column_order = column_order

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._column_order]
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = File().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("No files are found.")
        self._logger.info("Files found %s" % files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        df = pandas.read_csv(fi, dtype=str, encoding=self._encoding)
        if set(self._column_order) - set(df.columns.values):
            raise InvalidParameter(
                "column_order define not included target file's column : %s"
                % (set(self._column_order) - set(df.columns.values))
            )
        df = df.loc[:, self._column_order]
        df.to_csv(
            fo,
            encoding=self._encoding,
            index=False,
        )


class CsvConcat(FileBaseTransform):
    """
    Concat csv files
    """

    def __init__(self):
        super().__init__()
        self._src_filenames = None

    def src_filenames(self, src_filenames):
        self._src_filenames = src_filenames

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._dest_dir, self._dest_name]
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        if not self._src_pattern and not self._src_filenames:
            raise InvalidParameter(
                "Specifying either 'src_pattern' or 'src_filenames' is essential."
            )
        if self._src_pattern and self._src_filenames:
            raise InvalidParameter("Cannot specify both 'src_pattern' and 'src_filenames'.")

        if self._src_pattern:
            files = File().get_target_files(self._src_dir, self._src_pattern)
        else:
            files = []
            for file in self._src_filenames:
                files.append(os.path.join(self._src_dir, file))

        if len(files) == 0:
            raise FileNotFound("No files are found.")
        elif len(files) == 1:
            self._logger.warning("Two or more input files are required.")

        file = files.pop(0)
        df1 = pandas.read_csv(
            file,
            dtype=str,
            encoding=self._encoding,
        )

        for file in files:
            df2 = pandas.read_csv(
                file,
                dtype=str,
                encoding=self._encoding,
            )
            df1 = pandas.concat([df1, df2])

        dest_name = self._dest_name

        df1.to_csv(
            os.path.join(self._dest_dir, dest_name),
            encoding=self._encoding,
            index=False,
        )


class CsvConvert(FileBaseTransform):
    """
    Change csv format
    """

    def __init__(self):
        super().__init__()
        self._headers = []
        self._headers_existence = True
        self._before_format = "csv"
        self._before_enc = self._encoding
        self._after_format = None
        self._after_enc = None
        self._after_nl = "LF"
        self._reader_quote = "QUOTE_NONE"
        self._quote = "QUOTE_MINIMAL"

    def headers(self, headers):
        self._headers = headers

    def headers_existence(self, headers_existence):
        self._headers_existence = headers_existence

    def before_format(self, before_format):
        self._before_format = before_format

    def before_enc(self, before_enc):
        self._before_enc = before_enc

    def after_format(self, after_format):
        self._after_format = after_format

    def after_enc(self, after_enc):
        self._after_enc = after_enc

    def after_nl(self, after_nl):
        self._after_nl = after_nl

    def reader_quote(self, reader_quote):
        self._reader_quote = reader_quote

    def quote(self, quote):
        self._quote = quote

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._before_format, self._before_enc],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        if self._after_format is None:
            self._after_format = self._before_format

        if self._after_enc is None:
            self._after_enc = self._before_enc

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, ext=self._after_format, func=self.convert)

    def convert(self, fi, fo):
        with open(fi, mode="rt", encoding=self._before_enc) as i:
            reader = csv.reader(
                i,
                delimiter=Csv.delimiter_convert(self._before_format),
                quoting=Csv.quote_convert(self._reader_quote),
            )
            with open(fo, mode="wt", newline="", encoding=self._after_enc) as o:
                writer = csv.writer(
                    o,
                    delimiter=Csv.delimiter_convert(self._after_format),
                    quoting=Csv.quote_convert(self._quote),
                    lineterminator=Csv.newline_convert(self._after_nl),
                )

                for i, line in enumerate(reader):
                    if i == 0:
                        if self._headers_existence is False:
                            continue
                        writer.writerow(self._replace_headers(line))
                    else:
                        writer.writerow(line)

    def _replace_headers(self, old_headers):
        """
        Replace old headers to new headers
        """
        if not self._headers:
            return old_headers

        converter = {}
        for headers in self._headers:
            for k, v in headers.items():
                converter[k] = v

        new_headers = []
        for oh in old_headers:
            r = converter.get(oh)
            new_headers.append(r if r is not None else oh)

        return new_headers


class CsvSort(FileBaseTransform):
    """
    Sort csv.
    """

    def __init__(self):
        super().__init__()
        self._order = []
        self._quote = "QUOTE_MINIMAL"
        self._no_duplicate = False

    def order(self, order):
        self._order = order

    def quote(self, quote):
        self._quote = quote

    def no_duplicate(self, no_duplicate):
        self._no_duplicate = no_duplicate

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._order, self._src_dir, self._src_pattern, self._dest_dir],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        ymd_hms = datetime.now().strftime("%Y%m%d%H%M%S%f")
        dbname = ".%s_%s.db" % (ymd_hms, StringUtil().random_str(8))
        tblname = "temp_table"

        sqlite = SqliteAdapter()
        sqlite.connect(dbname)
        try:
            for file in files:
                _, filename = os.path.split(file)
                dest_file = os.path.join(self._dest_dir, filename)

                sqlite.import_table(file, tblname, encoding=self._encoding)
                sqlite.export_table(
                    tblname,
                    dest_file,
                    quoting=Csv.quote_convert(self._quote),
                    encoding=self._encoding,
                    order=self._order,
                    no_duplicate=self._no_duplicate,
                )
            sqlite.close()
        finally:
            os.remove(dbname)


class CsvToJsonl(FileBaseTransform):
    """
    Transform csv to jsonlines.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, ext="jsonl", func=self.convert)

    def convert(self, fi, fo):
        with open(fi, mode="r", encoding=self._encoding, newline="") as i, jsonlines.open(
            fo, mode="w"
        ) as writer:
            reader = csv.DictReader(i)
            for row in reader:
                writer.write(row)

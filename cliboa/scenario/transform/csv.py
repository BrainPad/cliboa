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
import glob
import hashlib
import os
import re
import shutil
from datetime import datetime
from enum import Enum
from typing import Any, List, Set, Tuple

import dask.dataframe as dask_df
import jsonlines
import pandas

from cliboa.adapter.csv import Csv
from cliboa.adapter.file import File
from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.exception import FileNotFound, InvalidCount, InvalidParameter
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

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            for c in self._columns:
                df[c] = df[c].apply(self._stringToHash)
            df.to_csv(
                fo,
                encoding=self._encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


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


class CsvColumnDelete(FileBaseTransform):
    """
    Delete specific columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._regex_pattern = None

    def regex_pattern(self, regex_pattern):
        self._regex_pattern = regex_pattern

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        if not self._regex_pattern:
            raise InvalidParameter("'regex_pattern' is essential.")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        pattern = re.compile(self._regex_pattern)
        for df in tfr:
            for column in df.columns.values:
                if pattern.fullmatch(column):
                    df = df.drop(column, axis=1)
                # output an empty file when all columns are deleted
                if df.empty:
                    df = df.dropna(how="all")
            df.to_csv(
                fo,
                encoding=self._encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


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
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._column_regex_pattern],
        )
        valid()

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

        if len(self._columns) < 2 or type(self._columns) is not list:
            raise InvalidParameter("'columns' must 2 or more lengths")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        dest_str = None
        for df in tfr:
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
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvTypeConvert(FileBaseTransform):
    """
    Convert the type of specific column in a csv file.
    """

    def __init__(self):
        super().__init__()
        self._column = None
        self._type = None

    def column(self, column):
        self._column = column

    def type(self, type):
        self._type = type

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._dest_dir, self._column, self._type],
        )
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)
        self._logger.info("Files found %s" % files)
        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=object,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            for column in self._column:
                try:
                    if self._type == "int":
                        # When reading from csv, the following error occurs:
                        # ValueError: invalid literal for int() with base 10
                        # To avoid this, convert to float and then convert to int
                        df[column] = df[column].astype("float")
                        df[column] = df[column].astype("int")
                    else:
                        df[column] = df[column].astype(self._type)
                except Exception:
                    raise InvalidParameter(
                        "Conversion to this type is not possible. %s" % self._type
                    )
            df.to_csv(
                fo,
                encoding=self._encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvMergeExclusive(FileBaseTransform):
    """
    Compare specific columns each file.
    If matched, exclude rows.
    """

    def __init__(self):
        super().__init__()
        self.df_target_list = None
        self._src_column = None
        self._target_compare_path = None
        self._target_column = None
        self._all_column = False

    def src_column(self, src_column):
        self._src_column = src_column

    def target_compare_path(self, target_compare_path):
        self._target_compare_path = target_compare_path

    def target_column(self, target_column):
        self._target_column = target_column

    def all_column(self, all_column):
        self._all_column = all_column

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._target_compare_path,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        target = super().get_target_files(
            os.path.dirname(self._target_compare_path),
            os.path.basename(self._target_compare_path),
        )
        self.check_file_existence(target)

        if self._all_column and (self._src_column or self._target_column):
            raise KeyError("all_column cannot coexist with src_column or target_column.")

        header = pandas.read_csv(self._target_compare_path, nrows=0)
        if self._all_column is False and self._target_column not in header:
            raise KeyError(
                "Target Compare file does not exist target column [%s]." % self._target_column
            )

        if self._all_column:
            df_target = pandas.read_csv(self._target_compare_path, dtype=str)
            self.df_target_list = df_target.values.tolist()
        else:
            df_target = pandas.read_csv(
                self._target_compare_path, usecols=[self._target_column], dtype=str
            )
            self.df_target_list = df_target[self._target_column].values.tolist()

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self._all_column is False:
            header = pandas.read_csv(fi, dtype=str, encoding=self._encoding, nrows=0)
            try:
                header[self._src_column].values.tolist()
            except KeyError:
                raise KeyError("Src file does not exist target column [%s]." % self._target_column)

        self._csv_write(fi, fo)

    def _csv_write(self, fi, fo):
        # Used in chunk_size_handling
        df = pandas.read_csv(fi, dtype=str, na_filter=False)
        if self._all_column:
            df_target_set = {hash(tuple(row)) for row in self.df_target_list}
            df = df.drop(self._all_elements_match(df.values.tolist(), df_target_set))
        else:
            df = df[~df[self._src_column].isin(self.df_target_list)]
        df.to_csv(
            fo,
            encoding=self._encoding,
            header=True,
            index=False,
            mode="a",
        )

    def _all_elements_match(self, df_src_list, df_target_set):
        return [i for i, row in enumerate(df_src_list) if hash(tuple(row)) in df_target_set]


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
        self._target_path = None
        self._join_on = None
        self._engine = None
        # not parameter
        self._target_df = None

    def target_path(self, value):
        self._target_path = value

    def join_on(self, value):
        self._join_on = value

    def engine(self, value):
        self._engine = value

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._target_path,
                self._dest_dir,
                self._join_on,
            ],
        )
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        source_files = self.get_target_files(self._src_dir, self._src_pattern)
        if len(source_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist." % os.path.join(self._src_dir, self._src2_pattern)
            )

        target_files = glob.glob(self._target_path)
        if len(target_files) == 0:
            raise InvalidCount("An target file %s does not exist." % self.target_path)
        elif len(target_files) > 1:
            self._logger.error("Hit target files %s" % target_files)
            raise InvalidCount("Target files must be only one.")

        if self._engine == "dask":
            engine = "dask"
        else:
            engine = "pandas"
        self._logger.info("Merge(%s) %s to %s" % (engine, target_files[0], source_files))
        if engine == "dask":
            self._dask_merge(source_files, target_files[0])
        else:
            self._pandas_merge(source_files, target_files[0])

    def _dask_merge(self, source_files: List[str], target_file: str) -> None:
        """
        merge using dask
        """
        dd_target = dask_df.read_csv(target_file, encoding=self._encoding)

        for source_file in source_files:
            dest_path = os.path.join(self._dest_dir, os.path.basename(source_file))
            dd_source = dask_df.read_csv(source_file, encoding=self._encoding)
            merged_dd = dask_df.merge(dd_source, dd_target, on=self._join_on)
            merged_dd.to_csv(dest_path, single_file=True, index=False, encoding=self._encoding)

    def _pandas_merge(self, source_files: List[str], target_file: str) -> None:
        """
        merge using pandas
        NOTE: if target file is too large, use dask engine.
        """
        self._target_df = pandas.read_csv(target_file)
        for source_file in source_files:
            chunk_size_handling(self._pandas_merge_one, source_file)

    def _pandas_merge_one(self, chunksize: int, source_file: str) -> None:
        dest_path = os.path.join(self._dest_dir, os.path.basename(source_file))

        chunks = pandas.read_csv(source_file, chunksize=chunksize, encoding=self._encoding)
        is_first_chunk = True
        for chunk in chunks:
            merged_chunk = pandas.merge(chunk, self._target_df, on=self._join_on)

            if is_first_chunk:
                merged_chunk.to_csv(
                    dest_path, index=False, mode="w", header=True, encoding=self._encoding
                )
                is_first_chunk = False
            else:
                merged_chunk.to_csv(
                    dest_path, index=False, mode="a", header=False, encoding=self._encoding
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
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._column_order],
        )
        valid()

        files = File().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("No files are found.")
        self._logger.info("Files found %s" % files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            if set(self._column_order) - set(df.columns.values):
                raise InvalidParameter(
                    "column_order define not included target file's column : %s"
                    % (set(self._column_order) - set(df.columns.values))
                )
            df = df.loc[:, self._column_order]
            df.to_csv(
                fo,
                encoding=self._encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


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

        # Create output headers to conform to the concat specification.
        file_1 = files[0]
        output_header = pandas.read_csv(
            file_1, dtype=str, encoding=self._encoding, nrows=0, na_filter=False
        )
        for file in files[1:]:
            output_header = pandas.concat(
                [
                    output_header,
                    pandas.read_csv(
                        file, dtype=str, encoding=self._encoding, nrows=0, na_filter=False
                    ),
                ]
            )

        chunk_size_handling(self._read_csv_func, files, output_header)

    def _read_csv_func(self, chunksize, files, output_header):
        # Used in chunk_size_handling
        first_write = True
        for file in files:
            tfr = pandas.read_csv(
                file,
                dtype=str,
                encoding=self._encoding,
                chunksize=chunksize,
                na_filter=False,
            )
            for df in tfr:
                # Change the header order to the one you plan to output.
                df = pandas.concat([output_header, df])
                df.to_csv(
                    os.path.join(self._dest_dir, self._dest_name),
                    encoding=self._encoding,
                    header=True if first_write else False,
                    index=False,
                    mode="w" if first_write else "a",
                )
                first_write = False


class CsvConvert(FileBaseTransform):
    """
    Change csv format
    """

    def __init__(self):
        super().__init__()
        self._headers = []
        self._headers_existence = True
        self._add_headers = None
        self._before_format = "csv"
        self._before_enc = self._encoding
        self._before_escapechar = None
        self._after_format = None
        self._after_enc = None
        self._after_nl = "LF"
        self._after_escapechar = None
        self._reader_quote = "QUOTE_MINIMAL"
        self._quote = "QUOTE_MINIMAL"

    def headers(self, headers):
        self._headers = headers

    def headers_existence(self, headers_existence):
        self._headers_existence = headers_existence

    def add_headers(self, add_headers):
        self._add_headers = add_headers

    def before_format(self, before_format):
        self._before_format = before_format

    def before_enc(self, before_enc):
        self._before_enc = before_enc

    def before_escapechar(self, before_escapechar):
        self._before_escapechar = before_escapechar

    def after_format(self, after_format):
        self._after_format = after_format

    def after_enc(self, after_enc):
        self._after_enc = after_enc

    def after_nl(self, after_nl):
        self._after_nl = after_nl

    def after_escapechar(self, after_escapechar):
        self._after_escapechar = after_escapechar

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
                escapechar=self._before_escapechar,
                doublequote=False if self._before_escapechar else True,
            )
            with open(fo, mode="wt", newline="", encoding=self._after_enc) as o:
                writer = csv.writer(
                    o,
                    delimiter=Csv.delimiter_convert(self._after_format),
                    quoting=Csv.quote_convert(self._quote),
                    lineterminator=Csv.newline_convert(self._after_nl),
                    escapechar=self._after_escapechar,
                    doublequote=False if self._after_escapechar else True,
                )

                for i, line in enumerate(reader):
                    if i == 0:
                        if self._headers_existence is False:
                            continue
                        if self._add_headers:
                            writer.writerow(self._add_headers)
                            writer.writerow(line)
                        else:
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

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, ext="jsonl", func=self.convert)

    def convert(self, fi, fo):
        with (
            open(fi, mode="r", encoding=self._encoding, newline="") as i,
            jsonlines.open(fo, mode="w") as writer,
        ):
            reader = csv.DictReader(i)
            for row in reader:
                writer.write(row)


class CsvColumnCopy(FileBaseTransform):
    """
    Copy column data (new or overwrite)
    """

    def __init__(self):
        super().__init__()
        self._src_column = None
        self._dest_column = None

    def src_column(self, src_column):
        self._src_column = src_column

    def dest_column(self, dest_column):
        self._dest_column = dest_column

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._dest_dir, self._src_column, self._dest_column],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        header = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            nrows=0,
            na_filter=False,
        )
        if self._src_column not in header:
            raise KeyError("Copy source column does not exist in file. [%s]" % self._src_column)

        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )

        for df in tfr:
            df[self._dest_column] = df[self._src_column]
            df.to_csv(
                fo,
                encoding=self._encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvColumnReplace(FileBaseTransform):
    """
    Replace matching regular expression values for a specific column from a csv file.
    """

    def __init__(self):
        super().__init__()
        self._column = None
        self._regex_pattern = None
        self._rep_str = None
        self._regex_compile = None

    def column(self, column):
        self._column = column

    def regex_pattern(self, regex_pattern):
        self._regex_pattern = regex_pattern

    def rep_str(self, rep_str):
        self._rep_str = rep_str

    def _replace_string(self, string):
        return self._regex_compile.sub(self._rep_str, string)

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern, self._column],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        if self._rep_str is None:
            raise InvalidParameter("The converted string is not defined in yaml file: rep_str")
        if self._regex_pattern is None:
            raise InvalidParameter(
                "The conversion pattern is not defined in yaml file: regex_pattern"
            )
        self._regex_compile = re.compile(self._regex_pattern)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        header = pandas.read_csv(fi, dtype=str, encoding=self._encoding, nrows=0, na_filter=False)
        if self._column not in header:
            raise KeyError("Replace source column does not exist in file. [%s]" % self._column)

        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunksize,
            na_filter=False,
        )

        for df in tfr:
            df[self._column] = df[self._column].apply(self._replace_string)
            df.to_csv(
                fo,
                header=True if first_write else False,
                encoding=self._encoding,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvDuplicateRowDelete(FileBaseTransform):
    def __init__(self):
        super().__init__()
        self._delimiter = ","
        self._engine = "pandas"  # 'pandas' or 'dask' for large files

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def engine(self, engine):
        """
        Set processing engine.

        Args:
            engine (str): 'pandas' for memory-efficient processing with
                         preserved order, 'dask' for very large files
                         (may not preserve original order)
        """
        if engine not in ["pandas", "dask"]:
            raise InvalidParameter("Engine must be 'pandas' or 'dask'")
        self._engine = engine

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._src_dir, self._src_pattern],
        )
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)

        self.check_file_existence(files)
        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self._engine == "dask":
            self._convert_with_dask(fi, fo)
        else:
            self._convert_with_pandas(fi, fo)

    def _convert_with_pandas(self, fi, fo):
        """
        Memory-efficient duplicate removal using pandas with set for tracking
        seen rows.

        Preserves original row order.
        """
        # Initialize shared state for chunk processing
        self._seen_rows = set()
        self._first_write = True

        # Use existing chunk_size_handling infrastructure
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        """
        Process CSV in chunks with duplicate removal.
        Used by chunk_size_handling for memory-efficient processing.
        """
        for chunk in pandas.read_csv(
            fi,
            delimiter=self._delimiter,
            dtype=str,
            na_filter=False,
            chunksize=chunksize,
            header=None,
            encoding=self._encoding,
        ):
            # Convert DataFrame to list of tuples for hashability
            unique_rows = []
            for _, row in chunk.iterrows():
                row_tuple = tuple(row.values)
                if row_tuple not in self._seen_rows:
                    self._seen_rows.add(row_tuple)
                    unique_rows.append(row.values)

            # Write unique rows to output file
            if unique_rows:
                unique_df = pandas.DataFrame(unique_rows)
                unique_df.to_csv(
                    fo,
                    mode="w" if self._first_write else "a",
                    header=False,
                    index=False,
                    sep=self._delimiter,
                    encoding=self._encoding,
                )
                self._first_write = False

    def _convert_with_dask(self, fi, fo):
        """
        Handle very large files using dask.

        May not preserve original row order but provides better memory
        efficiency for extremely large datasets.
        """
        self._logger.info("Using dask engine - original row order may not be preserved")

        # Read CSV with dask
        df = dask_df.read_csv(
            fi,
            delimiter=self._delimiter,
            dtype=str,
            na_filter=False,
            encoding=self._encoding,
            header=None,
        )

        # Remove duplicates using dask
        df_unique = df.drop_duplicates()

        # Write to output file
        df_unique.to_csv(
            fo,
            single_file=True,
            header=False,
            index=False,
            sep=self._delimiter,
            encoding=self._encoding,
        )


class CsvRowDelete(FileBaseTransform):
    def __init__(self):
        super().__init__()
        self._alter_path = None
        self._src_key_column = None
        self._alter_key_column = None
        self._delimiter = ","
        self._has_match = True

    def alter_path(self, alter_path):
        self._alter_path = alter_path

    def src_key_column(self, src_key_column):
        self._src_key_column = src_key_column

    def alter_key_column(self, alter_key_column):
        self._alter_key_column = alter_key_column

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def has_match(self, has_match):
        self._has_match = has_match

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._alter_path,
                self._src_key_column,
                self._alter_key_column,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)

        self._logger.info("files is %s", files)
        self.check_file_existence(files)
        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        s = set()
        with open(self._alter_path, "r") as al:
            alt_reader = csv.DictReader(al, delimiter=self._delimiter)
            for alt_row in alt_reader:
                s.add(alt_row[self._alter_key_column])
        with open(fi, "r") as i:
            reader = csv.DictReader(i, delimiter=self._delimiter)
            with open(fo, "w", newline="") as o:
                writer = csv.DictWriter(o, fieldnames=reader.fieldnames, delimiter=self._delimiter)
                writer.writeheader()
                for row in reader:
                    if self._has_match is True and row[self._src_key_column] not in s:
                        writer.writerow(row)
                    elif self._has_match is False and row[self._src_key_column] in s:
                        writer.writerow(row)


class _CsvSplitMethodEnum(Enum):
    ROWS = "rows"
    GROUPED = "grouped"


class CsvSplit(FileBaseTransform):
    """
    Split csv files, using the value of a specific column as the output filename.

    - The output filename is the value from the specified column, with a .csv extension appended.
    - If the value of the specified column is empty, that row should be ignored.
    - When multiple source files exist, the column definitions must all be the same.
    """

    def __init__(self):
        super().__init__()
        self._method = None
        self._key_column = None
        self._rows = None
        self._suffix_format = ".{:02d}"

    def method(self, value: str) -> None:
        self._method = _CsvSplitMethodEnum(value)

    def key_column(self, value) -> None:
        self._key_column = value

    def rows(self, value) -> None:
        self._rows = value

    def suffix_format(self, value) -> None:
        self._suffix_format = value

    @property
    def output_dir(self) -> str:
        if self._dest_dir:
            return self._dest_dir
        else:
            return self._src_dir

    def execute(self, *args) -> None:
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._method,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)
        os.makedirs(self.output_dir, exist_ok=True)

        properties = vars(self)
        properties["output_dir"] = self.output_dir
        if self._method.value == "rows":
            executeInstance = _CsvSplitMethodRows(properties)
        elif self._method.value == "grouped":
            executeInstance = _CsvSplitMethodGrouped(properties)
        else:
            raise NotImplementedError(
                f"Defined {self._method.value} in _CsvSplitMethodEnum,"
                " but not implemented logic in execute."
            )
        executeInstance.execute(files)


class _CsvSplitMethodBase:
    def __init__(self, properties):
        self._caller_properties = properties

    def __getattr__(self, name) -> Any:
        try:
            return self._caller_properties[name]
        except KeyError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def execute(self, files: List[str]) -> None:
        raise NotImplementedError("Please implement execute method.")


class _CsvSplitMethodRows(_CsvSplitMethodBase):
    def execute(self, files: List[str]) -> None:
        valid2 = EssentialParameters(
            self.__class__.__name__,
            [
                self._rows,
            ],
        )
        valid2()
        for filepath in files:
            self._split_one(filepath)

    def _split_one(self, filepath: str) -> None:
        self._logger.info("Split {:s} per {:d} rows".format(filepath, self._rows))
        file_name, ext = os.path.splitext(os.path.basename(filepath))
        with open(filepath, "r", encoding=self._encoding, newline="") as f_in:
            reader = csv.reader(f_in)
            try:
                header = next(reader)
            except StopIteration:
                self._logger.error(f"Empty {filepath}")
                return

            file_index = 0
            rows_count = 0
            writer = None
            f_out = None
            output_filepath = None

            for line in reader:
                # new file per self._rows
                if rows_count % self._rows == 0:
                    # close file-pointer if exists
                    if f_out:
                        f_out.close()
                        self._logger.info(
                            f"Generated {output_filepath} with {self._rows} rows"
                            f" by read up to line {rows_count} of the original."
                        )
                    # open new file-pointer
                    suffix = self._suffix_format.format(file_index)
                    output_filepath = os.path.join(self.output_dir, f"{file_name}{suffix}{ext}")
                    f_out = open(output_filepath, "w", encoding=self._encoding, newline="")
                    writer = csv.writer(f_out)
                    writer.writerow(header)
                    file_index += 1
                # write current line
                writer.writerow(line)
                rows_count += 1
            # close last file-pointer if exists
            if f_out:
                f_out.close()
                last_rows = rows_count % self._rows
                if last_rows == 0:
                    last_rows = self._rows
                self._logger.info(
                    f"Generated {output_filepath} with {last_rows} rows"
                    f" by read up to line {rows_count} of the original."
                )


class _CsvSplitMethodGrouped(_CsvSplitMethodBase):
    def execute(self, files: List[str]) -> None:
        valid2 = EssentialParameters(
            self.__class__.__name__,
            [
                self._key_column,
            ],
        )
        valid2()

        if len(files) >= 2:
            self._validate_headers(files)

        unique_keys, found_empty = chunk_size_handling(self._collect_unique_keys, files)
        if not unique_keys:
            raise ValueError(
                "No valid keys found in the specified column. No files will be created."
            )
        self._logger.info(f"Found {len(unique_keys)} unique key(s). Starting file split process.")

        if len(unique_keys) == 1:
            chunk_size_handling(self._process_single_key, files, unique_keys.pop(), found_empty)
        else:
            chunk_size_handling(self._process_multiple_keys, files, found_empty)

    def _validate_headers(self, files: List[str]) -> None:
        reference_header = pandas.read_csv(
            files[0], nrows=0, encoding=self._encoding
        ).columns.tolist()

        for file_path in files[1:]:
            current_header = pandas.read_csv(
                file_path, nrows=0, encoding=self._encoding
            ).columns.tolist()
            if current_header != reference_header:
                raise ValueError(
                    f"Header mismatch found. File '{files[0]}' header is {reference_header}, "
                    f"but file '{file_path}' header is {current_header}."
                )

    def _collect_unique_keys(self, chunksize: int, files: List[str]) -> Tuple[Set[str], bool]:
        unique_keys = set()
        found_empty = False
        for file_path in files:
            with pandas.read_csv(
                file_path,
                chunksize=chunksize,
                usecols=[self._key_column],
                keep_default_na=False,
                encoding=self._encoding,
            ) as reader:
                for chunk in reader:
                    cleaned_keys = chunk[self._key_column].astype(str).str.strip()
                    if not found_empty and (cleaned_keys == "").any():
                        found_empty = True
                    non_empty_keys = cleaned_keys[cleaned_keys != ""].unique()
                    unique_keys.update(non_empty_keys)
        return unique_keys, found_empty

    def _process_single_key(
        self, chunksize: int, files: List[str], key: str, found_empty: bool
    ) -> None:
        self._logger.info(f"Processing in single-key mode. Outputting to '{key}.csv'.")
        output_filename = f"{key}.csv"
        output_path = os.path.join(self.output_dir, output_filename)

        write_mode = "w"
        if found_empty is False:
            files = files.copy()
            first_file = files.pop()
            shutil.copy2(first_file, output_path)
            if len(files) == 0:
                # When single input file and single key and not found empty value, very fast.
                return
            write_mode = "a"

        is_first_write = True
        with open(output_path, write_mode, newline="", encoding=self._encoding) as f_out:
            for file_path in files:
                with pandas.read_csv(
                    file_path, chunksize=chunksize, keep_default_na=False, encoding=self._encoding
                ) as reader:
                    for chunk in reader:
                        if found_empty is False:
                            chunk.to_csv(f_out, header=False, index=False, encoding=self._encoding)
                        else:
                            filtered_chunk = chunk[chunk[self._key_column].astype(str) == key]
                            if not filtered_chunk.empty:
                                filtered_chunk.to_csv(
                                    f_out,
                                    header=is_first_write,
                                    index=False,
                                    encoding=self._encoding,
                                )
                                is_first_write = False

    def _process_multiple_keys(self, chunksize: int, files: List[str], found_empty: bool) -> None:
        self._logger.info("Processing in multi-key mode.")
        file_pointers = {}
        try:
            for file_path in files:
                with pandas.read_csv(
                    file_path, chunksize=chunksize, keep_default_na=False, encoding=self._encoding
                ) as reader:
                    for chunk in reader:
                        if found_empty:
                            chunk.dropna(subset=[self._key_column], inplace=True)
                            chunk[self._key_column] = (
                                chunk[self._key_column].astype(str).str.strip()
                            )
                            chunk = chunk[chunk[self._key_column] != ""]
                        if chunk.empty:
                            continue

                        for key, group_df in chunk.groupby(self._key_column):
                            output_filename = f"{key}.csv"
                            if output_filename not in file_pointers:
                                file_pointers[output_filename] = open(
                                    os.path.join(self.output_dir, output_filename),
                                    "w",
                                    newline="",
                                    encoding=self._encoding,
                                )
                                group_df.to_csv(
                                    file_pointers[output_filename], header=True, index=False
                                )
                            else:
                                group_df.to_csv(
                                    file_pointers[output_filename], header=False, index=False
                                )
        finally:
            for fp in file_pointers.values():
                fp.close()


def chunk_size_handling(read_csv_func, *args, **kwd):
    """
    Processing to avoid memory errors in pandas's read_csv.
    Use this function when you want to do the same handling when extending cliboa.
    """
    chunksize = 1024 * 1024
    while 0 < chunksize:
        try:
            return read_csv_func(chunksize, *args, **kwd)
        except MemoryError as error:
            if chunksize <= 1:
                raise error
            chunksize //= 2

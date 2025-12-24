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
from typing import Literal, Set, Tuple

import dask.dataframe as dask_df
import jsonlines
import pandas
from pydantic import ConfigDict, Field, computed_field, model_validator

from cliboa.adapter.csv import Csv
from cliboa.adapter.file import File
from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.base import _BaseObject  # _warn_deprecated_args
from cliboa.util.exception import FileNotFound, InvalidCount, InvalidParameter
from cliboa.util.string import StringUtil


class CsvColumnHash(FileBaseTransform):
    """
    Hash(SHA256) specific columns from csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        columns: list[str]

    def _stringToHash(self, string):
        return hashlib.sha256(string.encode()).hexdigest()

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            for c in self.args.columns:
                df[c] = df[c].apply(self._stringToHash)
            df.to_csv(
                fo,
                encoding=self.args.encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvColumnExtract(FileBaseTransform):
    """
    Remove specific columns from csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        model_config = ConfigDict(coerce_numbers_to_str=True)

        columns: list[str] | None = None
        column_numbers: str | None = None

    def execute(self, *args):
        if not self.args.columns and not self.args.column_numbers:
            raise InvalidParameter("Specifying either 'column' or 'column_numbers' is essential.")
        if self.args.columns and self.args.column_numbers:
            raise InvalidParameter("Cannot specify both 'column' and 'column_numbers'.")

        files = self.get_src_files()
        self.check_file_existence(files)

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self.args.columns:
            Csv.extract_columns_with_names(fi, fo, self.args.columns)
        elif self.args.column_numbers:
            if isinstance(self.args.column_numbers, int) is True:
                remain_column_numbers = [self.args.column_numbers]
            else:
                column_numbers = self.args.column_numbers.split(",")
                remain_column_numbers = [int(n) for n in column_numbers]
            Csv.extract_columns_with_numbers(fi, fo, remain_column_numbers)


class CsvColumnDelete(FileBaseTransform):
    """
    Delete specific columns from csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        regex_pattern: str

    def execute(self, *args):
        files = self.get_src_files()
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
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        pattern = re.compile(self.args.regex_pattern)
        for df in tfr:
            for column in df.columns.values:
                if pattern.fullmatch(column):
                    df = df.drop(column, axis=1)
                # output an empty file when all columns are deleted
                if df.empty:
                    df = df.dropna(how="all")
            df.to_csv(
                fo,
                encoding=self.args.encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvValueExtract(FileBaseTransform):
    """
    Extract a specific column from a CSV file and then replace it with a regular expression.
    """

    class Arguments(FileBaseTransform.Arguments):
        column_regex_pattern: dict[str, str]

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)
        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        with open(fi, mode="rt") as i:
            reader = csv.DictReader(i)
            with open(fo, mode="wt") as o:
                writer = csv.DictWriter(o, reader.fieldnames)
                writer.writeheader()
                for line in reader:
                    for column, regex_pattern in self.args.column_regex_pattern.items():
                        if re.search(regex_pattern, line[column]):
                            line[column] = re.search(regex_pattern, line[column]).group()
                        else:
                            line[column] = ""
                    writer.writerow(dict(line))


class CsvColumnConcat(FileBaseTransform):
    """
    Concat specific columns from csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        columns: list[str] = Field(min_length=2)
        dest_column_name: str
        sep: str = ""

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        dest_str = None
        for df in tfr:
            for c in self.args.columns:
                if dest_str is None:
                    dest_str = df[c].astype(str)
                else:
                    dest_str = dest_str + self.args.sep + df[c].astype(str)
                df = df.drop(columns=[c])
            df[self.args.dest_column_name] = dest_str
            df.to_csv(
                fo,
                encoding=self.args.encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvTypeConvert(FileBaseTransform):
    """
    Convert the type of specific column in a csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        dest_dir: str
        column: list[str]
        type: str

    def execute(self, *args):
        self.args.resolve_dest_dir()

        files = self.get_src_files()
        self.check_file_existence(files)
        self.logger.info("Files found %s" % files)
        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=object,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            for column in self.args.column:
                try:
                    if self.args.type == "int":
                        # When reading from csv, the following error occurs:
                        # ValueError: invalid literal for int() with base 10
                        # To avoid this, convert to float and then convert to int
                        df[column] = df[column].astype("float")
                        df[column] = df[column].astype("int")
                    else:
                        df[column] = df[column].astype(self.args.type)
                except Exception:
                    raise InvalidParameter(
                        "Conversion to this type is not possible. %s" % self.args.type
                    )
            df.to_csv(
                fo,
                encoding=self.args.encoding,
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

    def __init__(self, **kwargs):
        super().__init__(*kwargs)
        self.df_target_list = None

    class Arguments(FileBaseTransform.Arguments):
        target_compare_path: str
        src_column: str | None = None
        target_column: str | None = None
        all_column: bool = False

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        target = self._resolve("adapter_file", File).get_target_files(
            os.path.dirname(self.args.target_compare_path),
            os.path.basename(self.args.target_compare_path),
        )
        self.check_file_existence(target)

        if self.args.all_column and (self.args.src_column or self.args.target_column):
            raise KeyError("all_column cannot coexist with src_column or target_column.")

        header = pandas.read_csv(self.args.target_compare_path, nrows=0)
        if self.args.all_column is False and self.args.target_column not in header:
            raise KeyError(
                "Target Compare file does not exist target column [%s]." % self.args.target_column
            )

        if self.args.all_column:
            df_target = pandas.read_csv(self.args.target_compare_path, dtype=str)
            self.df_target_list = df_target.values.tolist()
        else:
            df_target = pandas.read_csv(
                self.args.target_compare_path, usecols=[self.args.target_column], dtype=str
            )
            self.df_target_list = df_target[self.args.target_column].values.tolist()

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self.args.all_column is False:
            header = pandas.read_csv(fi, dtype=str, encoding=self.args.encoding, nrows=0)
            try:
                header[self.args.src_column].values.tolist()
            except KeyError:
                raise KeyError(
                    "Src file does not exist target column [%s]." % self.args.target_column
                )

        self._csv_write(fi, fo)

    def _csv_write(self, fi, fo):
        # Used in chunk_size_handling
        df = pandas.read_csv(fi, dtype=str, na_filter=False)
        if self.args.all_column:
            df_target_set = {hash(tuple(row)) for row in self.df_target_list}
            df = df.drop(self._all_elements_match(df.values.tolist(), df_target_set))
        else:
            df = df[~df[self.args.src_column].isin(self.df_target_list)]
        df.to_csv(
            fo,
            encoding=self.args.encoding,
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

    class Arguments(FileBaseTransform.Arguments):
        adjust: dict[str, int]

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_writers(files, encoding=self.args.encoding)

        for fi, fo in super().io_writers(files, encoding=self.args.encoding):
            reader = csv.DictReader(fi)
            writer = csv.DictWriter(fo, reader.fieldnames)
            writer.writeheader()

            for row in reader:
                for k, v in self.args.adjust.items():
                    f1 = row.get(k)
                    if len(f1) > v:
                        row[k] = f1[:v]
                writer.writerow(row)
            fo.flush()


class CsvMerge(FileBaseTransform):
    """
    Merge two csv files
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._target_df = None

    class Arguments(FileBaseTransform.Arguments):
        dest_dir: str
        target_path: str
        join_on: str
        engine: Literal["pandas", "dask"] = "pandas"

    def execute(self, *args):
        self.args.resolve_dest_dir()

        source_files = self.get_src_files()
        if not self.check_file_existence(source_files):
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self.args.src_dir, self.args.src_pattern)
            )

        target_files = glob.glob(self.args.target_path)
        if len(target_files) == 0:
            raise InvalidCount("An target file %s does not exist." % self.target_path)
        elif len(target_files) > 1:
            self.logger.error("Hit target files %s" % target_files)
            raise InvalidCount("Target files must be only one.")

        if self.args.engine == "dask":
            self._dask_merge(source_files, target_files[0])
        else:
            self._pandas_merge(source_files, target_files[0])

    def _dask_merge(self, source_files: list[str], target_file: str) -> None:
        """
        merge using dask
        """
        dd_target = dask_df.read_csv(target_file, encoding=self.args.encoding)

        for source_file in source_files:
            dest_path = os.path.join(self.args.dest_dir, os.path.basename(source_file))
            dd_source = dask_df.read_csv(source_file, encoding=self.args.encoding)
            merged_dd = dask_df.merge(dd_source, dd_target, on=self.args.join_on)
            merged_dd.to_csv(dest_path, single_file=True, index=False, encoding=self.args.encoding)

    def _pandas_merge(self, source_files: list[str], target_file: str) -> None:
        """
        merge using pandas
        NOTE: if target file is too large, use dask engine.
        """
        self._target_df = pandas.read_csv(target_file)
        for source_file in source_files:
            chunk_size_handling(self._pandas_merge_one, source_file)

    def _pandas_merge_one(self, chunksize: int, source_file: str) -> None:
        dest_path = os.path.join(self.args.dest_dir, os.path.basename(source_file))

        chunks = pandas.read_csv(source_file, chunksize=chunksize, encoding=self.args.encoding)
        is_first_chunk = True
        for chunk in chunks:
            merged_chunk = pandas.merge(chunk, self._target_df, on=self.args.join_on)

            if is_first_chunk:
                merged_chunk.to_csv(
                    dest_path, index=False, mode="w", header=True, encoding=self.args.encoding
                )
                is_first_chunk = False
            else:
                merged_chunk.to_csv(
                    dest_path, index=False, mode="a", header=False, encoding=self.args.encoding
                )


class CsvColumnSelect(FileBaseTransform):
    """
    Select columns in Csv file in specified order
    """

    class Arguments(FileBaseTransform.Arguments):
        column_order: list[str]

    def execute(self, *args):
        files = self.get_src_files()
        if not self.check_file_existence(files):
            raise FileNotFound("No files are found.")

        super().io_files(files, func=self.convert)

    def convert(self, fi, fo):
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )
        for df in tfr:
            if set(self.args.column_order) - set(df.columns.values):
                raise InvalidParameter(
                    "column_order define not included target file's column : %s"
                    % (set(self.args.column_order) - set(df.columns.values))
                )
            df = df.loc[:, self.args.column_order]
            df.to_csv(
                fo,
                encoding=self.args.encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvConcat(FileBaseTransform):
    """
    Concat csv files
    """

    class Arguments(FileBaseTransform.Arguments):
        src_pattern: str | None = None
        src_filenames: list[str] | None = None
        dest_dir: str
        dest_name: str

        @model_validator(mode="before")
        def check_exclusive_fields(cls, data: dict) -> dict:
            if not isinstance(data, dict):
                raise ValueError(f"arguments is not dict: {data}")

            filenames_present = "src_filenames" in data
            pattern_present = "src_pattern" in data
            if not pattern_present and not filenames_present:
                raise InvalidParameter(
                    "Specifying either 'src_pattern' or 'src_filenames' is essential."
                )
            elif pattern_present and filenames_present:
                raise InvalidParameter("Cannot specify both 'src_pattern' and 'src_filenames'.")
            return data

    def execute(self, *args):
        if self.args.src_pattern:
            files = self.get_src_files()
        else:
            files = []
            for file in self.args.src_filenames:
                files.append(os.path.join(self.args.src_dir, file))

        if len(files) == 0:
            raise FileNotFound("No files are found.")
        elif len(files) == 1:
            self.logger.warning("Two or more input files are required.")

        # Create output headers to conform to the concat specification.
        file_1 = files[0]
        output_header = pandas.read_csv(
            file_1, dtype=str, encoding=self.args.encoding, nrows=0, na_filter=False
        )
        for file in files[1:]:
            output_header = pandas.concat(
                [
                    output_header,
                    pandas.read_csv(
                        file, dtype=str, encoding=self.args.encoding, nrows=0, na_filter=False
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
                encoding=self.args.encoding,
                chunksize=chunksize,
                na_filter=False,
            )
            for df in tfr:
                # Change the header order to the one you plan to output.
                df = pandas.concat([output_header, df])
                df.to_csv(
                    os.path.join(self.args.resolve_dest_dir(), self.args.dest_name),
                    encoding=self.args.encoding,
                    header=True if first_write else False,
                    index=False,
                    mode="w" if first_write else "a",
                )
                first_write = False


class CsvConvert(FileBaseTransform):
    """
    Change csv format
    """

    class Arguments(FileBaseTransform.Arguments):
        headers: list[dict[str, str]] = Field(default_factory=list)
        headers_existence: bool = True
        add_headers: list[str] | None = None
        before_format: str = "csv"
        before_enc: str = "utf-8"
        before_escapechar: str | None = None
        after_format: str | None = None
        after_enc: str | None = None
        after_nl: str = "LF"
        after_escapechar: str | None = None
        reader_quote: str = "QUOTE_MINIMAL"
        quote: str = "QUOTE_MINIMAL"

    def execute(self, *args):
        if self.args.after_format is None:
            self.args.after_format = self.args.before_format

        if self.args.after_enc is None:
            self.args.after_enc = self.args.before_enc

        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, ext=self.args.after_format, func=self.convert)

    def convert(self, fi, fo):
        with open(fi, mode="rt", encoding=self.args.before_enc) as i:
            reader = csv.reader(
                i,
                delimiter=Csv.delimiter_convert(self.args.before_format),
                quoting=Csv.quote_convert(self.args.reader_quote),
                escapechar=self.args.before_escapechar,
                doublequote=False if self.args.before_escapechar else True,
            )
            with open(fo, mode="wt", newline="", encoding=self.args.after_enc) as o:
                writer = csv.writer(
                    o,
                    delimiter=Csv.delimiter_convert(self.args.after_format),
                    quoting=Csv.quote_convert(self.args.quote),
                    lineterminator=Csv.newline_convert(self.args.after_nl),
                    escapechar=self.args.after_escapechar,
                    doublequote=False if self.args.after_escapechar else True,
                )

                for i, line in enumerate(reader):
                    if i == 0:
                        if self.args.headers_existence is False:
                            continue
                        if self.args.add_headers:
                            writer.writerow(self.args.add_headers)
                            writer.writerow(line)
                        else:
                            writer.writerow(self._replace_headers(line))
                    else:
                        writer.writerow(line)

    def _replace_headers(self, old_headers):
        """
        Replace old headers to new headers
        """
        if not self.args.headers:
            return old_headers

        converter = {}
        for headers in self.args.headers:
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

    class Arguments(FileBaseTransform.Arguments):
        dest_dir: str
        order: list = []
        quote: str = "QUOTE_MINIMAL"
        no_duplicate: bool = False

    def execute(self, *args):
        self.args.resolve_dest_dir()

        files = self.get_src_files()
        self.check_file_existence(files)

        ymd_hms = datetime.now().strftime("%Y%m%d%H%M%S%f")
        dbname = ".%s_%s.db" % (ymd_hms, StringUtil().random_str(8))
        tblname = "temp_table"

        sqlite = SqliteAdapter()
        sqlite.connect(dbname)
        try:
            for file in files:
                _, filename = os.path.split(file)
                dest_file = os.path.join(self.args.dest_dir, filename)

                sqlite.import_table(file, tblname, encoding=self.args.encoding)
                sqlite.export_table(
                    tblname,
                    dest_file,
                    quoting=Csv.quote_convert(self.args.quote),
                    encoding=self.args.encoding,
                    order=self.args.order,
                    no_duplicate=self.args.no_duplicate,
                )
            sqlite.close()
        finally:
            os.remove(dbname)


class CsvToJsonl(FileBaseTransform):
    """
    Transform csv to jsonlines.
    """

    class Arguments(FileBaseTransform.Arguments):
        pass

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        super().io_files(files, ext="jsonl", func=self.convert)

    def convert(self, fi, fo):
        with (
            open(fi, mode="r", encoding=self.args.encoding, newline="") as i,
            jsonlines.open(fo, mode="w") as writer,
        ):
            reader = csv.DictReader(i)
            for row in reader:
                writer.write(row)


class CsvColumnCopy(FileBaseTransform):
    """
    Copy column data (new or overwrite)
    """

    class Arguments(FileBaseTransform.Arguments):
        dest_dir: str
        src_column: str
        dest_column: str

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        header = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            nrows=0,
            na_filter=False,
        )
        if self.args.src_column not in header:
            raise KeyError("Copy source column does not exist in file. [%s]" % self.args.src_column)

        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )

        for df in tfr:
            df[self.args.dest_column] = df[self.args.src_column]
            df.to_csv(
                fo,
                encoding=self.args.encoding,
                header=True if first_write else False,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvColumnReplace(FileBaseTransform):
    """
    Replace matching regular expression values for a specific column from a csv file.
    """

    class Arguments(FileBaseTransform.Arguments):
        column: str
        regex_pattern: str
        rep_str: str

        @computed_field
        @property
        def regex_compile(self) -> re.Pattern:
            return re.compile(self.regex_pattern)

    def _replace_string(self, string):
        return self.args.regex_compile.sub(self.args.rep_str, string)

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        header = pandas.read_csv(
            fi, dtype=str, encoding=self.args.encoding, nrows=0, na_filter=False
        )
        if self.args.column not in header:
            raise KeyError("Replace source column does not exist in file. [%s]" % self.args.column)

        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        # Used in chunk_size_handling
        first_write = True
        tfr = pandas.read_csv(
            fi,
            dtype=str,
            encoding=self.args.encoding,
            chunksize=chunksize,
            na_filter=False,
        )

        for df in tfr:
            df[self.args.column] = df[self.args.column].apply(self._replace_string)
            df.to_csv(
                fo,
                header=True if first_write else False,
                encoding=self.args.encoding,
                index=False,
                mode="w" if first_write else "a",
            )
            first_write = False


class CsvDuplicateRowDelete(FileBaseTransform):

    class Arguments(FileBaseTransform.Arguments):
        delimiter: str = ","
        engine: Literal["pandas", "dask"] = "pandas"

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)

        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        if self.args.engine == "dask":
            self._convert_with_dask(fi, fo)
        else:
            self._convert_with_pandas(fi, fo)

    def _convert_with_pandas(self, fi, fo):
        """
        Memory-efficient duplicate removal using pandas with set for tracking
        seen rows.

        Preserves original row order.
        """
        # Use existing chunk_size_handling infrastructure
        chunk_size_handling(self._read_csv_func, fi, fo)

    def _read_csv_func(self, chunksize, fi, fo):
        """
        Process CSV in chunks with duplicate removal.
        Used by chunk_size_handling for memory-efficient processing.
        """
        # Initialize state for each execution
        seen_rows = set()
        first_write = True

        for chunk in pandas.read_csv(
            fi,
            delimiter=self.args.delimiter,
            dtype=str,
            na_filter=False,
            chunksize=chunksize,
            header=None,
            encoding=self.args.encoding,
        ):
            # Convert DataFrame to list of tuples for hashability
            unique_rows = []
            for _, row in chunk.iterrows():
                row_tuple = tuple(row.values)
                if row_tuple not in seen_rows:
                    seen_rows.add(row_tuple)
                    unique_rows.append(row.values)

            # Write unique rows to output file
            if unique_rows:
                unique_df = pandas.DataFrame(unique_rows)
                unique_df.to_csv(
                    fo,
                    mode="w" if first_write else "a",
                    header=False,
                    index=False,
                    sep=self.args.delimiter,
                    encoding=self.args.encoding,
                )
                first_write = False

    def _convert_with_dask(self, fi, fo):
        """
        Handle very large files using dask.

        May not preserve original row order but provides better memory
        efficiency for extremely large datasets.
        """
        self.logger.info("Using dask engine - original row order may not be preserved")

        # Read CSV with dask
        df = dask_df.read_csv(
            fi,
            delimiter=self.args.delimiter,
            dtype=str,
            na_filter=False,
            encoding=self.args.encoding,
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
            sep=self.args.delimiter,
            encoding=self.args.encoding,
        )


class CsvRowDelete(FileBaseTransform):
    class Arguments(FileBaseTransform.Arguments):
        alter_path: str
        src_key_column: str
        alter_key_column: str
        delimiter: str = ","
        has_match: bool = True

    def execute(self, *args):
        files = self.get_src_files()

        self.check_file_existence(files)
        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        s = set()
        with open(self.args.alter_path, "r") as al:
            alt_reader = csv.DictReader(al, delimiter=self.args.delimiter)
            for alt_row in alt_reader:
                s.add(alt_row[self.args.alter_key_column])
        with open(fi, "r") as i:
            reader = csv.DictReader(i, delimiter=self.args.delimiter)
            with open(fo, "w", newline="") as o:
                writer = csv.DictWriter(
                    o, fieldnames=reader.fieldnames, delimiter=self.args.delimiter
                )
                writer.writeheader()
                for row in reader:
                    if self.args.has_match is True and row[self.args.src_key_column] not in s:
                        writer.writerow(row)
                    elif self.args.has_match is False and row[self.args.src_key_column] in s:
                        writer.writerow(row)


class CsvSplit(FileBaseTransform):
    """
    Split csv files, using the value of a specific column as the output filename.

    - The output filename is the value from the specified column, with a .csv extension appended.
    - If the value of the specified column is empty, that row should be ignored.
    - When multiple source files exist, the column definitions must all be the same.
    """

    class Arguments(FileBaseTransform.Arguments):
        method: Literal["rows", "grouped"]
        key_column: str | None = None
        rows: int | None = None
        suffix_format: str = ".{:02d}"

    def execute(self, *args) -> None:
        files = self.get_src_files()
        self.check_file_existence(files)

        if self.args.method == "rows":
            executeInstance = _CsvSplitMethodRows(self.args)
        elif self.args.method == "grouped":
            executeInstance = _CsvSplitMethodGrouped(self.args)
        else:
            raise NotImplementedError(
                f"Defined {self.args.method} is not implemented logic in execute."
            )
        executeInstance.execute(files)


class _CsvSplitMethodBase(_BaseObject):
    def __init__(self, args):
        super().__init__()
        self.args = args

    def execute(self, files: list[str]) -> None:
        raise NotImplementedError("Please implement execute method.")


class _CsvSplitMethodRows(_CsvSplitMethodBase):
    def execute(self, files: list[str]) -> None:
        valid2 = EssentialParameters(
            self.__class__.__name__,
            [
                self.args.rows,
            ],
        )
        valid2()
        for filepath in files:
            self._split_one(filepath)

    def _split_one(self, filepath: str) -> None:
        self._logger.info("Split {:s} per {:d} rows".format(filepath, self.args.rows))
        file_name, ext = os.path.splitext(os.path.basename(filepath))
        with open(filepath, "r", encoding=self.args.encoding, newline="") as f_in:
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
                # new file per self.args.rows
                if rows_count % self.args.rows == 0:
                    # close file-pointer if exists
                    if f_out:
                        f_out.close()
                        self._logger.info(
                            f"Generated {output_filepath} with {self.args.rows} rows"
                            f" by read up to line {rows_count} of the original."
                        )
                    # open new file-pointer
                    suffix = self.args.suffix_format.format(file_index)
                    output_filepath = os.path.join(
                        self.args.resolve_dest_dir(), f"{file_name}{suffix}{ext}"
                    )
                    f_out = open(output_filepath, "w", encoding=self.args.encoding, newline="")
                    writer = csv.writer(f_out)
                    writer.writerow(header)
                    file_index += 1
                # write current line
                writer.writerow(line)
                rows_count += 1
            # close last file-pointer if exists
            if f_out:
                f_out.close()
                last_rows = rows_count % self.args.rows
                if last_rows == 0:
                    last_rows = self.args.rows
                self._logger.info(
                    f"Generated {output_filepath} with {last_rows} rows"
                    f" by read up to line {rows_count} of the original."
                )


class _CsvSplitMethodGrouped(_CsvSplitMethodBase):
    def execute(self, files: list[str]) -> None:
        valid2 = EssentialParameters(
            self.__class__.__name__,
            [
                self.args.key_column,
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

    def _validate_headers(self, files: list[str]) -> None:
        reference_header = pandas.read_csv(
            files[0], nrows=0, encoding=self.args.encoding
        ).columns.tolist()

        for file_path in files[1:]:
            current_header = pandas.read_csv(
                file_path, nrows=0, encoding=self.args.encoding
            ).columns.tolist()
            if current_header != reference_header:
                raise ValueError(
                    f"Header mismatch found. File '{files[0]}' header is {reference_header}, "
                    f"but file '{file_path}' header is {current_header}."
                )

    def _collect_unique_keys(self, chunksize: int, files: list[str]) -> Tuple[Set[str], bool]:
        unique_keys = set()
        found_empty = False
        for file_path in files:
            with pandas.read_csv(
                file_path,
                chunksize=chunksize,
                usecols=[self.args.key_column],
                keep_default_na=False,
                encoding=self.args.encoding,
            ) as reader:
                for chunk in reader:
                    cleaned_keys = chunk[self.args.key_column].astype(str).str.strip()
                    if not found_empty and (cleaned_keys == "").any():
                        found_empty = True
                    non_empty_keys = cleaned_keys[cleaned_keys != ""].unique()
                    unique_keys.update(non_empty_keys)
        return unique_keys, found_empty

    def _process_single_key(
        self, chunksize: int, files: list[str], key: str, found_empty: bool
    ) -> None:
        self._logger.info(f"Processing in single-key mode. Outputting to '{key}.csv'.")
        output_filename = f"{key}.csv"
        output_path = os.path.join(self.args.resolve_dest_dir(), output_filename)

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
        with open(output_path, write_mode, newline="", encoding=self.args.encoding) as f_out:
            for file_path in files:
                with pandas.read_csv(
                    file_path,
                    chunksize=chunksize,
                    keep_default_na=False,
                    encoding=self.args.encoding,
                ) as reader:
                    for chunk in reader:
                        if found_empty is False:
                            chunk.to_csv(
                                f_out, header=False, index=False, encoding=self.args.encoding
                            )
                        else:
                            filtered_chunk = chunk[chunk[self.args.key_column].astype(str) == key]
                            if not filtered_chunk.empty:
                                filtered_chunk.to_csv(
                                    f_out,
                                    header=is_first_write,
                                    index=False,
                                    encoding=self.args.encoding,
                                )
                                is_first_write = False

    def _process_multiple_keys(self, chunksize: int, files: list[str], found_empty: bool) -> None:
        self._logger.info("Processing in multi-key mode.")
        file_pointers = {}
        try:
            for file_path in files:
                with pandas.read_csv(
                    file_path,
                    chunksize=chunksize,
                    keep_default_na=False,
                    encoding=self.args.encoding,
                ) as reader:
                    for chunk in reader:
                        if found_empty:
                            chunk.dropna(subset=[self.args.key_column], inplace=True)
                            chunk[self.args.key_column] = (
                                chunk[self.args.key_column].astype(str).str.strip()
                            )
                            chunk = chunk[chunk[self.args.key_column] != ""]
                        if chunk.empty:
                            continue

                        for key, group_df in chunk.groupby(self.args.key_column):
                            output_filename = f"{key}.csv"
                            if output_filename not in file_pointers:
                                file_pointers[output_filename] = open(
                                    os.path.join(self.args.resolve_dest_dir(), output_filename),
                                    "w",
                                    newline="",
                                    encoding=self.args.encoding,
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

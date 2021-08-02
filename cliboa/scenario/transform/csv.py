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

import codecs
import csv
import jsonlines
import os
import pandas
from cliboa.core.validator import EssentialParameters
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.csv import Csv
from cliboa.util.exception import (
    FileNotFound,
    InvalidCount,
    InvalidParameter,
)
from cliboa.util.file import File
from cliboa.util.sqlite import SqliteAdapter
from cliboa.util.string import StringUtil
from datetime import datetime


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
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        if not self._columns and not self._column_numbers:
            raise InvalidParameter(
                "Specifying either 'column' or 'column_numbers' is essential."
            )
        if self._columns and self._column_numbers:
            raise InvalidParameter("Cannot specify both 'column' and 'column_numbers'.")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for fi, fo in super().io_files(files):
            if self._columns:
                Csv.extract_columns_with_names(fi, fo, self._columns)
            elif self._column_numbers:
                if isinstance(self._column_numbers, int) is True:
                    remain_column_numbers = []
                    remain_column_numbers.append(self._column_numbers)
                else:
                    column_numbers = self._column_numbers.split(",")
                    remain_column_numbers = [int(n) for n in column_numbers]
                Csv.extract_columns_with_numbers(fi, fo, remain_column_numbers)


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
            [
                self._src_dir,
                self._src_pattern,
                self._adjust,
            ],
        )
        valid()

        # TODO All the statements inside 'if' block will be deleted in the near future.
        if self._dest_path:
            self._logger.warning("'dest_path' will be unavailable in the near future.")

            files = super().get_target_files(self._src_dir, self._src_pattern)
            if len(files) != 1:
                raise Exception("Input file must be only one.")
            self._logger.info("Files found %s" % files)

            with codecs.open(
                files[0], mode="r", encoding=self._encoding
            ) as fi, codecs.open(
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
        else:
            files = super().get_target_files(self._src_dir, self._src_pattern)
            self.check_file_existence(files)
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
            ],
        )
        valid()

        if self._dest_pattern:
            self._logger.warning(
                "'dest_pattern' will be unavailable in the near future."
                + "'dest_pattern' will change to 'dest_name'."
            )
        else:
            valid = EssentialParameters(self.__class__.__name__, [self._dest_name])
            valid()

        target1_files = File().get_target_files(self._src_dir, self._src1_pattern)
        target2_files = File().get_target_files(self._src_dir, self._src2_pattern)
        if len(target1_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self._src1_pattern)
            )
        elif len(target2_files) == 0:
            raise InvalidCount(
                "An input file %s does not exist."
                % os.path.join(self._src_dir, self._src2_pattern)
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

        # TODO All the statements inside 'if' block will be deleted in the near future.
        if self._dest_pattern:
            dest_name = self._dest_pattern
        else:
            dest_name = self._dest_name

        df.to_csv(
            os.path.join(self._dest_dir, dest_name),
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
            self.__class__.__name__, [self._src_dir, self._dest_dir]
        )
        valid()

        if self._dest_pattern:
            self._logger.warning(
                "'dest_pattern' will be unavailable in the near future."
                + "'dest_pattern' will change to 'dest_name'."
            )
        else:
            valid = EssentialParameters(self.__class__.__name__, [self._dest_name])
            valid()

        if not self._src_pattern and not self._src_filenames:
            raise InvalidParameter(
                "Specifying either 'src_pattern' or 'src_filenames' is essential."
            )
        if self._src_pattern and self._src_filenames:
            raise InvalidParameter(
                "Cannot specify both 'src_pattern' and 'src_filenames'."
            )

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

        # TODO All the statements inside 'if' block will be deleted in the near future.
        if self._dest_pattern:
            dest_name = self._dest_pattern
        else:
            dest_name = self._dest_name

        df1.to_csv(
            os.path.join(self._dest_dir, dest_name),
            encoding=self._encoding,
            index=False,
        )


class CsvHeaderConvert(FileBaseTransform):
    """
    Convert csv headers

    Deprecated.
    Please Use CsvConvert instead.
    """

    def __init__(self):
        super().__init__()
        self._headers = []

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        self._logger.warning("Deprecated. Please Use CsvConvert instead.")

        if self._dest_pattern:
            self._logger.warning(
                "'dest_pattern' will be unavailable in the near future."
                + "Basically every classes which extends FileBaseTransform will be allowed"
                + " plural input files, and output files will be the same name with input"
                + " file names.\n"
                "At that time, if 'dest_dir' is given, transformed files will be created in the given directory.\n"  # noqa
                + "If not, original files will be updated by transformed files."
            )

            valid = EssentialParameters(
                self.__class__.__name__,
                [
                    self._src_dir,
                    self._src_pattern,
                    self._dest_dir,
                    self._dest_pattern,
                    self._headers,
                ],
            )
            valid()

            target_files = super().get_target_files(self._src_dir, self._src_pattern)
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
                "Convert header of %s. An output file is %s."
                % (target_files[0], dest_path)
            )
            with open(target_files[0], "r", encoding=self._encoding) as s, open(
                dest_path, "w", encoding=self._encoding
            ) as d:
                reader = csv.reader(s)
                writer = csv.writer(d, quoting=csv.QUOTE_ALL)
                headers = next(reader, None)
                new_headers = self._replace_headers(headers)
                writer.writerow(new_headers)
                for r in reader:
                    writer.writerow(r)
                d.flush()
        else:
            valid = EssentialParameters(
                self.__class__.__name__,
                [
                    self._src_dir,
                    self._src_pattern,
                    self._headers,
                ],
            )
            valid()

            files = super().get_target_files(self._src_dir, self._src_pattern)
            self.check_file_existence(files)

            for fi, fo in super().io_writers(files, encoding=self._encoding):
                self._logger.info(
                    "Convert header of %s. An output file is %s." % (fi, fo)
                )
                reader = csv.reader(fi)
                writer = csv.writer(fo, quoting=csv.QUOTE_ALL)
                headers = next(reader, None)
                new_headers = self._replace_headers(headers)
                writer.writerow(new_headers)
                for r in reader:
                    writer.writerow(r)

    def _replace_headers(self, old_headers):
        """
        Replace old headers to new headers
        """
        converter = {}
        for headers in self._headers:
            for k, v in headers.items():
                converter[k] = v

        new_headers = []
        for oh in old_headers:
            r = converter.get(oh)
            new_headers.append(r if r is not None else oh)

        return new_headers


class CsvFormatChange(FileBaseTransform):
    """
    Change csv format

    Deprecated.
    Please Use CsvConvert instead.
    """

    def __init__(self):
        super().__init__()
        self._before_format = None
        self._before_enc = None
        self._after_format = None
        self._after_enc = None
        self._after_nl = "LF"
        self._quote = "QUOTE_MINIMAL"

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

    def quote(self, quote):
        self._quote = quote

    def execute(self, *args):
        self._logger.warning("Deprecated. Please Use CsvConvert instead.")

        # TODO All the statements inside 'if' block will be deleted in the near future.
        if self._dest_pattern:
            self._logger.warning(
                "'dest_pattern' will be unavailable in the near future."
                + "Basically every classes which extends FileBaseTransform will be allowed"
                + " plural input files, and output files will be the same name with input"
                + " file names.\n"
                "At that time, if 'dest_dir' is given, transformed files will be created in the given directory.\n"  # noqa
                + "If not, original files will be updated by transformed files."
            )

            # essential parameters check
            valid = EssentialParameters(
                self.__class__.__name__,
                [
                    self._src_dir,
                    self._src_pattern,
                    self._before_format,
                    self._before_enc,
                    self._after_format,
                    self._after_enc,
                    self._dest_dir,
                    self._dest_pattern,
                ],
            )
            valid()

            files = super().get_target_files(self._src_dir, self._src_pattern)
            if len(files) != 1:
                raise Exception("Input file must be only one.")
            self._logger.info("Files found %s" % files)

            with open(files[0], mode="rt", encoding=self._before_enc) as i:
                reader = csv.reader(
                    i, delimiter=Csv.delimiter_convert(self._before_format)
                )
                with open(
                    os.path.join(self._dest_dir, self._dest_pattern),
                    mode="wt",
                    newline="",
                    encoding=self._after_enc,
                ) as o:
                    writer = csv.writer(
                        o,
                        delimiter=Csv.delimiter_convert(self._after_format),
                        quoting=Csv.quote_convert(self._quote),
                        lineterminator=Csv.newline_convert(self._after_nl),
                    )
                    with open(
                        os.path.join(self._dest_dir, self._dest_pattern),
                        mode="wt",
                        newline="",
                        encoding=self._after_enc,
                    ) as o:
                        writer = csv.writer(
                            o,
                            delimiter=Csv.delimiter_convert(self._after_format),
                            quoting=Csv.quote_convert(self._quote),
                            lineterminator=Csv.newline_convert(self._after_nl),
                        )
                        for line in reader:
                            writer.writerow(line)
        else:
            valid = EssentialParameters(
                self.__class__.__name__,
                [
                    self._src_dir,
                    self._src_pattern,
                    self._before_format,
                    self._before_enc,
                    self._after_format,
                    self._after_enc,
                ],
            )
            valid()

            files = super().get_target_files(self._src_dir, self._src_pattern)
            self.check_file_existence(files)

            for fi, fo in super().io_files(files, ext=self._after_format):
                with open(fi, mode="rt", encoding=self._before_enc) as i:
                    reader = csv.reader(
                        i, delimiter=Csv.delimiter_convert(self._before_format)
                    )
                    with open(
                        fo,
                        mode="wt",
                        newline="",
                        encoding=self._after_enc,
                    ) as o:
                        writer = csv.writer(
                            o,
                            delimiter=Csv.delimiter_convert(self._after_format),
                            quoting=Csv.quote_convert(self._quote),
                            lineterminator=Csv.newline_convert(self._after_nl),
                        )
                        for line in reader:
                            writer.writerow(line)


class CsvConvert(FileBaseTransform):
    """
    Change csv format
    """

    def __init__(self):
        super().__init__()
        self._headers = []
        self._before_format = "csv"
        self._before_enc = self._encoding
        self._after_format = None
        self._after_enc = None
        self._after_nl = "LF"
        self._quote = "QUOTE_MINIMAL"

    def headers(self, headers):
        self._headers = headers

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

    def quote(self, quote):
        self._quote = quote

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._src_dir,
                self._src_pattern,
                self._before_format,
                self._before_enc,
            ],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        if self._after_format is None:
            self._after_format = self._before_format
        if self._after_enc is None:
            self._after_enc = self._before_enc

        for fi, fo in super().io_files(files, ext=self._after_format):
            with open(fi, mode="rt", encoding=self._before_enc) as i:
                reader = csv.reader(
                    i, delimiter=Csv.delimiter_convert(self._before_format)
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
            [
                self._order,
                self._src_dir,
                self._src_pattern,
                self._dest_dir,
            ],
        )
        valid()

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
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for fi, fo in super().io_files(files, ext="jsonl"):
            with open(
                fi, mode="r", encoding=self._encoding, newline=""
            ) as i, jsonlines.open(fo, mode="w") as writer:
                reader = csv.DictReader(i)
                for row in reader:
                    writer.write(row)

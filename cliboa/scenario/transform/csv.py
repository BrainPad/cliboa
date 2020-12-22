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
import os

import pandas

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.csv import Csv
from cliboa.util.exception import CliboaException, FileNotFound, InvalidCount, InvalidParameter
from cliboa.util.file import File


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
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._dest_dir],
        )
        valid()

        if not self._columns and not self._column_numbers:
            raise InvalidParameter(
                "Specifying either 'column' or 'column_numbers' is essential."
            )
        if self._columns and self._column_numbers:
            raise InvalidParameter("Cannot specify both 'column' and 'column_numbers'.")

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("The specified csv file not found.")

        for f in files:
            _, filename = os.path.split(f)
            dest_path = os.path.join(self._dest_dir, filename)
            if self._columns:
                Csv.extract_columns_with_names(f, dest_path, self._columns)
            elif self._column_numbers:
                if isinstance(self._column_numbers, int) is True:
                    remain_column_numbers = []
                    remain_column_numbers.append(self._column_numbers)
                else:
                    column_numbers = self._column_numbers.split(",")
                    remain_column_numbers = [int(n) for n in column_numbers]
                Csv.extract_columns_with_numbers(f, dest_path, remain_column_numbers)


class CsvColsExtract(FileBaseTransform):
    """
    Remove columns from csv file.
    """

    def __init__(self):
        super().__init__()
        self._columns = None

    def columns(self, columns):
        self._columns = columns

    def execute(self, *args):
        file = super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._columns])
        valid()

        File().remove_columns(file, self._dest_path, self._columns)


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
                self._dest_pattern,
            ],
        )
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
        df.to_csv(
            os.path.join(self._dest_dir, self._dest_pattern),
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
            self.__class__.__name__,
            [self._src_dir, self._dest_dir, self._dest_pattern],
        )
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

        if len(files) < 2:
            raise InvalidCount("Two or more input files are required.")

        file = files.pop(0)
        df1 = pandas.read_csv(file, dtype=str, encoding=self._encoding,)

        for file in files:
            df2 = pandas.read_csv(file, dtype=str, encoding=self._encoding,)
            df1 = pandas.concat([df1, df2])

        df1.to_csv(
            os.path.join(self._dest_dir, self._dest_pattern),
            encoding=self._encoding,
            index=False,
        )


class CsvHeaderConvert(FileBaseTransform):
    """
    Convert csv headers
    """

    def __init__(self):
        super().__init__()
        self._headers = []

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        # essential parameters check
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
    """

    def __init__(self):
        super().__init__()
        self._before_format = None
        self._before_enc = None
        self._after_format = None
        self._after_enc = None
        self._after_nl = "LF"
        self._quote = "QUOTE_MINIMAL"

    @property
    def before_format(self):
        return self._before_format

    @before_format.setter
    def before_format(self, before_format):
        self._before_format = before_format

    @property
    def before_enc(self):
        return self._before_enc

    @before_enc.setter
    def before_enc(self, before_enc):
        self._before_enc = before_enc

    @property
    def after_format(self):
        return self._after_format

    @after_format.setter
    def after_format(self, after_format):
        self._after_format = after_format

    @property
    def after_enc(self):
        return self._after_enc

    @after_enc.setter
    def after_enc(self, after_enc):
        self._after_enc = after_enc

    @property
    def after_nl(self):
        return self._after_nl

    @after_nl.setter
    def after_nl(self, after_nl):
        self._after_nl = after_nl

    @property
    def quote(self):
        return self._quote

    @quote.setter
    def quote(self, quote):
        self._quote = quote

    def execute(self, *args):
        file = super().execute()
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__,
            [
                self._before_format,
                self._before_enc,
                self._after_format,
                self._after_enc,
                self._dest_dir,
                self._dest_pattern,
            ],
        )
        valid()

        with open(file, mode="rt", encoding=self._before_enc) as i:
            reader = csv.reader(i, delimiter=self._csv_delimiter(self._before_format))
            with open(
                os.path.join(self._dest_dir, self._dest_pattern),
                mode="wt",
                newline="",
                encoding=self._after_enc,
            ) as o:
                writer = csv.writer(
                    o,
                    delimiter=self._csv_delimiter(self._after_format),
                    quoting=self._csv_quote(),
                    lineterminator=self._csv_newline(),
                )
                for line in reader:
                    writer.writerow(line)

    def _csv_newline(self):
        if self._after_nl.upper() == "LF":
            return "\n"
        elif self._after_nl.upper() == "CR":
            return "\r"
        elif self._after_nl.upper() == "CRLF":
            return "\r\n"
        else:
            raise CliboaException(
                "Unknown New LIne. One of the followings are allowd [LF, CR, CRLF]"
            )

    def _csv_delimiter(self, ext):
        if ext.upper() == "CSV":
            return ","
        elif ext.upper() == "TSV":
            return "\t"
        else:
            raise CliboaException(
                "Unknown ext. One of the followings are allowd [CSV, TSV]"
            )

    def _csv_quote(self):
        if "QUOTE_ALL" == self._quote:
            return csv.QUOTE_ALL
        elif "QUOTE_MINIMAL" == self._quote:
            return csv.QUOTE_MINIMAL
        elif "QUOTE_NONNUMERIC" == self._quote:
            return csv.QUOTE_NONNUMERIC
        elif "QUOTE_NONE" == self._quote:
            return csv.QUOTE_NONE
        else:
            raise CliboaException(
                "Unknown quote. One of the followings are allowd [QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONNUMERIC, QUOTE_NONE]"  # noqa
            )

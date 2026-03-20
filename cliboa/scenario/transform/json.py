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
from abc import abstractmethod

import jsonlines
import pandas

from cliboa.adapter.csv import Csv
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.base import _warn_deprecated_args


class JsonlToCsvBase(FileBaseTransform):
    """
    Base class of jsonlines transform to csv.
    """

    class Arguments(FileBaseTransform.Arguments):
        quote: str = "QUOTE_MINIMAL"
        after_nl: str = "LF"
        escape_char: str | None = None

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _quote(self):
        return self.args.quote

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _after_nl(self):
        return self.args.after_nl

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _escape_char(self):
        return self.args.escape_char

    @abstractmethod
    def convert_row(self, row):
        pass

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)
        self.io_files(files, ext="csv", func=self.convert)

    def convert(self, fi, fo):
        writer = None
        with (
            jsonlines.open(fi) as reader,
            open(fo, mode="w", encoding=self.args.encoding, newline="") as f,
        ):
            for row in reader:
                new_rows = self.convert_row(row)
                if not new_rows:
                    continue
                if not writer:
                    writer = csv.DictWriter(
                        f,
                        new_rows[0].keys(),
                        quoting=Csv.quote_convert(self.args.quote),
                        lineterminator=Csv.newline_convert(self.args.after_nl),
                        escapechar=self.args.escape_char,
                    )
                    writer.writeheader()
                writer.writerows(new_rows)


class JsonlToCsv(JsonlToCsvBase):
    """
    Transform jsonlines to csv.
    """

    def convert_row(self, row):
        return [row]


class JsonlAddKeyValue(FileBaseTransform):
    """
    Insert key value to jsonlines.
    """

    class Arguments(FileBaseTransform.Arguments):
        pairs: dict

    def execute(self, *args):
        files = self.get_src_files()
        self.check_file_existence(files)
        self.io_files(files, func=self.convert)

    def convert(self, fi, fo):
        with open(fi) as fi, open(fo, mode="w") as fo:
            df = pandas.read_json(fi, orient="records", lines=True)
            for key, value in self.args.pairs.items():
                df[key] = value
            df.to_json(fo, orient="records", lines=True)

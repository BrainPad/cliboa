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
from abc import abstractmethod
import csv
import jsonlines

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.csv import Csv


class JsonlToCsvBase(FileBaseTransform):
    """
    Base class of jsonlines transform to csv.
    """
    def __init__(self):
        super().__init__()
        self._quote = "QUOTE_MINIMAL"
        self._after_nl = "LF"
        self._escape_char = None

    def quote(self, quote):
        self._quote = quote

    def after_nl(self, after_nl):
        self._after_nl = after_nl

    def escape_char(self, escape_char):
        self._escape_char = escape_char

    @abstractmethod
    def convert_row(self, row):
        pass

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self.check_file_existence(files)

        for fi, fo in super().io_files(files, ext="csv"):
            writer = None
            with jsonlines.open(fi) as reader, open(
                    fo, mode="w", encoding=self._encoding, newline=""
            ) as f:
                for row in reader:
                    new_rows = self.convert_row(row)
                    if not new_rows:
                        continue
                    if not writer:
                        writer = csv.DictWriter(
                            f,
                            new_rows[0].keys(),
                            quoting=Csv.quote_convert(self._quote),
                            lineterminator=Csv.newline_convert(self._after_nl),
                            escapechar=self._escape_char
                        )
                        writer.writeheader()
                    writer.writerows(new_rows)


class JsonlToCsv(JsonlToCsvBase):
    """
    Transform jsonlines to csv.
    """
    def execute(self, *args):
        super().execute()

    def convert_row(self, row):
        return [row]

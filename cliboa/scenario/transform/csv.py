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

import os

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.transform.file import FileBaseTransform
from cliboa.util.csv import Csv
from cliboa.util.exception import FileNotFound, InvalidParameter


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

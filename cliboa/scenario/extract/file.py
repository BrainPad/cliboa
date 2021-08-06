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
import csv
from glob import glob

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters, IOInput
from cliboa.util.cache import ObjectStore
from cliboa.util.exception import CliboaException, FileNotFound


class FileRead(BaseStep):
    """
    The parent class to read the specified file
    """

    def __init__(self):
        super().__init__()
        self._src_path = None
        self._src_dir = None
        self._src_pattern = None
        self._encoding = "utf-8"

    def src_path(self, src_path):
        self._src_path = src_path

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()


class CsvRead(FileRead):
    """
    Read the specified csv file

    Deprecated.
    """

    LOAD_CHUNK_SIZE = 10000

    def __init__(self):
        super().__init__()
        self._columns = None

    def columns(self, columns):
        self._columns = columns

    def execute(self, *args):
        self._logger.warning("Deprecated. Please do not use this class.")

        input_valid = IOInput(self._io)
        input_valid()

        files = glob(self._src_path)
        if len(files) > 1:
            raise CliboaException("Input file must be only one.")

        if len(files) == 0:
            raise FileNotFound("The specified csv file not found.")

        with open(files[0], "r", encoding=self._encoding) as f:

            # save per one column
            if self._columns:
                reader = csv.DictReader(f, delimiter=",")
                for row in reader:
                    # extract only the specified columns
                    row_dict = {}
                    for c in self._columns:
                        if not row.get(c):
                            continue
                        row_dict[c] = row.get(c)
                    self._s.save(row_dict)
            else:
                reader = csv.reader(f)
                header = next(reader, None)
                for row in reader:
                    row_dict = dict(zip(header, row))
                    self._s.save(row_dict)

        # cache downloaded file names
        ObjectStore.put(self._step, files)

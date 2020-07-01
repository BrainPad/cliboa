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
import ast
import csv

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import IOOutput


class FileWrite(BaseStep):
    """
    Load file to server
    """

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_path = None
        self._encoding = "utf-8"
        self._mode = "a"

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def encoding(self, encoding):
        self._encoding = encoding

    def mode(self, mode):
        self._mode = mode


class CsvWrite(FileWrite):
    """
    Write data fetched by io: inptu to csv file
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        super().execute()
        input_valid = IOOutput(self._io)
        input_valid()

        with open(self._s.cache_file, "r", encoding=self._encoding) as i, open(
            self._dest_path, self._mode, encoding=self._encoding
        ) as o:
            writer = csv.writer(o, quoting=csv.QUOTE_ALL)

            # write csv header
            head_dict = ast.literal_eval(i.readline())
            header = []
            for k in head_dict.keys():
                header.append(k)
            writer.writerow(header)

            # write as csv per one line
            i.seek(0)
            for l_str in i:
                l_dict = ast.literal_eval(l_str)
                contents = []
                for k, v in l_dict.items():
                    contents.append(v)
                writer.writerow(contents)
        self._s.remove()

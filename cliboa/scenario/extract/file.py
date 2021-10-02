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
from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters


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
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

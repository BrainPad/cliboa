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
import tempfile
from abc import abstractmethod
from typing import List, Optional

from cliboa.adapter.file import File
from cliboa.util.base import _BaseObject
from cliboa.util.cache import StepArgument
from cliboa.util.exception import FileNotFound, InvalidParameter


class BaseStep(_BaseObject):
    """
    Base class of all the step classes
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._step = None
        self._symbol = None

    def step(self, step):
        self._step = step

    def symbol(self, symbol):
        self._symbol = symbol

    @abstractmethod
    def execute(self, *args, **kwargs) -> Optional[int]:
        pass

    def get_target_files(self, src_dir, src_pattern) -> List[str]:
        """
        Search files either with regular expression
        """
        return File().get_target_files(src_dir, src_pattern)

    def get_step_argument(self, name):
        """
        Returns an argument from scenario.yaml definitions
        """
        sa = StepArgument.get(self._symbol)
        if sa:
            return sa.get(name)

    def _property_path_reader(self, src, encoding="utf-8"):
        """
        Returns an resource contents from the path if src starts with "path:",
        returns src if not
        """
        self._logger.warning("DeprecationWarning: Will be removed in the near future")
        if src[:5].upper() == "PATH:":
            fpath = src[5:]
            if os.path.exists(fpath) is False:
                raise FileNotFound(src)
            with open(fpath, mode="r", encoding=encoding) as f:
                return f.read()
        return src

    def _source_path_reader(self, src, encoding="utf-8"):
        """
        Returns an path to temporary file contains content specify in src if src is dict,
        returns src if not
        """
        if src is None:
            return src
        if isinstance(src, dict) and "content" in src:
            with tempfile.NamedTemporaryFile(mode="w", encoding=encoding, delete=False) as fp:
                fp.write(src["content"])
                return fp.name
        elif isinstance(src, dict) and "file" in src:
            if os.path.exists(src["file"]) is False:
                raise FileNotFound(src)
            return src["file"]
        else:
            raise InvalidParameter("The parameter is invalid.")

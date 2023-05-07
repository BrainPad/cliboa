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
import configparser
import os
import re
import tempfile
from abc import abstractmethod

from cliboa.conf import env
from cliboa.util.cache import StepArgument
from cliboa.util.exception import FileNotFound, InvalidParameter
from cliboa.util.file import File


class BaseStep(object):
    """
    Base class of all the step classes
    """

    def __init__(self):
        self._step = None
        self._symbol = None
        self._parallel = None
        self._logger = None
        self._listeners = []

    def step(self, step):
        self._step = step

    def symbol(self, symbol):
        self._symbol = symbol

    def parallel(self, parallel):
        self._parallel = parallel

    def logger(self, logger):
        self._logger = logger

    def listeners(self, listeners):
        self._listeners = listeners

    def trigger(self, *args):
        mask = None
        path = os.path.join(env.BASE_DIR, "conf", "cliboa.ini")
        if os.path.exists(path):
            try:
                conf = configparser.ConfigParser()
                conf.read(path, encoding="utf-8")
                mask = conf.get("logging", "mask")
                pattern = re.compile(mask)
            except Exception as e:
                self._logger.warning(e)

        for k, v in self.__dict__.items():
            if mask is not None and pattern.search(k):
                self._logger.info("%s : ****" % k)
            else:
                self._logger.info("%s : %s" % (k, v))
        try:
            for listener in self._listeners:
                listener.before_step(self)

            ret = self.execute(args)

            for listener in self._listeners:
                listener.after_step(self)

            return ret

        except Exception as e:
            for listener in self._listeners:
                listener.error_step(self, e)

            return self._exception_dispatcher(e)
        finally:
            for listener in self._listeners:
                listener.after_completion(self)

    @abstractmethod
    def execute(self, *args):
        pass

    def get_target_files(self, src_dir, src_pattern):
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

    def _exception_dispatcher(self, e):
        """
        Handle and dispath CliboaExceptions
        """
        # TODO Currently not doing anything
        raise e

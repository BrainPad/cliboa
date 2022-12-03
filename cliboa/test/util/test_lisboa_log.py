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
import logging
import os
import sys

from cliboa.interface import CommandArgumentParser
from cliboa.conf import env
from cliboa.util.lisboa_log import LisboaLog


class TestLisboaLog(object):
    def setup_method(self, method):
        CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("hoge")
        sys.argv.append("hoge")
        self._log_file = os.path.join(env.BASE_DIR, "logs", "app.log")

    def test_get_logger(self):
        logger = LisboaLog().get_logger(__name__)
        assert type(logger) == logging.Logger

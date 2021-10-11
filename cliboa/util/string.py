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
import random
import string

from cliboa.util.lisboa_log import LisboaLog


class StringUtil(object):
    def __init__(self):
        self._logger = LisboaLog.get_logger(__name__)

    def random_str(self, length):
        """
        Generate a random string

        Args:
            length: Length of characters

        Returns:
            str: New randomly generated string
        """
        return "".join([random.choice(string.ascii_letters + string.digits) for i in range(length)])

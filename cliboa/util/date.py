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
import dateutil.parser as parser

from cliboa.util.lisboa_log import LisboaLog


class DateUtil(object):
    def __init__(self):
        self._logger = LisboaLog.get_logger(__name__)

    def convert_date_format(self, str, format):
        """
        Convert date string to other format date string.
        The converter parser is based on the specifications below.

        https://dateutil.readthedocs.io/en/stable/parser.html

        Args:
            str: string before convert
            format: formatter

        Returns:
            str: new format date string or None if str is None
        """
        if not str:
            return None

        p = parser.parse(str)
        return p.strftime(format)

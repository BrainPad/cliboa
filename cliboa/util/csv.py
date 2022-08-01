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
import csv

from cliboa.util.exception import CliboaException


class Csv(object):
    @staticmethod
    def quote_convert(string):
        """
        Convert string to csv quoting type.
        """
        convert_type = {
            "QUOTE_ALL": csv.QUOTE_ALL,
            "QUOTE_MINIMAL": csv.QUOTE_MINIMAL,
            "QUOTE_NONNUMERIC": csv.QUOTE_NONNUMERIC,
            "QUOTE_NONE": csv.QUOTE_NONE,
        }

        if string.upper() not in convert_type:
            raise CliboaException(
                "Unknown string. One of the followings are allowed [QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONNUMERIC, QUOTE_NONE]"  # noqa
            )
        return convert_type.get(string.upper())

    @staticmethod
    def delimiter_convert(string):
        """
        Convert string to csv delimiter.
        """
        if string.upper() == "CSV":
            return ","
        elif string.upper() == "TSV":
            return "\t"
        else:
            raise CliboaException("Unknown string. One of the followings are allowed [CSV, TSV]")

    @staticmethod
    def newline_convert(string):
        convert_type = {
            "LF": "\n",
            "CR": "\r",
            "CRLF": "\r\n",
        }

        if string.upper() not in convert_type:
            raise CliboaException(
                "Unknown string. One of the followings are allowed [LF, CR, CRLF]"
            )
        return convert_type.get(string.upper())

    @staticmethod
    def get_column_names(src, enc="utf-8"):
        """
        Returns csv column names
        """
        columns = []
        with open(src, "r", encoding=enc) as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
        return columns

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
"""
Exception classes of cliboa
"""


class CliboaException(Exception):
    # cliboa unique exception
    pass


class DirStructureInvalid(CliboaException):
    # Exception when directory structure invalid
    pass


class FileNotFound(CliboaException):
    # Exception when specified file not found
    pass


class InvalidFileCount(CliboaException):
    # Exception when many files exist more than expacted
    pass


class InvalidParameter(CliboaException):
    # Exception when parameters are invalid
    pass


class InvalidCount(CliboaException):
    # Exception when invalid count was specified
    pass


class InvalidFormat(CliboaException):
    # Exception when invalid format was specified
    pass


class ScenarioFileInvalid(CliboaException):
    # Exception when scenario.yml(json) is invalid
    pass


class StepExecutionFailed(CliboaException):
    # Exception when step execution was failed
    pass


class SqliteInvalid(CliboaException):
    # Exception when specified file not found
    pass


class DatabaseException(CliboaException):
    # Database error
    pass

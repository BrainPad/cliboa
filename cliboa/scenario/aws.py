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
from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters


class BaseAws(BaseStep):
    """
    Base class of AWS related classes
    """

    def __init__(self):
        super().__init__()
        self._region = None
        self._access_key = None
        self._secret_key = None
        self._profile = None

    def region(self, region):
        self._region = region

    def access_key(self, access_key):
        self._access_key = access_key

    def secret_key(self, secret_key):
        self._secret_key = secret_key

    def profile(self, profile):
        self._profile = profile

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._region])
        valid()


class BaseS3(BaseAws):
    """
    Base class of S3 related classes
    """

    def __init__(self):
        super().__init__()
        self._bucket = None

    def bucket(self, bucket):
        self._bucket = bucket

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._bucket])
        valid()

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


class BaseAzure(BaseStep):
    """
    Base class of Azure related classes
    """

    def __init__(self):
        super().__init__()
        self._account_url = None
        self._account_access_key = None
        self._connection_string = None

    def account_url(self, account_url):
        self._account_url = account_url

    def account_access_key(self, account_access_key):
        self._account_access_key = account_access_key

    def connection_string(self, connection_string):
        self._connection_string = connection_string


class BaseAzureBlob(BaseAzure):
    """
    Base class of Azure Blob Storage related classes
    """

    def __init__(self):
        super().__init__()
        self._container_name = None

    def container_name(self, container_name):
        self._container_name = container_name

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._container_name])
        valid()

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

from pydantic import BaseModel

from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated_args


class BaseAzure(BaseStep):
    """
    Base class of Azure related classes
    """

    class Arguments(BaseModel):
        account_url: str | None = None
        account_access_key: str | None = None
        connection_string: str | None = None

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _account_url(self):
        return self.args.account_url

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _account_access_key(self):
        return self.args.account_access_key

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _connection_string(self):
        return self.args.connection_string


class BaseAzureBlob(BaseAzure):
    """
    Base class of Azure Blob Storage related classes
    """

    class Arguments(BaseAzure.Arguments):
        container_name: str

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _container_name(self):
        return self.args.container_name

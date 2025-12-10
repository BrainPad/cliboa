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


class BaseGcp(BaseStep):
    """
    Base class of Gcp usage.
    """

    class Arguments(BaseModel):
        project_id: str
        credentials: str

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _project_id(self):
        return self.args.project_id

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _credentials(self):
        return self.args.credentials

    def get_credentials(self):
        return self.args.credentials


class BaseBigQuery(BaseGcp):
    """
    Base class of BigQuery use.

    """

    class Arguments(BaseGcp.Arguments):
        dataset: str
        tblname: str
        location: str

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dataset(self):
        return self.args.dataset

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _tblname(self):
        return self.args.tblname

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _location(self):
        return self.args.location


class BaseGcs(BaseGcp):
    """
    Base class of Gcs use.

    """

    class Arguments(BaseGcp.Arguments):
        bucket: str

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _bucket(self):
        return self.args.bucket


class BaseFirestore(BaseGcp):
    """
    Base class of Firebase use.

    """

    class Arguments(BaseGcp.Arguments):
        collection: str
        document: str | None = None

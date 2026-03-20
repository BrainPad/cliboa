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


class BaseAws(BaseStep):
    """
    Base class of AWS related classes
    """

    class Arguments(BaseModel):
        region: str
        access_key: str | None = None
        secret_key: str | None = None
        profile: str | None = None

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _region(self):
        return self.args.region

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _access_key(self):
        return self.args.access_key

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _secret_key(self):
        return self.args.secret_key

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _profile(self):
        return self.args.profile


class BaseS3(BaseAws):
    """
    Base class of S3 related classes
    """

    class Arguments(BaseAws.Arguments):
        region: str | None = None
        bucket: str
        role_arn: str | None = None
        external_id: str | None = None

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _bucket(self):
        return self.args.bucket

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _role_arn(self):
        return self.args.role_arn

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _external_id(self):
        return self.args.external_id

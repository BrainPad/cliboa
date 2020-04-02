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
import boto3
from boto3.session import Session

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

    def region(self, region):
        self._region = region

    def access_key(self, access_key):
        self._access_key = access_key

    def secret_key(self, secret_key):
        self._secret_key = secret_key

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._region])
        valid()

    def _client(self, service_name):
        if self._access_key and self._secret_key:
            return boto3.client(
                service_name=service_name,
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                region_name=self._region,
            )
        else:
            return boto3.client(service_name)

    def _resource(self, service_name):
        if self._access_key and self._secret_key:
            session = Session(
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                region_name=self._region,
            )
            return session.resource(service_name)
        else:
            return boto3.resource("s3")


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

    def _s3_client(self):
        return self._client("s3")

    def _s3_resource(self):
        return self._resource("s3")

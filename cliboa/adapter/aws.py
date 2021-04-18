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
import boto3
from boto3.session import Session


class S3Adapter(object):
    """
    Adapter of AWS S3
    """

    def __init__(self, access_key: str = None, secret_key: str = None):
        self._access_key = access_key
        self._secret_key = secret_key

    def get_client(self):
        """
        Get s3 client
        """
        return (
            boto3.client(
                "s3",
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            )
            if self._access_key and self._secret_key
            else boto3.client("s3")
        )

    def get_resource(self):
        """
        Get s3 resource
        """
        if self._access_key and self._secret_key:
            session = Session(
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            )
            return session.resource("s3")
        else:
            return boto3.resource("s3")

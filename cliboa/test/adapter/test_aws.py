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

from cliboa.adapter.aws import S3Adapter
from cliboa.test import BaseCliboaTest


class TestS3Adapter(BaseCliboaTest):
    def test_get_client_with_keys(self):
        c = S3Adapter("spam", "spam").get_client()
        self.assertIsNotNone(c)

    def test_get_client_with_no_keys(self):
        c = S3Adapter().get_client()
        self.assertIsNotNone(c)

    def test_get_resource_with_keys(self):
        r = S3Adapter("spam", "spam").get_resource()
        self.assertIsNotNone(r)

    def test_get_resource_with_no_keys(self):
        r = S3Adapter().get_resource()
        assert r == boto3.resource("s3")

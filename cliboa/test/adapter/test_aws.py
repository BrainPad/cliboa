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
from unittest.mock import patch

import boto3

from cliboa.adapter.aws import S3Adapter
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import InvalidParameter


class TestS3Adapter(BaseCliboaTest):

    def _setup_mock_credentials(self, mock_boto3_client):
        """Helper method to setup mock credentials"""
        mock_sts_client = mock_boto3_client.return_value
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "test_access_key",
                "SecretAccessKey": "test_secret_key",
                "SessionToken": "test_session_token",
            }
        }

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

    @patch("boto3.client")
    def test_get_client_with_role_arn(self, mock_boto3_client):
        """Test get_client with cross-account IAM role"""
        self._setup_mock_credentials(mock_boto3_client)
        client = S3Adapter(role_arn="arn:aws:iam::123456789012:role/TestRole").get_client()
        self.assertIsNotNone(client)

    @patch("boto3.client")
    def test_get_resource_with_role_arn(self, mock_boto3_client):
        """Test get_resource with cross-account IAM role"""
        self._setup_mock_credentials(mock_boto3_client)
        resource = S3Adapter(role_arn="arn:aws:iam::123456789012:role/TestRole").get_resource()
        self.assertIsNotNone(resource)

    @patch("boto3.client")
    def test_get_client_with_role_arn_and_external_id(self, mock_boto3_client):
        """Test get_client with cross-account IAM role and external ID"""
        self._setup_mock_credentials(mock_boto3_client)
        client = S3Adapter(
            role_arn="arn:aws:iam::123456789012:role/TestRole", external_id="test-external-id"
        ).get_client()
        self.assertIsNotNone(client)

    def test_validation_role_arn_with_access_key(self):
        """Test validation: role_arn cannot be specified with access_key"""
        with self.assertRaises(InvalidParameter):
            S3Adapter(role_arn="arn:aws:iam::123456789012:role/TestRole", access_key="test_key")

    def test_validation_role_arn_with_profile(self):
        """Test validation: role_arn cannot be specified with profile"""
        with self.assertRaises(InvalidParameter):
            S3Adapter(role_arn="arn:aws:iam::123456789012:role/TestRole", profile="test_profile")

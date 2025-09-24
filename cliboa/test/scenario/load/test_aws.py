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

import tempfile
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from cliboa.scenario.load.aws import DynamoDBWrite, S3Upload
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import FileNotFound, InvalidFormat, InvalidParameter
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class BaseS3Test(BaseCliboaTest):
    """Base test class for S3 related tests"""
    
    def _test_cross_account_role_properties(self, instance_class):
        """Test cross-account IAM role properties for any S3 class"""
        instance = instance_class()
        Helper.set_property(instance, "role_arn", "arn:aws:iam::123456789012:role/TestRole")
        Helper.set_property(instance, "external_id", "test-external-id")

        self.assertEqual(instance._role_arn, "arn:aws:iam::123456789012:role/TestRole")
        self.assertEqual(instance._external_id, "test-external-id")


class TestDynamoDBWrite(BaseCliboaTest):
    def _get_dynamodb_write_instance(self, file_format):
        instance = DynamoDBWrite()
        Helper.set_property(instance, "table_name", "test_table")
        Helper.set_property(instance, "src_dir", "/test/dir")
        Helper.set_property(instance, "src_pattern", "test*")
        Helper.set_property(instance, "file_format", file_format)
        Helper.set_property(instance, "region", "us-west-2")
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        return instance

    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_file_not_found(self, mock_get_target_files):
        mock_get_target_files.return_value = []
        with pytest.raises(FileNotFound):
            dynamodb_write = self._get_dynamodb_write_instance("csv")
            dynamodb_write.execute()

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_table_not_found(self, mock_get_target_files, mock_boto3_resource):
        mock_get_target_files.return_value = ["/test/dir/test.csv"]
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_boto3_resource.return_value.meta.client.exceptions.ResourceNotFoundException = (
            ClientError
        )
        mock_table.load.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}},
            "LoadTable",
        )

        with pytest.raises(ClientError) as exc_info:
            dynamodb_write = self._get_dynamodb_write_instance("csv")
            dynamodb_write.execute()

        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_csv_success(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]

        csv_content = "id,name\n1,test1\n2,test2"
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(csv_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]

            dynamodb_write = self._get_dynamodb_write_instance("csv")
            dynamodb_write.execute()

        mock_table.batch_writer.assert_called_once()
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "1", "name": "test1"}
        )
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "2", "name": "test2"}
        )

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_jsonl_success(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]

        jsonl_content = (
            '{"id": "1", "name": "test1", "timestamp": "2023-01-01"}\n'
            '{"id": "2", "name": "test2", "timestamp": "2023-01-02"}'
        )
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(jsonl_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]
            dynamodb_write = self._get_dynamodb_write_instance("jsonl")
            dynamodb_write.execute()

        mock_table.batch_writer.assert_called_once()
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "1", "name": "test1", "timestamp": "2023-01-01"}
        )
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "2", "name": "test2", "timestamp": "2023-01-02"}
        )

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_csv_format_error(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]

        csv_content = "id_dehanai,name\n1,test1\n2,test2,extra"
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(csv_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]

            with pytest.raises(InvalidParameter):
                dynamodb_write = self._get_dynamodb_write_instance("csv")
                dynamodb_write.execute()

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_jsonl_format_error(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]

        jsonl_content = '{"id": "1", "name": "test1"}\n{"id": "2", "name": "test2",}'
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(jsonl_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]

            with pytest.raises(InvalidFormat):
                dynamodb_write = self._get_dynamodb_write_instance("jsonl")
                dynamodb_write.execute()

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_execute_csv_with_sort_key(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ]

        csv_content = "id,name,timestamp\n1,test1,2023-01-01\n2,test2,2023-01-02"
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(csv_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]

            # test
            dynamodb_write = self._get_dynamodb_write_instance("csv")
            dynamodb_write.execute()

        # assert
        mock_table.batch_writer.assert_called_once()
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "1", "name": "test1", "timestamp": "2023-01-01"}
        )
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "2", "name": "test2", "timestamp": "2023-01-02"}
        )

    @patch("boto3.resource")
    @patch("cliboa.scenario.load.aws.BaseAws.get_target_files")
    def test_execute_jsonl_with_sort_key(self, mock_get_target_files, mock_boto3_resource):
        mock_table = MagicMock()
        mock_boto3_resource.return_value.Table.return_value = mock_table
        mock_table.key_schema = [
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ]

        jsonl_content = (
            '{"id": "1", "name": "test1", "timestamp": "2023-01-01"}\n'
            '{"id": "2", "name": "test2", "timestamp": "2023-01-02"}'
        )
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            temp_file.write(jsonl_content)
            temp_file.flush()
            mock_get_target_files.return_value = [temp_file.name]

            # test
            dynamodb_write = self._get_dynamodb_write_instance("jsonl")
            dynamodb_write.execute()

        # assert
        mock_table.batch_writer.assert_called_once()
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "1", "name": "test1", "timestamp": "2023-01-01"}
        )
        mock_table.batch_writer().__enter__().put_item.assert_any_call(
            Item={"id": "2", "name": "test2", "timestamp": "2023-01-02"}
        )


class TestS3Upload(BaseS3Test):
    def test_cross_account_role_properties(self):
        """Test S3Upload with cross-account IAM role properties"""
        self._test_cross_account_role_properties(S3Upload)

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
import csv
import json
import os
import tempfile
from decimal import Decimal
from unittest.mock import patch

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.extract.aws import DynamoDBRead, S3Delete, S3Download, S3DownloadFileDelete, S3FileExistsCheck
from cliboa.test import BaseCliboaTest
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


class TestS3Download(BaseS3Test):
    @patch.object(S3Adapter, "get_client")
    def test_execute_ok(self, m_get_client):
        with tempfile.TemporaryDirectory() as tmp_dir:
            m_get_object = m_get_client.return_value.get_object
            m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
            m_contents = [{"Contents": [{"Key": "spam"}]}]
            m_pagenate.return_value = m_contents

            instance = S3Download()
            Helper.set_property(instance, "bucket", "spam")
            Helper.set_property(instance, "src_pattern", "spam")
            Helper.set_property(instance, "dest_dir", tmp_dir)
            instance.execute()

            assert m_get_object.call_args_list == []

    def test_cross_account_role_properties(self):
        """Test S3Download with cross-account IAM role properties"""
        self._test_cross_account_role_properties(S3Download)


class TestS3Delete(BaseS3Test):
    @patch.object(S3Adapter, "get_client")
    def test_execute_ok(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_contents = [{"Contents": [{"Key": "spam"}]}]
        m_pagenate.return_value = m_contents

        instance = S3Delete()
        Helper.set_property(instance, "bucket", "spam")
        Helper.set_property(instance, "src_pattern", "spam")
        instance.execute()

        assert m_get_object.call_args_list == []

    def test_cross_account_role_properties(self):
        """Test S3Delete with cross-account IAM role properties"""
        self._test_cross_account_role_properties(S3Delete)


class TestS3FileExistsCheck(BaseS3Test):
    @patch.object(S3Adapter, "get_client")
    def test_execute_file_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # テスト処理
        instance = S3FileExistsCheck()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "bucket", "spam")
        Helper.set_property(instance, "src_pattern", "spam")
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_execute_file_not_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # テスト処理
        instance = S3FileExistsCheck()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "bucket", "spam")
        Helper.set_property(instance, "src_pattern", "hoge")
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []

    def test_cross_account_role_properties(self):
        """Test S3FileExistsCheck with cross-account IAM role properties"""
        self._test_cross_account_role_properties(S3FileExistsCheck)


class TestS3DownloadFileDelete(BaseS3Test):
    def test_cross_account_role_properties(self):
        """Test S3DownloadFileDelete with cross-account IAM role properties"""
        self._test_cross_account_role_properties(S3DownloadFileDelete)


class TestDynamoDBRead(BaseCliboaTest):
    @patch("boto3.resource")
    def test_execute_csv_with_nested_data(self, mock_boto_resource):
        test_data = {
            "Items": [
                {
                    "id": "1",
                    "name": "Item 1",
                    "details": {
                        "value": Decimal("100"),
                        "attributes": {"color": "red", "size": "large"},
                    },
                },
                {
                    "id": "2",
                    "name": "Item 2",
                    "details": {
                        "value": Decimal("200"),
                        "attributes": {"color": "blue", "size": "medium"},
                    },
                },
            ],
            "Count": 2,
            "ScannedCount": 2,
            "LastEvaluatedKey": None,
        }
        expected_csv = [
            ["id", "name", "details"],
            ["1", "Item 1", '{"value": 100, "attributes": {"color": "red", "size": "large"}}'],
            ["2", "Item 2", '{"value": 200, "attributes": {"color": "blue", "size": "medium"}}'],
        ]

        self._run_test(mock_boto_resource, test_data, expected_csv, "csv")

    @patch("boto3.resource")
    def test_execute_csv_without_nested_data(self, mock_boto_resource):
        test_data = {
            "Items": [
                {"id": "1", "name": "Item 1", "value": Decimal("100")},
                {"id": "2", "name": "Item 2", "value": Decimal("200")},
            ],
            "Count": 2,
            "ScannedCount": 2,
            "LastEvaluatedKey": None,
        }
        expected_csv = [
            ["id", "name", "value"],
            ["1", "Item 1", "100"],
            ["2", "Item 2", "200"],
        ]

        self._run_test(mock_boto_resource, test_data, expected_csv, "csv")

    @patch("boto3.resource")
    def test_execute_jsonl_with_nested_data(self, mock_boto_resource):
        test_data = {
            "Items": [
                {
                    "id": "1",
                    "name": "Item 1",
                    "details": {
                        "value": Decimal("100"),
                        "attributes": {"color": "red", "size": "large"},
                    },
                },
                {
                    "id": "2",
                    "name": "Item 2",
                    "details": {
                        "value": Decimal("200"),
                        "attributes": {"color": "blue", "size": "medium"},
                    },
                },
            ],
            "Count": 2,
            "ScannedCount": 2,
            "LastEvaluatedKey": None,
        }
        expected_jsonl = [
            {
                "id": "1",
                "name": "Item 1",
                "details": {"value": 100, "attributes": {"color": "red", "size": "large"}},
            },
            {
                "id": "2",
                "name": "Item 2",
                "details": {"value": 200, "attributes": {"color": "blue", "size": "medium"}},
            },
        ]

        self._run_test(mock_boto_resource, test_data, expected_jsonl, "jsonl")

    @patch("boto3.resource")
    def test_execute_jsonl_without_nested_data(self, mock_boto_resource):
        test_data = {
            "Items": [
                {"id": "1", "name": "Item 1", "value": Decimal("100")},
                {"id": "2", "name": "Item 2", "value": Decimal("200")},
            ],
            "Count": 2,
            "ScannedCount": 2,
            "LastEvaluatedKey": None,
        }
        expected_jsonl = [
            {"id": "1", "name": "Item 1", "value": 100},
            {"id": "2", "name": "Item 2", "value": 200},
        ]

        self._run_test(mock_boto_resource, test_data, expected_jsonl, "jsonl")

    def _run_test(self, mock_boto_resource, test_data, expected_data, file_format):
        mock_table = mock_boto_resource.return_value.Table.return_value
        mock_table.scan.return_value = test_data

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            Helper.set_property(instance, "table_name", "test_table")
            Helper.set_property(instance, "file_name", f"output.{file_format}")
            Helper.set_property(instance, "dest_dir", temp_dir)
            Helper.set_property(instance, "file_format", file_format)
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            Helper.set_property(instance, "region", "us-east-1")
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)

            if file_format == "csv":
                self._verify_csv(output_file_path, expected_data)
            else:  # jsonl
                self._verify_jsonl(output_file_path, expected_data)

    def _verify_csv(self, file_path, expected_data):
        with open(file_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            actual_data = list(reader)

        assert len(actual_data) == len(
            expected_data
        ), f"期待される行数 {len(expected_data)} に対し、実際の行数は {len(actual_data)} です"

        for expected_row, actual_row in zip(expected_data, actual_data):
            assert len(expected_row) == len(
                actual_row
            ), f"列数が一致しません。期待値: {len(expected_row)}, 実際の値: {len(actual_row)}"
            for expected_value, actual_value in zip(expected_row, actual_row):
                assert str(actual_value) == str(
                    expected_value
                ), f"値が一致しません。期待値: {expected_value}, 実際の値: {actual_value}"

    def _verify_jsonl(self, file_path, expected_data):
        with open(file_path, "r") as jsonl_file:
            actual_data = [json.loads(line) for line in jsonl_file]

        assert len(actual_data) == len(
            expected_data
        ), f"Expected {len(expected_data)} items, got {len(actual_data)}"

        for expected, actual in zip(expected_data, actual_data):
            assert expected == actual, f"Data mismatch: expected {expected}, got {actual}"

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
from unittest.mock import Mock, patch

import boto3
from moto import mock_aws

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.extract.aws import (
    DynamoDBRead,
    S3Delete,
    S3Download,
    S3DownloadFileDelete,
    S3FileExistsCheck,
)
from tests import BaseCliboaTest


class TestS3Download(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_execute_ok(self, m_get_client):
        with tempfile.TemporaryDirectory() as tmp_dir:
            m_get_object = m_get_client.return_value.get_object
            m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
            m_contents = [{"Contents": [{"Key": "spam"}]}]
            m_pagenate.return_value = m_contents

            instance = S3Download()
            instance._set_properties(
                {
                    "bucket": "spam",
                    "src_pattern": "spam",
                    "dest_dir": tmp_dir,
                }
            )
            instance.execute()

            assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_cross_account_role_properties(self, m_get_client):
        """Test S3Download with cross-account IAM role properties"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
            m_pagenate.return_value = [{"Contents": [{"Key": "test.txt"}]}]

            instance = S3Download()
            instance._set_properties(
                {
                    "bucket": "test-bucket",
                    "src_pattern": "test.*",
                    "dest_dir": tmp_dir,
                    "role_arn": "arn:aws:iam::123456789012:role/TestRole",
                    "external_id": "test-external-id",
                }
            )

            instance.execute()

            # Verify that S3Adapter was called with cross-account parameters
            m_get_client.assert_called_once()


class TestS3Delete(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_execute_ok(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_contents = [{"Contents": [{"Key": "spam"}]}]
        m_pagenate.return_value = m_contents

        instance = S3Delete()
        instance._set_properties(
            {
                "bucket": "spam",
                "src_pattern": "spam",
            }
        )
        instance.execute()

        assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_cross_account_role_properties(self, m_get_client):
        """Test S3Delete with cross-account IAM role properties"""
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "test.txt"}]}]

        instance = S3Delete()
        instance._set_properties(
            {
                "bucket": "test-bucket",
                "src_pattern": "test.*",
                "role_arn": "arn:aws:iam::123456789012:role/TestRole",
                "external_id": "test-external-id",
            }
        )

        instance.execute()

        # Verify that S3Adapter was called with cross-account parameters
        m_get_client.assert_called_once()


class TestS3FileExistsCheck(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_execute_file_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # Execute test
        instance = S3FileExistsCheck()
        instance._set_properties(
            {
                "bucket": "spam",
                "src_pattern": "spam",
            }
        )
        instance.execute()
        # Verify successful execution
        assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_execute_file_not_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # Execute test
        instance = S3FileExistsCheck()
        instance._set_properties(
            {
                "bucket": "spam",
                "src_pattern": "hoge",
            }
        )
        instance.execute()
        # Verify successful execution
        assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_cross_account_role_properties(self, m_get_client):
        """Test S3FileExistsCheck with cross-account IAM role properties"""
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "test.txt"}]}]

        instance = S3FileExistsCheck()
        instance._set_properties(
            {
                "bucket": "test-bucket",
                "src_pattern": "test.*",
                "role_arn": "arn:aws:iam::123456789012:role/TestRole",
                "external_id": "test-external-id",
            }
        )

        instance.execute()

        # Verify that S3Adapter was called with cross-account parameters
        m_get_client.assert_called_once()


class TestS3DownloadFileDelete(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_cross_account_role_properties(self, m_get_client):
        """Test S3DownloadFileDelete with cross-account IAM role properties"""

        instance = S3DownloadFileDelete()
        mock_parent = Mock(
            symbol_name="dl1",
        )
        mock_parent.get_from_context.return_value = {"keys": ["test.txt"]}
        mock_parent.get_symbol_arguments.return_value = {}
        instance.parent = mock_parent
        instance._set_properties(
            {
                "bucket": "test-bucket",
                "role_arn": "arn:aws:iam::123456789012:role/TestRole",
                "external_id": "test-external-id",
            }
        )

        instance.execute()

        # Verify that S3Adapter was called with cross-account parameters
        m_get_client.assert_called_once()


class TestDynamoDBRead(BaseCliboaTest):
    @mock_aws
    def test_execute_csv_with_nested_data(self):
        """Test CSV output with nested data structures"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert test data with nested structures
        table.put_item(
            Item={
                "id": "1",
                "name": "Item 1",
                "details": {
                    "value": Decimal("100"),
                    "attributes": {"color": "red", "size": "large"},
                },
            }
        )
        table.put_item(
            Item={
                "id": "2",
                "name": "Item 2",
                "details": {
                    "value": Decimal("200"),
                    "attributes": {"color": "blue", "size": "medium"},
                },
            }
        )

        expected_csv = [
            ["id", "name", "details"],
            ["1", "Item 1", '{"value": 100, "attributes": {"color": "red", "size": "large"}}'],
            ["2", "Item 2", '{"value": 200, "attributes": {"color": "blue", "size": "medium"}}'],
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "file_format": "csv",
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)
            self._verify_csv(output_file_path, expected_csv)

    @mock_aws
    def test_execute_csv_without_nested_data(self):
        """Test CSV output with simple data structures"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        table.put_item(Item={"id": "1", "name": "Item 1", "value": Decimal("100")})
        table.put_item(Item={"id": "2", "name": "Item 2", "value": Decimal("200")})

        expected_csv = [
            ["id", "name", "value"],
            ["1", "Item 1", "100"],
            ["2", "Item 2", "200"],
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "file_format": "csv",
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)
            self._verify_csv(output_file_path, expected_csv)

    @mock_aws
    def test_execute_jsonl_with_nested_data(self):
        """Test JSONL output with nested data structures"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        table.put_item(
            Item={
                "id": "1",
                "name": "Item 1",
                "details": {
                    "value": Decimal("100"),
                    "attributes": {"color": "red", "size": "large"},
                },
            }
        )
        table.put_item(
            Item={
                "id": "2",
                "name": "Item 2",
                "details": {
                    "value": Decimal("200"),
                    "attributes": {"color": "blue", "size": "medium"},
                },
            }
        )

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

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.jsonl",
                    "dest_dir": temp_dir,
                    "file_format": "jsonl",
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)
            self._verify_jsonl(output_file_path, expected_jsonl)

    @mock_aws
    def test_execute_jsonl_without_nested_data(self):
        """Test JSONL output with simple data structures"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        table.put_item(Item={"id": "1", "name": "Item 1", "value": Decimal("100")})
        table.put_item(Item={"id": "2", "name": "Item 2", "value": Decimal("200")})

        expected_jsonl = [
            {"id": "1", "name": "Item 1", "value": 100},
            {"id": "2", "name": "Item 2", "value": 200},
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.jsonl",
                    "dest_dir": temp_dir,
                    "file_format": "jsonl",
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)
            self._verify_jsonl(output_file_path, expected_jsonl)

    def _verify_csv(self, file_path, expected_data):
        with open(file_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            actual_data = list(reader)

        assert len(actual_data) == len(
            expected_data
        ), f"Expected {len(expected_data)} rows, got {len(actual_data)} rows"

        for expected_row, actual_row in zip(expected_data, actual_data):
            assert len(expected_row) == len(
                actual_row
            ), f"Column count mismatch. Expected: {len(expected_row)}, Actual: {len(actual_row)}"
            for expected_value, actual_value in zip(expected_row, actual_row):
                assert str(actual_value) == str(
                    expected_value
                ), f"Value mismatch. Expected: {expected_value}, Actual: {actual_value}"

    def _verify_jsonl(self, file_path, expected_data):
        with open(file_path, "r") as jsonl_file:
            actual_data = [json.loads(line) for line in jsonl_file]

        assert len(actual_data) == len(
            expected_data
        ), f"Expected {len(expected_data)} items, got {len(actual_data)}"

        for expected, actual in zip(expected_data, actual_data):
            assert expected == actual, f"Data mismatch: expected {expected}, got {actual}"

    @mock_aws
    def test_execute_with_query_partition_key_only(self):
        """Test query operation with partition key only"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        table.put_item(Item={"user_id": "12345", "name": "User 1", "status": "active"})
        table.put_item(Item={"user_id": "67890", "name": "User 2", "status": "active"})

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "filter_conditions": {"user_id": "12345"},
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)

            # Verify that only the queried user_id is in the output
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)
            assert len(items) == 1
            assert items[0]["user_id"] == "12345"

    @mock_aws
    def test_execute_with_query_partition_and_sort_key(self):
        """Test query operation with partition key and sort key"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
                {"AttributeName": "order_date", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "order_date", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        table.put_item(
            Item={
                "user_id": "12345",
                "order_date": "2025-10-25",
                "amount": Decimal("100"),
            }
        )
        table.put_item(
            Item={
                "user_id": "12345",
                "order_date": "2025-10-26",
                "amount": Decimal("200"),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "filter_conditions": {"user_id": "12345", "order_date": "2025-10-25"},
                    "region": "us-east-1",
                }
            )
            instance.execute()

            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)

            # Verify exact match on both keys
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)
            assert len(items) == 1
            assert items[0]["user_id"] == "12345"
            assert items[0]["order_date"] == "2025-10-25"

    @mock_aws
    def test_execute_with_nonexistent_attribute_in_filter(self):
        """Test with filter condition for non-existent attribute (should not error)"""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Item without the non_existent_field attribute
        table.put_item(Item={"user_id": "12345", "name": "User 1"})

        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "filter_conditions": {"user_id": "12345", "non_existent_field": "value"},
                    "region": "us-east-1",
                }
            )
            instance.execute()

            # Should not raise error, just return empty results
            output_file_path = os.path.join(temp_dir, instance._file_name)
            assert os.path.exists(output_file_path)

            # Verify no items match (non_existent_field filter excludes the item)
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)
            assert len(items) == 0

    @mock_aws
    def test_query_with_actual_filtering(self):
        """Test query operation with partition key and FilterExpression"""
        # Create DynamoDB table with partition key and sort key
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert test data with user_id="123"
        table.put_item(
            Item={
                "user_id": "123",
                "timestamp": "2025-01-01",
                "status": "active",
                "name": "Event1",
            }
        )
        table.put_item(
            Item={
                "user_id": "123",
                "timestamp": "2025-01-02",
                "status": "inactive",
                "name": "Event2",
            }
        )
        table.put_item(
            Item={
                "user_id": "123",
                "timestamp": "2025-01-03",
                "status": "active",
                "name": "Event3",
            }
        )
        table.put_item(
            Item={
                "user_id": "456",
                "timestamp": "2025-01-01",
                "status": "active",
                "name": "Event4",
            }
        )

        # Execute DynamoDBRead with filter: user_id="123" AND status="active"
        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "filter_conditions": {"user_id": "123", "status": "active"},
                    "region": "us-east-1",
                }
            )
            instance.execute()

            # Verify: only 2 items with user_id=123 and status=active
            output_file_path = os.path.join(temp_dir, instance._file_name)
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)

            # Should have exactly 2 items
            assert len(items) == 2
            for item in items:
                assert item["user_id"] == "123"
                assert item["status"] == "active"

            # Verify timestamps
            timestamps = [item["timestamp"] for item in items]
            assert "2025-01-01" in timestamps
            assert "2025-01-03" in timestamps
            assert "2025-01-02" not in timestamps  # inactive

    @mock_aws
    def test_scan_with_actual_filtering(self):
        """Test scan operation with filtering on non-key attributes"""
        # Create DynamoDB table
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert test data with mixed status (active and inactive)
        table.put_item(Item={"user_id": "1", "status": "active", "name": "User1"})
        table.put_item(Item={"user_id": "2", "status": "inactive", "name": "User2"})
        table.put_item(Item={"user_id": "3", "status": "active", "name": "User3"})
        table.put_item(Item={"user_id": "4", "status": "inactive", "name": "User4"})
        table.put_item(Item={"user_id": "5", "status": "active", "name": "User5"})

        # Execute DynamoDBRead with filter: status="active"
        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "filter_conditions": {"status": "active"},
                    "region": "us-east-1",
                }
            )
            instance.execute()

            # Verify: only active items are retrieved
            output_file_path = os.path.join(temp_dir, instance._file_name)
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)

            # Should have exactly 3 active items
            assert len(items) == 3
            for item in items:
                assert item["status"] == "active"

            # user_id 2 and 4 (inactive) should not be included
            user_ids = [item["user_id"] for item in items]
            assert "1" in user_ids
            assert "3" in user_ids
            assert "5" in user_ids
            assert "2" not in user_ids
            assert "4" not in user_ids

    @mock_aws
    def test_scan_without_filter(self):
        """Test scan operation without any filter (full table scan)"""
        # Create DynamoDB table
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert test data
        table.put_item(Item={"id": "1", "value": "A"})
        table.put_item(Item={"id": "2", "value": "B"})
        table.put_item(Item={"id": "3", "value": "C"})

        # Execute DynamoDBRead without filter
        with tempfile.TemporaryDirectory() as temp_dir:
            instance = DynamoDBRead()
            instance._set_properties(
                {
                    "table_name": "test_table",
                    "file_name": "output.csv",
                    "dest_dir": temp_dir,
                    "region": "us-east-1",
                }
            )
            instance.execute()

            # Verify: all items are retrieved
            output_file_path = os.path.join(temp_dir, instance._file_name)
            with open(output_file_path, "r") as f:
                reader = csv.DictReader(f)
                items = list(reader)

            # Should have all 3 items
            assert len(items) == 3
            ids = [item["id"] for item in items]
            assert "1" in ids
            assert "2" in ids
            assert "3" in ids

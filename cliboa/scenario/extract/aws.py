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
import re
from decimal import Decimal
from typing import Any, Literal

import boto3
from boto3.dynamodb.conditions import Key
from pydantic import model_validator

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.aws import BaseAws, BaseS3
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.constant import StepStatus
from cliboa.util.exception import InvalidParameter


class S3Download(BaseS3):
    """
    Download from S3
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._delimiter = ""
        self._src_pattern = None
        self._dest_dir = "."

    def prefix(self, prefix):
        self._prefix = prefix

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        adapter = S3Adapter(
            self._access_key, self._secret_key, self._profile, self._role_arn, self._external_id
        )
        client = adapter.get_client()

        p = client.get_paginator("list_objects")
        keys = []
        for page in p.paginate(Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix):
            for c in page.get("Contents", []):
                path = c.get("Key")
                filename = os.path.basename(path)
                rec = re.compile(self._src_pattern)
                if not rec.fullmatch(filename):
                    continue
                if self._dest_dir:
                    os.makedirs(self._dest_dir, exist_ok=True)
                dest_path = os.path.join(self._dest_dir, filename)
                client.download_file(self._bucket, path, dest_path)
                keys.append(path)

        # cache
        self.put_to_context({"bucket": self._bucket, "keys": keys})


class S3DownloadFileDelete(BaseS3):
    """
    Delete all downloaded files from S3
    """

    Arguments = None

    def execute(self, *args):
        stored = self.get_from_context()
        bucket = stored.get("bucket")
        keys = stored.get("keys")

        if keys is not None and len(keys) > 0:
            symbol_access_key = self.get_symbol_argument("access_key")
            symbol_secret_key = self.get_symbol_argument("secret_key")
            symbol_profile = self.get_symbol_argument("profile")
            symbol_role_arn = self.get_symbol_argument("role_arn")
            symbol_external_id = self.get_symbol_argument("external_id")

            adapter = S3Adapter(
                symbol_access_key,
                symbol_secret_key,
                symbol_profile,
                symbol_role_arn,
                symbol_external_id,
            )
            client = adapter.get_client()

            for key in keys:
                client.delete_object(Bucket=bucket, Key=key)
                self.logger.info("%s is successfully deleted." % bucket + "/" + key)
        else:
            self.logger.info("No files to delete.")


class S3Delete(BaseS3):
    """
    Delete from S3
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._delimiter = ""
        self._src_pattern = None

    def prefix(self, prefix):
        self._prefix = prefix

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        adapter = S3Adapter(
            self._access_key, self._secret_key, self._profile, self._role_arn, self._external_id
        )
        client = adapter.get_client()

        p = client.get_paginator("list_objects")
        for page in p.paginate(Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix):
            for c in page.get("Contents", []):
                path = c.get("Key")
                filename = os.path.basename(path)
                rec = re.compile(self._src_pattern)
                if rec.fullmatch(filename) is None:
                    continue
                client.delete_object(Bucket=self._bucket, Key=path)


class S3FileExistsCheck(BaseS3):
    """
    File check in S3
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._delimiter = ""
        self._src_pattern = None

    def prefix(self, prefix):
        self._prefix = prefix

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        adapter = S3Adapter(
            self._access_key, self._secret_key, self._profile, self._role_arn, self._external_id
        )

        p = adapter.get_client().get_paginator("list_objects")
        for page in p.paginate(Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix):
            for c in page.get("Contents", []):
                filename = os.path.basename(c.get("Key"))
                rec = re.compile(self._src_pattern)
                if not rec.fullmatch(filename):
                    continue
                # The file exist
                self._logger.info("File was found in S3. After process will be processed")
                return None

        # The file does not exist
        self._logger.info("File not found in S3. After process will not be processed")
        return StepStatus.SUCCESSFUL_TERMINATION


class DynamoDBRead(BaseAws):
    """
    Download data from DynamoDB and save as a CSV or JSONL file
    """

    class Arguments(BaseAws.Arguments):
        table_name: str
        dest_dir: str = "."
        file_name: str
        file_format: Literal["csv", "jsonl"] = "csv"
        partition_key: str | None = None
        partition_value: Any = None
        sort_key: str | None = None
        sort_value: Any = None

        @model_validator(mode="before")
        def check_key_conditions(cls, data: dict) -> dict:
            if not isinstance(data, dict):
                raise ValueError(f"arguments is not dict: {data}")

            partition_key_present = "partition_key" in data
            partition_value_present = "partition_value" in data
            if partition_key_present != partition_value_present:
                raise InvalidParameter(
                    "Both 'partition_key' and 'partition_value' must be specified together."
                )

            sort_key_present = "sort_key" in data
            sort_value_present = "sort_value" in data
            if sort_key_present != sort_value_present:
                raise InvalidParameter(
                    "Both 'sort_key' and 'sort_value' must be specified together."
                )

            if sort_key_present and not partition_key_present:
                raise InvalidParameter(
                    "'sort_key'/'sort_value' require 'partition_key'/'partition_value' "
                    "to also be specified."
                )

            return data

    def execute(self, *args):
        """
        Download items from a DynamoDB table and save them to a CSV or JSONL file.

        If 'partition_key'/'partition_value' are specified, a query operation is used
        (optionally narrowed further by 'sort_key'/'sort_value'). Otherwise, a scan
        operation is used to retrieve the whole table, matching the prior behavior.
        """
        os.makedirs(self.args.dest_dir, exist_ok=True)

        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=self.args.access_key,
            aws_secret_access_key=self.args.secret_key,
            region_name=self.args.region,
        )
        table = dynamodb.Table(self.args.table_name)

        if self.args.partition_key:
            items = self._query_table(table)
        else:
            items = self._scan_table(table)

        file_path = os.path.join(self.args.dest_dir, self.args.file_name)
        if self.args.file_format == "jsonl":
            self._write_jsonl(items, file_path)
        else:
            self._write_csv(items, file_path)

        self.logger.info(
            f"Downloaded items from DynamoDB table {self.args.table_name} to {file_path}"
        )

    def _paginate(self, operation, **kwargs):
        """
        Generator that repeatedly calls a boto3 Table operation (scan or query),
        following DynamoDB's ExclusiveStartKey/LastEvaluatedKey pagination.

        Args:
            operation: bound Table method to call, e.g. table.scan or table.query
            **kwargs: extra arguments passed to the operation, e.g. KeyConditionExpression

        Yields:
            dict: each item returned by the operation
        """
        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                response = operation(ExclusiveStartKey=last_evaluated_key, **kwargs)
            else:
                response = operation(**kwargs)

            for item in response["Items"]:
                yield item

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    def _scan_table(self, table):
        """
        Generator function that scans a DynamoDB table and retrieves all items.

        Args:
            table (boto3.resources.factory.dynamodb.Table): DynamoDB table to scan

        Yields:
            dict: each item from the table
        """
        yield from self._paginate(table.scan)

    def _query_table(self, table):
        """
        Generator function that queries a DynamoDB table by partition key
        (and optionally sort key), retrieving all matching items.

        Args:
            table (boto3.resources.factory.dynamodb.Table): DynamoDB table to query

        Yields:
            dict: each matching item from the table
        """
        key_condition = Key(self.args.partition_key).eq(self.args.partition_value)
        if self.args.sort_key:
            key_condition &= Key(self.args.sort_key).eq(self.args.sort_value)

        yield from self._paginate(table.query, KeyConditionExpression=key_condition)

    def _write_jsonl(self, items, file_path):
        """
        Write items to a file in JSONL format.

        Args:
            items (iterator): iterator of items to write
            file_path (str): destination file path
        """
        with open(file_path, "w") as f:
            for item in items:
                json_item = json.dumps(
                    item, default=self._json_serial, sort_keys=False, ensure_ascii=False
                )
                f.write(json_item + "\n")

    def _json_serial(self, obj):
        """
        JSON serialization helper for types not natively supported by json.dumps.
        """
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return str(obj)

    def _write_csv(self, items, file_path):
        """
        Write items to a file in CSV format.

        Args:
            items (iterator): iterator of items to write
            file_path (str): destination file path
        """
        with open(file_path, "w", newline="") as f:
            writer = None
            for item in items:
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(item.keys()))
                    writer.writeheader()

                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        # Nested attribute values are converted to JSON
                        item[key] = json.dumps(
                            value, default=self._json_serial, sort_keys=False, ensure_ascii=False
                        )

                writer.writerow(item)

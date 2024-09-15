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

import boto3

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.aws import BaseAws, BaseS3
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.constant import StepStatus
from cliboa.util.exception import FileNotFound, InvalidFormat, InvalidParameter


class S3Upload(BaseS3):
    """
    Upload to S3
    """

    def __init__(self):
        super().__init__()
        self._key = None
        self._src_dir = None
        self._src_pattern = None
        self._quit = False

    def key(self, key):
        self._key = key

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def quit(self, quit):
        self._quit = quit

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        adapter = S3Adapter(self._access_key, self._secret_key, self._profile)
        resource = adapter.get_resource()
        bucket = resource.Bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)

        if len(files) > 0:
            for f in files:
                if self._key:
                    if not self._key.endswith("/"):
                        self._key = self._key + "/"
                    s = "{}{}".format(self._key, os.path.basename(f))
                    bucket.upload_file(Key=s, Filename=f)
                else:
                    bucket.upload_file(Key=os.path.basename(f), Filename=f)
        else:
            self._logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self._src_dir, self._src_pattern)
                )
            )
            if self._quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION


class DynamoDBWrite(BaseAws):
    """
    Class to write data from CSV or JSONL files to DynamoDB
    """

    def __init__(self):
        super().__init__()
        self._table_name = None
        self._src_dir = None
        self._src_pattern = None
        self._file_format = "csv"

    def table_name(self, table_name):
        self._table_name = table_name

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def file_format(self, file_format):
        if file_format not in ["csv", "jsonl"]:
            raise InvalidParameter("file_format must be either 'csv' or 'jsonl'")
        self._file_format = file_format

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._table_name, self._src_dir, self._src_pattern, self._file_format],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("Target file was not found.")

        dynamodb = boto3.resource("dynamodb", region_name=self._region)

        try:
            table = dynamodb.Table(self._table_name)
            table.load()

            table_info = table.key_schema
            partition_key = next(key for key in table_info if key["KeyType"] == "HASH")[
                "AttributeName"
            ]
            sort_key = next(
                (key["AttributeName"] for key in table_info if key["KeyType"] == "RANGE"), None
            )
        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            self._logger.error(f"Table '{self._table_name}' does not exist.")
            raise

        for file in files:
            if self._file_format.lower() == "csv":
                self._write_csv_to_dynamodb(file, table, partition_key, sort_key)
            elif self._file_format.lower() == "jsonl":
                self._write_jsonl_to_dynamodb(file, table, partition_key, sort_key)
            else:
                raise InvalidParameter(f"Unsupported file type: {self._file_format}")

        self._logger.info("Data has been written to DynamoDB.")

    def _write_csv_to_dynamodb(self, file, table, partition_key, sort_key):
        """Write CSV file to DynamoDB
        Args:
            file (str): File path
            table (boto3.resources.factory.dynamodb.Table): DynamoDB table
            partition_key (str): Partition key
            sort_key (str): Sort key
        """
        with open(file, "r") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            if partition_key not in headers:
                raise InvalidParameter(
                    f"Partition key '{partition_key}' not found in CSV headers of file: {file}"
                )
            if sort_key and sort_key not in headers:
                raise InvalidParameter(
                    f"Sort key '{sort_key}' not found in CSV headers of file: {file}"
                )

            with table.batch_writer() as batch:
                for row in reader:
                    batch.put_item(Item=row)

    def _write_jsonl_to_dynamodb(self, file, table, partition_key, sort_key):
        """Write JSONL file to DynamoDB
        Args:
            file (str): File path
            table (boto3.resources.factory.dynamodb.Table): DynamoDB table
            partition_key (str): Partition key
            sort_key (str): Sort key
        """
        with open(file, "r") as f:
            with table.batch_writer() as batch:
                for line_number, line in enumerate(f, 1):
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError as e:
                        raise InvalidFormat(
                            f"Invalid JSON format in file {file} at line {line_number}: {str(e)}"
                        )

                    if partition_key not in item:
                        raise InvalidParameter(
                            f"Partition key '{partition_key}' not found in JSONL file: {file} at line {line_number}"
                        )
                    if sort_key and sort_key not in item:
                        raise InvalidParameter(
                            f"Sort key '{sort_key}' not found in JSONL file: {file} at line {line_number}"
                        )
                    batch.put_item(Item=item)

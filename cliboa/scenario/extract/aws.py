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

import boto3

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

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        stored = self.get_from_context()
        bucket = stored.get("bucket")
        keys = stored.get("keys")

        if keys is not None and len(keys) > 0:
            self._region = self.get_symbol_argument("region")
            self._access_key = self.get_symbol_argument("access_key")
            self._secret_key = self.get_symbol_argument("secret_key")
            self._profile = self.get_symbol_argument("profile")
            self._bucket = self.get_symbol_argument("bucket")
            self._key = self.get_symbol_argument("key")
            self._prefix = self.get_symbol_argument("prefix")
            self._delimiter = self.get_symbol_argument("delimiter")
            self._src_pattern = self.get_symbol_argument("src_pattern")

            adapter = S3Adapter(
                self._access_key, self._secret_key, self._profile, self._role_arn, self._external_id
            )
            client = adapter.get_client()

            for key in keys:
                client.delete_object(Bucket=bucket, Key=key)
                self._logger.info("%s is successfully deleted." % bucket + "/" + key)
        else:
            self._logger.info("No files to delete.")


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

    def __init__(self):
        super().__init__()
        self._table_name = None
        self._dest_dir = "."
        self._file_name = None
        self._file_format = "csv"

    def table_name(self, table_name):
        self._table_name = table_name

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def file_name(self, file_name):
        self._file_name = file_name

    def file_format(self, file_format):
        if file_format not in ["csv", "jsonl"]:
            raise InvalidParameter("file_format must be either 'csv' or 'jsonl'")
        self._file_format = file_format

    def execute(self, *args):
        """
        DynamoDBからデータをダウンロードし、指定されたフォーマットでファイルに保存します。
        """
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._table_name, self._file_name])
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )
        table = dynamodb.Table(self._table_name)

        file_path = os.path.join(self._dest_dir, self._file_name)
        if self._file_format == "jsonl":
            self._write_jsonl(self._scan_table(table), file_path)
        else:
            self._write_csv(self._scan_table(table), file_path)

        self._logger.info(f"Downloaded items from DynamoDB table {self._table_name} to {file_path}")

    def _scan_table(self, table):
        """
        DynamoDBテーブルをスキャンし、全アイテムを取得するジェネレータ関数。

        Args:
            table (boto3.resources.factory.dynamodb.Table): スキャン対象のDynamoDBテーブル

        Yields:
            dict: テーブルの各アイテム
        """
        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                response = table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = table.scan()

            for item in response["Items"]:
                yield item

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    def _write_jsonl(self, items, file_path):
        """
        アイテムをJSONL形式でファイルに書き込みます。
        Args:
            items (iterator): 書き込むアイテムのイテレータ
            file_path (str): 書き込み先のファイルパス
        """
        with open(file_path, "w") as f:
            for item in items:
                json_item = json.dumps(
                    item, default=self._json_serial, sort_keys=False, ensure_ascii=False
                )
                f.write(json_item + "\n")

    def _json_serial(self, obj):
        """
        JSONシリアライズ関数
        """
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return str(obj)

    def _write_csv(self, items, file_path):
        """
        アイテムをCSV形式でファイルに書き込みます。

        Args:
            items (iterator): 書き込むアイテムのイテレータ
            file_path (str): 書き込み先のファイルパス
        """
        with open(file_path, "w", newline="") as f:
            writer = None
            for item in items:
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(item.keys()))
                    writer.writeheader()

                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        # ネストされた属性値はJSON形式に変換
                        item[key] = json.dumps(
                            value, default=self._json_serial, sort_keys=False, ensure_ascii=False
                        )

                writer.writerow(item)

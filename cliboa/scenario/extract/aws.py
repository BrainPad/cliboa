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
from boto3.dynamodb.conditions import Attr, Key

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.aws import BaseAws, BaseS3
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.cache import ObjectStore
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
        ObjectStore.put(self._step, {"bucket": self._bucket, "keys": keys})


class S3DownloadFileDelete(BaseS3):
    """
    Delete all downloaded files from S3
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        stored = ObjectStore.get(self._symbol)
        bucket = stored.get("bucket")
        keys = stored.get("keys")

        if keys is not None and len(keys) > 0:
            self._region = super().get_step_argument("region")
            self._access_key = super().get_step_argument("access_key")
            self._secret_key = super().get_step_argument("secret_key")
            self._profile = super().get_step_argument("profile")
            self._bucket = super().get_step_argument("bucket")
            self._key = super().get_step_argument("key")
            self._prefix = super().get_step_argument("prefix")
            self._delimiter = super().get_step_argument("delimiter")
            self._src_pattern = super().get_step_argument("src_pattern")

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
        self._filter_conditions = None

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

    def filter_conditions(self, filter_conditions):
        self._filter_conditions = filter_conditions

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

        # filter_conditionsとテーブルのキー情報に基づいてquery/scanを選択
        if self._filter_conditions:
            items_generator = self._read_with_filter(table)
        else:
            items_generator = self._scan_table(table)

        file_path = os.path.join(self._dest_dir, self._file_name)
        if self._file_format == "jsonl":
            self._write_jsonl(items_generator, file_path)
        else:
            self._write_csv(items_generator, file_path)

        self._logger.info(f"Downloaded items from DynamoDB table {self._table_name} to {file_path}")

    def _scan_table(self, table):
        """
        DynamoDBテーブルをスキャンし、全アイテムを取得するジェネレータ関数。

        Args:
            table (boto3.resources.factory.dynamodb.Table): スキャン対象のDynamoDBテーブル

        Yields:
            dict: テーブルの各アイテム
        """
        yield from self._execute_with_pagination(table, "scan")

    def _read_with_filter(self, table):
        """
        フィルター条件に基づいてquery操作またはscan操作を実行

        Args:
            table: DynamoDBテーブルオブジェクト

        Yields:
            dict: テーブルの各アイテム
        """
        # テーブルのキー情報を取得
        key_schema = table.key_schema
        partition_key_name = None
        sort_key_name = None

        for key in key_schema:
            if key["KeyType"] == "HASH":
                partition_key_name = key["AttributeName"]
            elif key["KeyType"] == "RANGE":
                sort_key_name = key["AttributeName"]

        # パーティションキーが条件に含まれているかチェック
        partition_key_value = self._filter_conditions.get(partition_key_name)

        if partition_key_value is not None:
            # Query操作
            self._logger.info(
                f"Using query operation with partition key: "
                f"{partition_key_name}={partition_key_value}"
            )
            yield from self._query_with_filter(
                table, partition_key_name, partition_key_value, sort_key_name
            )
        else:
            # Scan操作
            self._logger.info("Using scan operation with filter")
            yield from self._scan_with_filter(table)

    def _build_filter_expression(self, exclude_keys: list[str] = []):
        """
        FilterExpression構築

        Args:
            exclude_keys: 除外するキーのリスト（パーティションキー、ソートキーなど）

        Returns:
            filter_expression or None
        """
        filter_expression = None
        for attr_name, attr_value in self._filter_conditions.items():
            if attr_name in exclude_keys:
                continue

            condition = Attr(attr_name).eq(attr_value)
            filter_expression = (
                condition
                if filter_expression is None
                else (filter_expression & condition)
            )

        return filter_expression

    def _execute_with_pagination(self, table, operation_name, **base_kwargs):
        """
        DynamoDB操作をページネーション付きで実行

        Args:
            table: DynamoDBテーブルオブジェクト
            operation_name: 'query' or 'scan'
            **base_kwargs: 操作固有のパラメータ（KeyConditionExpression等）

        Yields:
            dict: 各アイテム
        """
        operation = getattr(table, operation_name)
        last_evaluated_key = None

        while True:
            kwargs = base_kwargs.copy()
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = operation(**kwargs)

            for item in response["Items"]:
                yield item

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    def _query_with_filter(
        self, table, partition_key_name, partition_key_value, sort_key_name
    ):
        """Query操作でデータを取得"""
        # KeyConditionExpression構築
        key_condition = Key(partition_key_name).eq(partition_key_value)
        if sort_key_name and sort_key_name in self._filter_conditions:
            sort_key_value = self._filter_conditions[sort_key_name]
            key_condition = key_condition & Key(sort_key_name).eq(sort_key_value)

        # FilterExpression構築（パーティション/ソートキーを除外）
        exclude_keys = [partition_key_name]
        if sort_key_name:
            exclude_keys.append(sort_key_name)
        filter_expression = self._build_filter_expression(exclude_keys)

        # Query実行
        query_kwargs = {"KeyConditionExpression": key_condition}
        if filter_expression is not None:
            query_kwargs["FilterExpression"] = filter_expression

        yield from self._execute_with_pagination(table, "query", **query_kwargs)

    def _scan_with_filter(self, table):
        """Scan操作でデータを取得"""
        # FilterExpression構築（全条件）
        filter_expression = self._build_filter_expression()

        # Scan実行
        scan_kwargs = {}
        if filter_expression is not None:
            scan_kwargs["FilterExpression"] = filter_expression

        yield from self._execute_with_pagination(table, "scan", **scan_kwargs)

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

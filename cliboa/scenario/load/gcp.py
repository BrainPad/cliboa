#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
import ast
import csv
import os
import pandas
from datetime import datetime
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.extract.file import FileRead
from cliboa.scenario.gcp import BaseBigQuery, BaseGcs
from cliboa.util.cache import ObjectStore
from cliboa.util.exception import InvalidFormat, InvalidFileCount, FileNotFound


class BigQueryCreate(BaseBigQuery):
    """
    Insert data into BigQuery table
    """

    # default bulk line count to change to dataframe object
    BULK_LINE_CNT = 10000

    # BigQuery insert mode
    REPLACE = "replace"
    APPEND = "append"

    def __init__(self):
        super().__init__()
        self.__table_schema = None
        self.__replace = True

    @property
    def table_schema(self):
        return self.__table_schema

    @table_schema.setter
    def table_schema(self, table_schema):
        self.__table_schema = table_schema

    @property
    def replace(self):
        return self.__replace

    @replace.setter
    def replace(self, replace):
        self.__replace = replace

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        super().execute()

        param_valid = EssentialParameters(
            self.__class__.__name__, [self.__table_schema]
        )
        param_valid()

        cache_list = []
        inserts = False
        # initial if_exists
        if_exists = self.REPLACE if self.__replace is True else self.APPEND
        with open(self._s.cache_file, "r", encoding="utf-8") as f:
            for i, l_str in enumerate(f):
                l_dict = ast.literal_eval(l_str)
                cache_list.append(l_dict)
                if len(cache_list) == self.BULK_LINE_CNT:
                    df = pandas.DataFrame(self.__create_insert_data(cache_list))
                    if inserts is True:
                        # if_exists after the first insert execution
                        if_exists = self.APPEND
                    dest_tbl = self._dataset + "." + self._tblname
                    self._logger.info(
                        "Start insert %s rows to %s" % (len(cache_list), dest_tbl)
                    )
                    df.to_gbq(
                        dest_tbl,
                        project_id=self._project_id,
                        if_exists=if_exists,
                        table_schema=self.__table_schema,
                        location=self._location,
                        credentials=self._auth(),
                    )
                    cache_list.clear()
                    inserts = True
            if len(cache_list) > 0:
                df = pandas.DataFrame(self.__create_insert_data(cache_list))
                if inserts is True:
                    # if_exists after the first insert execution
                    if_exists = self.APPEND
                dest_tbl = self._dataset + "." + self._tblname
                self._logger.info(
                    "Start insert %s rows to %s" % (len(cache_list), dest_tbl)
                )
                df.to_gbq(
                    dest_tbl,
                    project_id=self._project_id,
                    if_exists=if_exists,
                    table_schema=self.__table_schema,
                    location=self._location,
                    credentials=self._auth(),
                )
        self._s.remove()

    def __create_insert_data(self, cache_list):
        """
        Create insert data like the below.

        insert_data = {
            "column1": [1, 2],
            "column2": ["spam", "spam"],
            ...
        }

        Args
            cache_list: dictionary list of input cache
        """
        insert_data = {}
        columns = [name_and_type["name"] for name_and_type in self.__table_schema]
        for c in columns:
            v_list = [d.get(c) for d in cache_list]
            if not v_list:
                raise InvalidFormat(
                    "Specified column %s does not exist in an input file." % c
                )
            insert_data[c] = v_list
        return insert_data


class GcsFileUpload(BaseGcs):
    """
    Upload local files to GCS
    """

    def __init__(self):
        super().__init__()

        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = ""

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self._src_dir = src_dir

    @property
    def src_pattern(self):
        return self._src_pattern

    @src_pattern.setter
    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    @property
    def dest_dir(self):
        return self._dest_dir

    @dest_dir.setter
    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.debug("%s : %s" % (k, v))

        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        gcs_client = storage.Client.from_service_account_json(self._credentials)
        bucket = gcs_client.get_bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Upload files %s" % files)
        for file in files:
            self._logger.info("Start upload %s" % file)
            blob = bucket.blob(os.path.join(self._dest_dir, os.path.basename(file)))
            blob.upload_from_filename(file)
            self._logger.info("Finish upload %s" % file)


class CsvReadBigQueryCreate(BaseBigQuery, FileRead):
    """
    Read csv and Insert data into BigQuery table
    """

    # default bulk line count to change to dataframe object
    BULK_LINE_CNT = 10000

    # BigQuery insert mode
    REPLACE = "replace"
    APPEND = "append"

    def __init__(self):
        super().__init__()
        self.__table_schema = None
        self.__replace = True
        self.__columns = []

    @property
    def table_schema(self):
        return self.__table_schema

    @table_schema.setter
    def table_schema(self, table_schema):
        self.__table_schema = table_schema

    @property
    def replace(self):
        return self.__replace

    @replace.setter
    def replace(self, replace):
        self.__replace = replace

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))

        BaseBigQuery.execute(self)
        FileRead.execute(self)

        param_valid = EssentialParameters(
            self.__class__.__name__, [self.__table_schema]
        )
        param_valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) > 1:
            raise InvalidFileCount("Input file must be only one.")
        if len(files) == 0:
            raise FileNotFound("The specified csv file not found.")

        insert_rows = []
        is_inserted = False
        # initial if_exists
        if_exists = self.REPLACE if self.__replace is True else self.APPEND
        self.__columns = [
            name_and_type["name"] for name_and_type in self.__table_schema
        ]
        with open(files[0], "r", encoding=self._encoding) as f:
            reader = csv.DictReader(f, delimiter=",")
            for r in reader:
                # extract only the specified columns
                row_dict = {}
                for c in self.__columns:
                    if not r.get(c):
                        continue
                    row_dict[c] = r.get(c)
                insert_rows.append(row_dict)

                if len(insert_rows) == self.BULK_LINE_CNT:
                    self.__exec_insert(insert_rows, is_inserted, if_exists)
                    insert_rows.clear()
                    is_inserted = True
            if len(insert_rows) > 0:
                self.__exec_insert(insert_rows, is_inserted, if_exists)

    def __exec_insert(self, insert_rows, is_inserted, if_exists):
        """
        Execute insert into a BigQuery table
        Args:
            insert_rows: rows to insert
            is_inserted: if the data is already inserted or not
            if_exists: replace or append
        """
        df = pandas.DataFrame(self.__format_insert_data(insert_rows))
        if is_inserted is True:
            # if_exists after the first insert execution
            if_exists = self.APPEND
        dest_tbl = self._dataset + "." + self._tblname
        self._logger.info("Start insert %s rows to %s" % (len(insert_rows), dest_tbl))
        df.to_gbq(
            dest_tbl,
            project_id=self._project_id,
            if_exists=if_exists,
            table_schema=self.__table_schema,
            location=self._location,
            credentials=self._auth(),
        )

    def __format_insert_data(self, insert_rows):
        """
        Format insert data to pass DataFrame as the below.

        insert_data = {
            "column1": [1, 2],
            "column2": ["spam", "spam"],
            ...
        }

        Args
            insert_rows: dictionary list of input cache
        """
        insert_data = {}
        for c in self.__columns:
            v_list = [d.get(c) for d in insert_rows]
            if not v_list:
                raise InvalidFormat(
                    "Specified column %s does not exist in an input file." % c
                )
            insert_data[c] = v_list
        return insert_data

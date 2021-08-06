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
import ast
import csv
import json
import os

import pandas

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.gcp import BaseBigQuery, BaseFirestore, BaseGcs
from cliboa.scenario.load.file import FileWrite
from cliboa.util.exception import FileNotFound, InvalidFileCount, InvalidFormat
from cliboa.util.gcp import Firestore, Gcs, ServiceAccount


class BigQueryWrite(BaseBigQuery, FileWrite):
    """
    Read csv and Insert data into BigQuery table
    """

    # default bulk line count to change to dataframe object
    _BULK_LINE_CNT = 10000

    # BigQuery insert mode
    _REPLACE = "replace"
    _APPEND = "append"

    def __init__(self):
        super().__init__()
        self._table_schema = None
        self._replace = True
        self._columns = []
        self._has_header = True

    def table_schema(self, table_schema):
        self._table_schema = table_schema

    def replace(self, replace):
        self._replace = replace

    def has_header(self, has_header):
        self._has_header = has_header

    def execute(self, *args):
        BaseBigQuery.execute(self)

        param_valid = EssentialParameters(self.__class__.__name__, [self._table_schema])
        param_valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("The specified csv file not found.")
        self._logger.info("insert target files %s" % files)

        is_inserted = False
        # initial if_exists
        if_exists = self._REPLACE if self._replace is True else self._APPEND
        self._columns = [name_and_type["name"] for name_and_type in self._table_schema]
        for file in files:
            insert_rows = []
            with open(file, "r", encoding=self._encoding) as f:
                reader = (
                    csv.DictReader(f, delimiter=",")
                    if self._has_header is True
                    else csv.reader(f, delimiter=",")
                )
                if self._has_header is True:
                    for r in reader:
                        # extract only the specified columns
                        contents = {}
                        for c in self._columns:
                            if not r.get(c):
                                continue
                            contents[c] = r.get(c)
                        insert_rows.append(contents)

                        # bulk insert
                        if len(insert_rows) == self._BULK_LINE_CNT:
                            self._exec_insert(insert_rows, is_inserted, if_exists)
                            insert_rows.clear()
                            is_inserted = True
                    if len(insert_rows) > 0:
                        self._exec_insert(insert_rows, is_inserted, if_exists)
                        is_inserted = True

                else:
                    # csv headers do not exist
                    for row in reader:
                        contents = {}
                        for i, c in enumerate(self._columns):
                            contents[c] = row[i]
                        insert_rows.append(contents)

                        # bulk insert
                        if len(insert_rows) == self._BULK_LINE_CNT:
                            self._exec_insert(insert_rows, is_inserted, if_exists)
                            insert_rows.clear()
                            is_inserted = True
                    if len(insert_rows) > 0:
                        self._exec_insert(insert_rows, is_inserted, if_exists)
                        is_inserted = True

    def _exec_insert(self, insert_rows, is_inserted, if_exists):
        """
        Execute insert into a BigQuery table
        Args:
            insert_rows: rows to insert
            is_inserted: if the data is already inserted or not
            if_exists: replace or append
        """
        df = pandas.DataFrame(self._format_insert_data(insert_rows))
        if is_inserted is True:
            # if_exists after the first insert execution
            if_exists = self._APPEND
        dest_tbl = self._dataset + "." + self._tblname
        self._logger.info("Start insert %s rows to %s" % (len(insert_rows), dest_tbl))

        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                    "Please see more information "
                    "https://github.com/BrainPad/cliboa/blob/master/docs/modules/bigquery_write.md"
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)
        df.to_gbq(
            dest_tbl,
            project_id=self._project_id,
            if_exists=if_exists,
            table_schema=self._table_schema,
            location=self._location,
            credentials=ServiceAccount.auth(key_filepath),
        )

    def _format_insert_data(self, insert_rows):
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
        for c in self._columns:
            v_list = [d.get(c) for d in insert_rows]
            if not v_list:
                raise InvalidFormat(
                    "Specified column %s does not exist in an input file." % c
                )
            insert_data[c] = v_list
        return insert_data


class BigQueryCreate(BaseBigQuery):
    """
    @deprecated
    Please Use BigQueryWrite instead.

    Insert data into BigQuery table
    """

    # default bulk line count to change to dataframe object
    BULK_LINE_CNT = 10000

    # BigQuery insert mode
    REPLACE = "replace"
    APPEND = "append"

    def __init__(self):
        super().__init__()
        self._table_schema = None
        self._replace = True

    def table_schema(self, table_schema):
        self._table_schema = table_schema

    def replace(self, replace):
        self._replace = replace

    def execute(self, *args):
        self._logger.warning("Deprecated. Please Use BigQueryWrite instead.")

        super().execute()

        param_valid = EssentialParameters(self.__class__.__name__, [self._table_schema])
        param_valid()

        cache_list = []
        inserts = False
        # initial if_exists
        if_exists = self.REPLACE if self._replace is True else self.APPEND

        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)

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
                        table_schema=self._table_schema,
                        location=self._location,
                        credentials=ServiceAccount.auth(key_filepath),
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
                    table_schema=self._table_schema,
                    location=self._location,
                    credentials=ServiceAccount.auth(key_filepath),
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
        columns = [name_and_type["name"] for name_and_type in self._table_schema]
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
    @deprecated
    Please Use GcsUpload instead.

    Upload local files to GCS
    """

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = ""

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        self._logger.warning("Deprecated. Please Use GcsUpload instead.")

        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)
        gcs_client = Gcs.get_gcs_client(key_filepath)
        bucket = gcs_client.bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Upload files %s" % files)
        for file in files:
            self._logger.info("Start upload %s" % file)
            blob = bucket.blob(os.path.join(self._dest_dir, os.path.basename(file)))
            blob.upload_from_filename(file)
            self._logger.info("Finish upload %s" % file)


class GcsUpload(BaseGcs):
    """
    Upload local files to GCS
    """

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = ""

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                    "Please see more information "
                    "https://github.com/BrainPad/cliboa/blob/master/docs/modules/gcs_upload.md"
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)
        gcs_client = Gcs.get_gcs_client(key_filepath)
        bucket = gcs_client.bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Upload files %s" % files)
        for file in files:
            self._logger.info("Start upload %s" % file)
            blob = bucket.blob(os.path.join(self._dest_dir, os.path.basename(file)))
            blob.upload_from_filename(file)
            self._logger.info("Finish upload %s" % file)


class CsvReadBigQueryCreate(BaseBigQuery, FileWrite):
    """
    deprecated
    Please Use GcsUpload instead.

    Read csv and Insert data into BigQuery table
    """

    # default bulk line count to change to dataframe object
    BULK_LINE_CNT = 10000

    # BigQuery insert mode
    REPLACE = "replace"
    APPEND = "append"

    def __init__(self):
        super().__init__()
        self._table_schema = None
        self._replace = True
        self.__columns = []

    def table_schema(self, table_schema):
        self._table_schema = table_schema

    def replace(self, replace):
        self._replace = replace

    def execute(self, *args):
        self._logger.warning("Deprecated. Please Use BigQueryWrite instead.")

        BaseBigQuery.execute(self)

        param_valid = EssentialParameters(self.__class__.__name__, [self._table_schema])
        param_valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) > 1:
            raise InvalidFileCount("Input file must be only one.")
        if len(files) == 0:
            raise FileNotFound("The specified csv file not found.")

        insert_rows = []
        is_inserted = False
        # initial if_exists
        if_exists = self.REPLACE if self._replace is True else self.APPEND
        self.__columns = [name_and_type["name"] for name_and_type in self._table_schema]
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
        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)

        df.to_gbq(
            dest_tbl,
            project_id=self._project_id,
            if_exists=if_exists,
            table_schema=self._table_schema,
            location=self._location,
            credentials=ServiceAccount.auth(key_filepath),
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


class FirestoreDocumentCreate(BaseFirestore):
    """
    Create document on FIrestore
    """

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__,
            [self._collection, self._src_dir, self._src_pattern],
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("No files are found.")

        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `credentials` will be changed to accept only dictionary types. "
                    "Please see more information "
                    "https://github.com/BrainPad/cliboa/blob/master/docs/modules/firestore_document_create.md"  # noqa
                )
            )
            key_filepath = self._credentials
        else:
            key_filepath = self._source_path_reader(self._credentials)
        firestore_client = Firestore.get_firestore_client(key_filepath)

        for file in files:
            with open(file) as f:
                fname = os.path.splitext(os.path.basename(file))[0]
                doc = firestore_client.collection(self._collection).document(fname)
                doc.set(json.load(f))

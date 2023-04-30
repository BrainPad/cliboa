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

import pandas

from cliboa.adapter.gcp import BigQueryAdapter, FireStoreAdapter, GcsAdapter, ServiceAccount
from cliboa.core.validator import EssentialParameters
from cliboa.scenario.gcp import BaseBigQuery, BaseFirestore, BaseGcs
from cliboa.scenario.load.file import FileWrite
from cliboa.util.exception import FileNotFound, InvalidFormat


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

        df.to_gbq(
            dest_tbl,
            project_id=self._project_id,
            if_exists=if_exists,
            table_schema=self._table_schema,
            location=self._location,
            credentials=ServiceAccount().auth(self.get_credentials()),
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
                raise InvalidFormat("Specified column %s does not exist in an input file." % c)
            insert_data[c] = v_list
        return insert_data


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

        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        gcs_client = GcsAdapter().get_client(credentials=self.get_credentials())
        bucket = gcs_client.bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Upload files %s" % files)
        for file in files:
            self._logger.info("Start upload %s" % file)
            blob = bucket.blob(os.path.join(self._dest_dir, os.path.basename(file)))
            blob.upload_from_filename(file)
            self._logger.info("Finish upload %s" % file)


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

        firestore_client = FireStoreAdapter().get_client(self.get_credentials())
        for file in files:
            with open(file) as f:
                fname = os.path.splitext(os.path.basename(file))[0]
                doc = firestore_client.collection(self._collection).document(fname)
                doc.set(json.load(f))


class BigQueryCopy(BaseBigQuery):
    """
    Copy Bigquery from one table to another
    """

    def __init__(self):
        super().__init__()

        self._dest_dataset = None
        self._dest_tblname = None

    def dest_dataset(self, dest_dataset):
        self._dest_dataset = dest_dataset

    def dest_tblname(self, dest_tblname):
        self._dest_tblname = dest_tblname

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._dataset, self._tblname, self._location, self._dest_dataset, self._dest_tblname],
        )
        valid()

        # Define Source Table and Destination Table
        source_table_id = "{}.{}.{}".format(self._project_id, self._dataset, self._tblname)
        destination_table_id = "{}.{}.{}".format(
            self._project_id, self._dest_dataset, self._dest_tblname
        )

        # Client Setup
        gbq_client = BigQueryAdapter().get_client(
            credentials=self.get_credentials(),
            project=self._project_id,
            location=self._location,
        )

        # Create Copy Job
        job = gbq_client.copy_table(source_table_id, destination_table_id)

        # Wait for the job to complete.
        job.result()

        print("A copy of the table created.")

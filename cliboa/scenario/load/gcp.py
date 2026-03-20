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
from cliboa.scenario.file import FileRead
from cliboa.scenario.gcp import BaseBigQuery, BaseFirestore, BaseGcs
from cliboa.util.exception import FileNotFound, InvalidFormat


class BigQueryWrite(BaseBigQuery, FileRead):
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
        self._columns = []

    class Arguments(BaseBigQuery.Arguments, FileRead.Arguments):
        table_schema: list[dict]
        replace: bool = True
        has_header: bool = True

    def execute(self, *args):
        files = self.get_src_files()
        if not self.check_file_existence(files):
            return 1
        self.logger.info("insert target files %s" % files)

        is_inserted = False
        # initial if_exists
        if_exists = self._REPLACE if self.args.replace is True else self._APPEND
        self._columns = [name_and_type["name"] for name_and_type in self.args.table_schema]
        for file in files:
            insert_rows = []
            with open(file, "r", encoding=self.args.encoding) as f:
                reader = (
                    csv.DictReader(f, delimiter=",")
                    if self.args.has_header is True
                    else csv.reader(f, delimiter=",")
                )
                if self.args.has_header is True:
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
        dest_tbl = self.args.dataset + "." + self.args.tblname
        self.logger.info("Start insert %s rows to %s" % (len(insert_rows), dest_tbl))

        df.to_gbq(
            dest_tbl,
            project_id=self.args.project_id,
            if_exists=if_exists,
            table_schema=self.args.table_schema,
            location=self.args.location,
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


class GcsUpload(BaseGcs, FileRead):
    """
    Upload local files to GCS
    """

    class Arguments(BaseGcs.Arguments, FileRead.Arguments):
        dest_dir: str = ""

    def execute(self, *args):
        gcs_client = GcsAdapter().get_client(credentials=self.get_credentials())
        bucket = gcs_client.bucket(self.args.bucket)
        files = self.get_src_files()
        self.logger.info("Upload files %s" % files)
        for file in files:
            self.logger.info("Start upload %s" % file)
            blob = bucket.blob(os.path.join(self.args.dest_dir, os.path.basename(file)))
            blob.upload_from_filename(file)
            self.logger.info("Finish upload %s" % file)


class FirestoreDocumentCreate(BaseFirestore, FileRead):
    """
    Create document on FIrestore
    """

    class Arguments(BaseFirestore.Arguments, FileRead.Arguments):
        pass

    def execute(self, *args):
        files = self.get_src_files()
        if not self.check_file_existence(files):
            raise FileNotFound("No files are found.")

        firestore_client = FireStoreAdapter().get_client(self.get_credentials())
        for file in files:
            with open(file) as f:
                fname = os.path.splitext(os.path.basename(file))[0]
                doc = firestore_client.collection(self.args.collection).document(fname)
                doc.set(json.load(f))


class BigQueryCopy(BaseBigQuery):
    """
    Copy Bigquery from one table to another
    """

    class Arguments(BaseBigQuery.Arguments):
        dest_dataset: str
        dest_tblname: str

    def execute(self, *args):
        # Define Source Table and Destination Table
        source_table_id = "{}.{}.{}".format(
            self.args.project_id, self.args.dataset, self.args.tblname
        )
        destination_table_id = "{}.{}.{}".format(
            self.args.project_id, self.args.dest_dataset, self.args.dest_tblname
        )

        # Client Setup
        gbq_client = BigQueryAdapter().get_client(
            credentials=self.get_credentials(),
            project=self.args.project_id,
            location=self.args.location,
        )

        # Create Copy Job
        job = gbq_client.copy_table(source_table_id, destination_table_id)

        # Wait for the job to complete.
        job.result()

        self.logger.info("A copy of the table created.")

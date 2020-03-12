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
import fnmatch
import json
import os
import pandas
import random
import re
import string
from datetime import datetime
from google.cloud import storage, bigquery

from cliboa.scenario.validator import EssentialParameters
from cliboa.scenario.gcp import BaseBigQuery, BaseGcs, BaseFirestore
from cliboa.util.cache import ObjectStore
from cliboa.util.gcp import ServiceAccount


class BigQueryReadCache(BaseBigQuery):
    """
    Get data from BigQuery and cache them as pandas.dataframe format.

    Use {BigQueryFileDownload} if the result query is estimated to be large.
    """

    def __init__(self):
        super().__init__()
        self._key = None
        self._query = None

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query

    def _get_query(self):
        if self._query is None:
            return "SELECT * FROM %s.%s" % (self._dataset, self._tblname)
        else:
            return self._query

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._key])
        valid()

        df = pandas.read_gbq(
            query=self._get_query(),
            dialect="standard",
            location=self._location,
            project_id=self._project_id,
            credentials=self._auth(),
        )
        ObjectStore.put(self._key, df)


class BigQueryFileDownload(BaseBigQuery):
    """
    Download query result as a csv file.

    This class saves BigQuery result as a temporary file in GCS, and then download.
    Download file could be a multiple if file size exceed 1GB.
    """

    def __init__(self):
        super().__init__()
        self._bucket = None
        self._dest_dir = None
        self._filename = None

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, bucket):
        self._bucket = bucket

    @property
    def dest_dir(self):
        return self._dest_dir

    @dest_dir.setter
    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, filename):
        self._filename = filename

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._tblname, self._bucket, self._dest_dir]
        )
        valid()

        os.makedirs(self._dest_dir, exist_ok=True)

        gbq_client = self._bigquery_client()
        gbq_ref = gbq_client.dataset(self._dataset).table(self._tblname)

        gcs_client = self._gcs_client()
        gcs_bucket = gcs_client.get_bucket(self._bucket)

        ymd_hms = datetime.now().strftime("%Y%m%d%H%M%S%f")
        path = "%s-%s" % ("".join(random.choices(string.ascii_letters, k=8)), ymd_hms)
        prefix = "%s/%s/%s" % (self._dataset, self._tblname, path)

        # gsc dir -> gs://{bucket_name}/{dataset_name}/{table_name}/{XXXXXXXX}-{yyyyMMddHHmmssSSS}/*.csv.gz
        if self._filename:
            dest_gcs = "gs://%s/%s/%s*.csv.gz" % (self._bucket, prefix, self._filename)
        else:
            dest_gcs = "gs://%s/%s/*.csv.gz" % (self._bucket, prefix)

        # job config settings
        job_config = bigquery.ExtractJobConfig()
        job_config.compression = bigquery.Compression.GZIP
        job_config.desctination_format = bigquery.DestinationFormat.CSV

        # Execute query.
        job = gbq_client.extract_table(
            gbq_ref, dest_gcs, job_config=job_config, location=self._location
        )
        job.result()

        # Download from gcs
        for blob in gcs_bucket.list_blobs(prefix=prefix):
            dest = os.path.join(self._dest_dir, os.path.basename(blob.name))
            blob.download_to_filename(dest)

        # Cleanup temporary files
        for blob in gcs_bucket.list_blobs(prefix=prefix):
            blob.delete()


class GcsDownload(BaseGcs):
    """
    Download from GCS
    """

    def __init__(self):
        super().__init__()
        self._prefix = None
        self._delimiter = None
        self._src_pattern = None
        self._dest_dir = "."

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, prefix):
        self._prefix = prefix

    @property
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    def delimiter(self, delimiter):
        self._delimiter = delimiter

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
            self._logger.info("%s : %s" % (k, v))
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        client = self._gcs_client()
        bucket = client.get_bucket(self._bucket)
        dl_files = []
        for blob in bucket.list_blobs(prefix=self._prefix, delimiter=self._delimiter):
            r = re.compile(self._src_pattern)
            if not r.fullmatch(blob.name):
                continue
            dl_files.append(blob.name)
            blob.download_to_filename(
                os.path.join(self._dest_dir, os.path.basename(blob.name))
            )

        ObjectStore.put(self._step, dl_files)


class GcsDownloadFileDelete(BaseGcs):
    """
    Delete files downloaded from GCS by using cache
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))
        dl_files = ObjectStore.get(self._symbol)

        if len(dl_files) > 0:
            self._logger.info("Delete files %s" % dl_files)
            client = self._gcs_client()
            bucket = client.get_bucket(super().get_step_argument("bucket"))
            for blob in bucket.list_blobs(
                prefix=super().get_step_argument("prefix"),
                delimiter=super().get_step_argument("delimiter"),
            ):
                for dl_f in dl_files:
                    if dl_f == blob.name:
                        blob.delete()
                        break
        else:
            self._logger.info("No files to delete.")


class FirestoreDownloadDocument(BaseFirestore):
    """
    Download a document from Firestore
    """

    def __init__(self):
        super().__init__()

        self._dest_dir = None

    @property
    def dest_dir(self):
        return self._dest_dir

    @dest_dir.setter
    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        for k, v in self.__dict__.items():
            self._logger.info("%s : %s" % (k, v))

        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._collection, self._document, self._dest_dir]
        )
        valid()

        firestore_client = self._firestore_client()
        ref = firestore_client.document(self._collection, self._document)
        doc = ref.get()

        with open(os.path.join(self.dest_dir, doc.id), mode="wt") as f:
            f.write(json.dumps(doc.to_dict()))

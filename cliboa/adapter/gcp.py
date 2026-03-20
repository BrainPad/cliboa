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
from google.cloud import bigquery, firestore, storage
from google.oauth2 import service_account

from cliboa.util.base import _BaseObject


class ServiceAccount(_BaseObject):
    """
    Service Account api wrapper
    Creates a Signer instance from a service account .json file path
    or a dictionary containing service account info in Google format.
    Args:
        credentials: gcp service account json
    """

    def auth(self, credentials):
        if not credentials:
            return None
        return service_account.Credentials.from_service_account_file(credentials)


class GcsAdapter(_BaseObject):
    """
    Gcp Adaptor
    """

    def get_client(self, credentials):
        credentials_info = ServiceAccount().auth(credentials)
        return (
            storage.Client(credentials=credentials_info, project=credentials_info.project_id)
            if credentials_info
            else storage.Client()
        )


class BigQueryAdapter(_BaseObject):
    """
    BigQuery Adaptor
    """

    def get_client(self, credentials, project=None, location=None):
        credentials_info = ServiceAccount().auth(credentials)
        return (
            bigquery.Client(
                credentials=credentials_info,
                project=project if project else credentials_info.project_id,
                location=location,
            )
            if credentials_info
            else bigquery.Client()
        )

    @staticmethod
    def get_extract_job_config(print_header=True):
        return bigquery.ExtractJobConfig(print_header=print_header)

    @staticmethod
    def get_query_job_config():
        return bigquery.QueryJobConfig()

    @staticmethod
    def get_write_disposition():
        return bigquery.WriteDisposition.WRITE_TRUNCATE

    @staticmethod
    def get_compression_type():
        """
        Output compression type
        """
        return bigquery.Compression.GZIP

    @classmethod
    def get_destination_format(cls, ext):
        """
        Output file format
        Args:
            ext: destination file extention
        """
        format_and_dest_format = {
            ".csv": bigquery.DestinationFormat.CSV,
            ".json": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        }
        return format_and_dest_format.get(ext)


class FireStoreAdapter(_BaseObject):
    """
    FireStore Adaptor
    """

    def get_client(credentials):
        credentials_info = ServiceAccount().auth(credentials)
        return (
            firestore.Client(credentials=credentials_info, project=credentials_info.project_id)
            if credentials_info
            else firestore.Client()
        )

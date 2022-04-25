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


class ServiceAccount(object):
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


class GcsAdapter(object):
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


class BigQueryAdapter(object):
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


class FireStoreAdapter(object):
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

#
# Copyright 2020 BrainPad Inc. All Rights Reserved.
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

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters


class BaseGcp(BaseStep):
    """
    Base class of Gcp usage.
    """

    def __init__(self):
        super().__init__()
        self._project_id = None
        self._credentials = None

    def project_id(self, project_id):
        self._project_id = project_id

    def credentials(self, credentials):
        self._credentials = credentials

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._project_id])
        valid()

    def _auth(self):
        """
        @deprecated
        use ServiceAccount.auth() in util.gcp
        """
        if self._credentials:
            return service_account.Credentials.from_service_account_file(
                self._credentials
            )

    def _bigquery_client(self):
        """
        @deprecated
        use BigQuery.get_bigquery_client() in util.gcp
        """
        if self._credentials:
            return bigquery.Client.from_service_account_json(self._credentials)
        else:
            return bigquery.Client()

    def _gcs_client(self):
        """
        @deprecated
        use Gcs.get_gcs_client() in util.gcp
        """
        if self._credentials:
            return storage.Client.from_service_account_json(self._credentials)
        else:
            return storage.Client()

    def _firestore_client(self):
        """
        @deprecated
        use Firestore.get_firestore_client() in util.gcp
        """
        if self._credentials:
            return firestore.Client.from_service_account_json(self._credentials)
        else:
            return firestore.Client()


class BaseBigQuery(BaseGcp):
    """
    Base class of BigQuery use.

    """

    def __init__(self):
        super().__init__()

        self._dataset = None
        self._tblname = None
        self._location = None

    def dataset(self, dataset):
        self._dataset = dataset

    def tblname(self, tblname):
        self._tblname = tblname

    def location(self, location):
        self._location = location

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(
            self.__class__.__name__, [self._location, self._dataset]
        )
        valid()


class BaseGcs(BaseGcp):
    """
    Base class of Gcs use.

    """

    def __init__(self):
        super().__init__()
        self._bucket = None

    def bucket(self, bucket):
        self._bucket = bucket

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._bucket])
        valid()


class BaseFirestore(BaseGcp):
    """
    Base class of Firebase use.

    """

    def __init__(self):
        super().__init__()
        self._collection = None
        self._document = None

    def collection(self, collection):
        self._collection = collection

    def document(self, document):
        self._document = document

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
from google.cloud import storage, bigquery
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

    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    def project_id(self, project_id):
        self._project_id = project_id

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, credentials):
        self._credentials = credentials

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._project_id])
        valid()

    def _auth(self):
        if self._credentials:
            return service_account.Credentials.from_service_account_file(self._credentials)

    def _bigquery_client(self):
        if self._credentials:
            return bigquery.Client.from_service_account_json(self._credentials)
        else:
            return bigquery.Client()

    def _gcs_client(self):
        if self._credentials:
            return storage.Client.from_service_account_json(self._credentials)
        else:
            return storage.Client()


class BaseBigQuery(BaseGcp):
    """
    Base class of BigQuery use.

    """

    def __init__(self):
        super().__init__()

        self._dataset = None
        self._tblname = None
        self._location = None

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, dataset):
        self._dataset = dataset

    @property
    def tblname(self):
        return self._tblname

    @tblname.setter
    def tblname(self, tblname):
        self._tblname = tblname

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = location

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(
            self.__class__.__name__, [self._dataset, self._location]
        )


class BaseGcs(BaseGcp):
    """
    Base class of Gcs use.

    """

    def __init__(self):
        super().__init__()
        self._bucket = None

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, bucket):
        self._bucket = bucket

    def execute(self, *args):
        super().execute()
        valid = EssentialParameters(self.__class__.__name__, [self._bucket])

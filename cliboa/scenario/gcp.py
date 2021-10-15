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

    def get_credentials(self):
        if isinstance(self._credentials, str):
            self._logger.warning(
                (
                    "DeprecationWarning: "
                    "In the near future, "
                    "the `key` will be changed to accept only dictionary types. "
                    "Please see more information "
                    "https://github.com/BrainPad/cliboa/blob/master/docs/modules/sftp_download.md"
                )
            )
            return self._credentials
        else:
            return self._source_path_reader(self._credentials)


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
        valid = EssentialParameters(self.__class__.__name__, [self._location, self._dataset])
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

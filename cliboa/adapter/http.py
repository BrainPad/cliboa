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
from abc import ABC, abstractmethod
from time import sleep

import requests
from requests.exceptions import HTTPError

from cliboa.scenario.validator import EssentialParameters
from cliboa.util.lisboa_log import LisboaLog


class FormAuth(object):
    """
    form authentication
    """

    def __init__(self):
        self._form_url = None
        self._form_id = None
        self._form_password = None

    @property
    def form_url(self):
        return self._form_url

    @form_url.setter
    def form_url(self, form_url):
        self._form_url = form_url

    @property
    def form_id(self):
        return self._form_id

    @form_id.setter
    def form_id(self, form_id):
        self._form_id = form_id

    @property
    def form_password(self):
        return self._form_password

    @form_password.setter
    def form_password(self, form_password):
        self._form_password = form_password

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._form_url, self._form_id, self._form_password],
        )
        valid()


class Http(ABC):
    """
    Http client abstract class
    """

    def __init__(self, url, dest_path, timeout, retry_cnt, retry_intvl_sec, params):
        self._logger = LisboaLog.get_logger(__name__)
        self._url = url
        self._dest_path = dest_path
        self._timeout = timeout
        self._retry_cnt = retry_cnt
        self._retry_intvl_sec = retry_intvl_sec
        self._params = params

    @abstractmethod
    def execute(self):
        pass


class Download(Http):
    """
    Download the specific file via simple Http GET
    """

    VALID_HTTP_STATUS = 200

    def __init__(
        self, url, dest_path, timeout, retry_cnt=2, retry_intvl_sec=10, query_string=None, **params
    ):
        # params is the **kwargs argument of request.get()
        super().__init__(url, dest_path, timeout, retry_cnt, retry_intvl_sec, params)
        # only when using request.get()
        self._query_string = query_string

    def execute(self):
        self._logger.info("Http GET url: %s" % self._url)
        self._logger.info("local path: %s" % self._dest_path)
        res = None
        for _ in range(self._retry_cnt):
            try:
                res = requests.get(
                    self._url, params=self._query_string, timeout=self._timeout, **self._params
                )
                res.raise_for_status()
                with open(self._dest_path, "wb") as f:
                    f.write(res.content)
                return

            except Exception as e:
                self._logger.warning(e)

            self._logger.warning(
                "Unexpected error occurred during http get. Retry will start in %s"
                % self._retry_intvl_sec
            )
            sleep(self._retry_intvl_sec)

        if res is not None:
            raise HTTPError("Http request failed. HTTP Status code: %s" % res.status_code)

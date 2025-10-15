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
import json
from abc import ABC, abstractmethod
from time import sleep

import requests
from requests.exceptions import HTTPError

from cliboa.util.log import _get_logger

VALID_HTTP_STATUS = 200


class Http(ABC):
    """
    Http client abstract class
    """

    def __init__(self, url, dest_path, timeout, retry_cnt, retry_intvl_sec, params):
        self._logger = _get_logger(__name__)
        self._url = url
        self._dest_path = dest_path
        self._timeout = timeout
        self._retry_cnt = retry_cnt
        self._retry_intvl_sec = retry_intvl_sec
        self._params = params

    @abstractmethod
    def request(self):
        pass

    def execute(self):
        self._logger.info("local path: %s" % self._dest_path)
        res = None
        for _ in range(self._retry_cnt):
            try:
                res = self.request()
                res.raise_for_status()
                with open(self._dest_path, "wb") as f:
                    f.write(res.content)
                return

            except Exception as e:
                self._logger.warning(e)

            self._logger.warning(
                "Unexpected error occurred during http request. Retry will start in %s",
                self._retry_intvl_sec,
            )
            sleep(self._retry_intvl_sec)

        if res is not None:
            raise HTTPError("Http request failed. HTTP Status code: %s" % res.status_code)
        else:
            raise HTTPError("Http request failed after all retries.")


class Download(Http):
    """
    Download via simple Http GET
    """

    def __init__(
        self, url, dest_path, timeout, retry_cnt=2, retry_intvl_sec=10, query_string=None, **params
    ):
        # params is the **kwargs argument of request.get()
        super().__init__(url, dest_path, timeout, retry_cnt, retry_intvl_sec, params)
        # only when using request.get()
        self._query_string = query_string

    def execute(self):
        super().execute()

    def request(self):
        self._logger.info("Http GET url: %s" % self._url)
        return requests.get(
            self._url, params=self._query_string, timeout=self._timeout, **self._params
        )


class Upload(Http):
    """
    Upload via simple Http POST
    """

    def __init__(self, url, dest_path, timeout, retry_cnt=2, retry_intvl_sec=10, **params):
        # params is the **kwargs argument of request.post()
        super().__init__(url, dest_path, timeout, retry_cnt, retry_intvl_sec, params)

    def execute(self):
        super().execute()

    def request(self):
        self._logger.info("Http POST url: %s" % self._url)
        return requests.post(
            self._url,
            timeout=self._timeout,
            headers=self._params["headers"],
            data=json.dumps(self._params["data"]),
        )


class Update(Http):
    """
    Update via simple Http PUT
    """

    def __init__(self, url, dest_path, timeout, retry_cnt=2, retry_intvl_sec=10, **params):
        # params is the **kwargs argument of request.put()
        super().__init__(url, dest_path, timeout, retry_cnt, retry_intvl_sec, params)

    def execute(self):
        super().execute()

    def request(self):
        self._logger.info("Http PUT url: %s" % self._url)
        return requests.put(
            self._url,
            timeout=self._timeout,
            headers=self._params["headers"],
            data=json.dumps(self._params["data"]),
        )


class Remove(Http):
    """
    Remove via simple Http DELETE
    """

    def __init__(self, url, dest_path, timeout, retry_cnt=2, retry_intvl_sec=10, **params):
        # params is the **kwargs argument of request.delete()
        super().__init__(url, dest_path, timeout, retry_cnt, retry_intvl_sec, params)

    def execute(self):
        super().execute()

    def request(self):
        self._logger.info("Http DELETE url: %s" % self._url)
        return requests.delete(self._url, timeout=self._timeout, **self._params)

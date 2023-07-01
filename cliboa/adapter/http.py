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
import os
from abc import ABC, abstractmethod
from time import sleep

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError

from cliboa.adapter.http import Download, Remove, Update, Upload
from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.lisboa_log import LisboaLog

VALID_HTTP_STATUS = 200


class HttpBase(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_url = None
        self._dest_dir = None
        self._dest_name = None
        self._timeout = 30
        self._retry_count = 3
        self._retry_intvl_sec = 10
        self._basic_auth = False
        self._user = None
        self._password = None

    def src_url(self, src_url):
        self._src_url = src_url

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def dest_name(self, dest_name):
        self._dest_name = dest_name

    def timeout(self, timeout):
        self._timeout = timeout

    def retry_count(self, retry_count):
        self._retry_count = retry_count

    def retry_intvl_sec(self, retry_intvl_sec):
        self._retry_intvl_sec = retry_intvl_sec

    def basic_auth(self, basic_auth):
        self._basic_auth = basic_auth

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def execute(self, *args):
        pass

    def get_params(self):
        if self._basic_auth:
            return {"auth": HTTPBasicAuth(self._user, self._password)}
        return {}


class HttpGet(HttpBase):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        d = Download(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            **super().get_params(),
        )
        d.execute()


class HttpPost(HttpBase):
    def __init__(self):
        super().__init__()
        self._payload = {}
        self._headers = {"content-type": "application/json"}

    def payload(self, payload):
        self._payload = payload

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        u = Upload(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            **super().get_params(),
            data=self._payload,
            headers=self._headers,
        )
        u.execute()


class HttpPut(HttpBase):
    def __init__(self):
        super().__init__()
        self._payload = {}
        self._headers = {"content-type": "application/json"}

    def payload(self, payload):
        self._payload = payload

    def headers(self, headers):
        self._headers = headers

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        u = Update(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            **super().get_params(),
            data=self._payload,
            headers=self._headers,
        )
        u.execute()


class HttpDelete(HttpBase):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        os.makedirs(self._dest_dir, exist_ok=True)

        if self._basic_auth:
            valid = EssentialParameters(
                self.__class__.__name__,
                [self._src_url, self._dest_dir, self._dest_name, self._user, self._password],
            )
        else:
            valid = EssentialParameters(
                self.__class__.__name__, [self._src_url, self._dest_dir, self._dest_name]
            )
        valid()
        url = self._src_url
        dest_path = os.path.join(self._dest_dir, self._dest_name)

        r = Remove(
            url,
            dest_path,
            self._timeout,
            self._retry_count,
            self._retry_intvl_sec,
            **super().get_params(),
        )
        r.execute()


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
        return requests.post(self._url, timeout=self._timeout, **self._params)


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
        return requests.put(self._url, timeout=self._timeout, **self._params)


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

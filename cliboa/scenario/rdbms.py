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

from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep


class BaseRdbms(BaseStep):
    """
    Base class of relational database class.
    """

    def __init__(self):
        super().__init__()

        self._host = None
        self._dbname = None
        self._user = None
        self._password = None

    def host(self, host):
        self._host = host

    def dbname(self, dbname):
        self._dbname = dbname

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def execute(self, *args):
        valid = EssentialParameters(
            self.__class__.__name__,
            [self._host, self._dbname, self._user, self._password],
        )
        valid()

    def get_adaptor(self):
        raise NotImplementedError("get_adaptor must be implemented in a subclass")


class BaseRdbmsRead(BaseRdbms):
    def __init__(self):
        super().__init__()
        self._query = None
        self._dest_path = None
        self._encoding = "UTF-8"

    def query(self, query):
        self._query = query

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._query, self._dest_path]
        )
        valid()

        with self.get_adaptor() as ps:
            with open(
                self._dest_path, mode="w", encoding=self._encoding, newline=""
            ) as f:
                if isinstance(self._query, str):
                    self._logger.warning(
                        (
                            "DeprecationWarning: "
                            "In the near future, "
                            "the `query` will be changed to accept only dictionary types. "
                        )
                    )
                    query = super()._property_path_reader(self._query)
                else:
                    query_filepath = self._source_path_reader(self._query)
                    with open(query_filepath, "r") as qf:
                        query = qf.read()
                cur = ps.select(query)
                writer = None
                for i, row in enumerate(cur):
                    if i == 0:
                        if type(row) is tuple:
                            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                            columns = [i[0] for i in cur.description]
                            writer.writerow(columns)
                        elif type(row) is dict:
                            writer = csv.DictWriter(
                                f, list(row.keys()), quoting=csv.QUOTE_ALL
                            )
                            writer.writeheader()
                    if writer:
                        writer.writerow(self.callback_handler(row))
                    else:
                        f.write(self.callback_handler(row))

    def callback_handler(self, row):
        if type(row) is tuple:
            return list(row)
        elif type(row) is dict:
            return row
        return row

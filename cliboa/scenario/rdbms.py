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
from cliboa.util.exception import FileNotFound, InvalidParameter
from cliboa.util.rdbms_util import Rdbms_Util


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
        self._port = None

    def host(self, host):
        self._host = host

    def dbname(self, dbname):
        self._dbname = dbname

    def user(self, user):
        self._user = user

    def password(self, password):
        self._password = password

    def port(self, port):
        self._port = port

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
        self._tblname = None
        self._dest_path = None
        self._encoding = "UTF-8"

    def query(self, query):
        self._query = query

    def tblname(self, tblname):
        self._tblname = tblname

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._dest_path])
        valid()

        if (self._query and self._tblname) or (not self._query and not self._tblname):
            raise InvalidParameter("Either query or tblname is required.")

        with self.get_adaptor() as adaptor:
            with open(self._dest_path, mode="w", encoding=self._encoding, newline="") as f:
                if self._tblname:
                    query = Rdbms_Util().select_sql(self._tblname)
                elif isinstance(self._query, str):
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
                cur = adaptor.select(query)
                writer = None
                for i, row in enumerate(cur):
                    if i == 0:
                        if type(row) is tuple:
                            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                            columns = [i[0] for i in cur.description]
                            writer.writerow(columns)
                        elif type(row) is dict:
                            writer = csv.DictWriter(f, list(row.keys()), quoting=csv.QUOTE_ALL)
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


class BaseRdbmsWrite(BaseRdbms):
    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._tblname = None
        self._encoding = "UTF-8"
        self._chunk_size = 100

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def tblname(self, tblname):
        self._tblname = tblname

    def encoding(self, encoding):
        self._encoding = encoding

    def chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._tblname]
        )
        valid()

        # Plural files are allowed to insert at the same time,
        # but all files must be the same csv format.
        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            raise FileNotFound("No csv file was found.")
        self._logger.info("Files found %s" % files)

        with open(files[0], mode="r", encoding=self._encoding) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

        rdbmsUtil = Rdbms_Util()
        query = rdbmsUtil.insert_sql(self._tblname, fieldnames)
        self._logger.info("query: %s" % query)
        for file in files:
            with self.get_adaptor() as adaptor:
                tuples = rdbmsUtil.csv_as_params(
                    file, chunk_size=self._chunk_size, encoding=self._encoding
                )
                self._logger.info("tuples: %s" % tuples)
                for params in tuples:
                    self._logger.info("params: %s" % params)
                    adaptor.insert(query, params)

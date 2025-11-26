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
import os

from pydantic import BaseModel

from cliboa.scenario.base import BaseStep
from cliboa.util.exception import FileNotFound, InvalidParameter


class BaseRdbms(BaseStep):
    """
    Base class of relational database class.
    """

    class Arguments(BaseModel):
        host: str
        dbname: str
        user: str
        password: str
        port: int | None = None

    def execute(self):
        pass

    def get_adaptor(self):
        raise NotImplementedError("get_adaptor must be implemented in a subclass")

    def select_sql(self, tblname):
        """
        Create select sql.

        Arguments:
            tblname (str): table name

        Returns:
            str: SELECT * FROM {tblname}
        """
        return "SELECT * FROM %s" % tblname

    def insert_sql(self, tblname, columns):
        """
        Create insert sql from csv file.

        Arguments:
            tblname (str): table name
            columns (array): db column names

        Returns:
            str: INSERT INTO {tblname} (columns) VALUES (%s,%s,%s...)
        """
        item = ""
        for i, row in enumerate(columns):
            if i != 0:
                item = item + ","
            item = item + row
        place_holders = "%s," * (i + 1)
        return "INSERT INTO {} ({}) VALUES ({})".format(tblname, item, place_holders[:-1])

    def csv_as_params(self, path, chunk_size=100, encoding="utf-8"):
        """
        Divide csv records into small-blocked parameter.
        Basically it uses for paramer of insert query.

        Arguments:
            path (str): Csv file path
            chunk_size=100 (int): Number of row in a single block

        Returns:
            array: [
                    [(row1), (row2), (row3)]
                    [(row4), (row5), (row6)]
                    [(row7), (row8)]
                ]
        """
        tuples = []
        with open(path, mode="r", encoding=encoding) as f:
            reader = csv.reader(f)
            next(reader)  # ignore header
            rows = []
            for i, row in enumerate(reader, 1):
                rows.append(tuple(row))
                if i % chunk_size == 0:
                    tuples.append(rows)
                    rows = []
        if rows:
            tuples.append(rows)
        return tuples


class BaseRdbmsRead(BaseRdbms):
    class Arguments(BaseRdbms.Arguments):
        dest_path: str
        query: str | None = None
        tblname: str | None = None
        encoding: str = "UTF-8"

    def _property_path_reader(self, src, encoding="utf-8"):
        """
        Returns an resource contents from the path if src starts with "path:",
        returns src if not
        """
        self.logger.warning("DeprecationWarning: Will be removed in the near future")
        if src[:5].upper() == "PATH:":
            fpath = src[5:]
            if os.path.exists(fpath) is False:
                raise FileNotFound(src)
            with open(fpath, mode="r", encoding=encoding) as f:
                return f.read()
        return src

    def execute(self):
        if (self.args.query and self.args.tblname) or (
            not self.args.query and not self.args.tblname
        ):
            raise InvalidParameter("Either query or tblname is required.")

        dest_dir = os.path.dirname(self.args.dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        with self.get_adaptor() as adaptor:
            with open(self.args.dest_path, mode="w", encoding=self.args.encoding, newline="") as f:
                if self.args.tblname:
                    query = self.select_sql(self.args.tblname)
                elif isinstance(self.args.query, str):
                    self.logger.warning(
                        (
                            "DeprecationWarning: "
                            "In the near future, "
                            "the `query` will be changed to accept only dictionary types. "
                        )
                    )
                    query = self.args.property_path_reader(self.args.query)
                else:
                    query_filepath = self.args.source_path_reader(self.args.query)
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
    class Arguments(BaseRdbms.Arguments):
        src_dir: str
        src_pattern: str
        tblname: str
        encoding: str = "UTF-8"
        chunk_size: int = 100

    def execute(self):
        # Plural files are allowed to insert at the same time,
        # but all files must be the same csv format.
        files = super().get_target_files(self.args.src_dir, self.args.src_pattern)
        if len(files) == 0:
            raise FileNotFound("No csv file was found.")
        self.logger.info("Files found %s" % files)

        with open(files[0], mode="r", encoding=self.args.encoding) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

        query = self.insert_sql(self.args.tblname, fieldnames)
        self.logger.info("query: %s" % query)
        for file in files:
            with self.get_adaptor() as adaptor:
                tuples = self.csv_as_params(
                    file, chunk_size=self.args.chunk_size, encoding=self.args.encoding
                )
                self.logger.info("tuples: %s" % tuples)
                for params in tuples:
                    self.logger.info("params: %s" % params)
                    adaptor.insert(query, params)

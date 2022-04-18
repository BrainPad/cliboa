import csv


class Rdbms_Util(object):
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

    def delete_sql(self, *args):
        # TODO Not implemented yet
        raise Exception("Not implemented yet")

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

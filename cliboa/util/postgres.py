
from cliboa.util.lisboa_log import LisboaLog

class PostgresAdapter(object):
    """
    Adapter class of PostgreSQL
    """

    def __init__(self):
        self._logger = LisboaLog.get_logger(__name__)
        self.__cur = None
        self.__con = None

    def connect(self,host,user,dbname,password):
        """
        Get postgres connection
        """
        
        self.__con = psycopg2.connect(host= host , user=user,dbname=dbname, password= password)
        self.__con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return self.__con
    

    def close(self):
        """
        Release sqlite connection
        """
        
        if self.__cur:
            self.__cur.close()
        
            self.__cur = None

        if self.__con:
            
            self.__con.close()

    def fetch(self, sql, row_factory=None):
        """
        Get cursor after executed query of select-related
        Args:
            sql (str): SQL
        Returns:
            cursor: cursor
        
        """
        
        if self.__cur:
            self.__cur.close()
            self.__cur = None
        
        self.__cur = self.__con.cursor()
      
        if row_factory:
            self.__cur.row_factory = row_factory
        self.__cur.execute(sql)
        return self.__cur
    
    def create_user_func(self, dict):
        """
        Create function
        """
        
        self.__con.create_function(dict["name"], dict["args"], dict["func"])

    def execute(self, sql):
        """
        Execute SQL
        Args:
            sql (str): SQL to execute
        """
    
        self.__con.execute(sql)

    def commit(self):
        """
        Commit
        """
        self.__con.commit()

    def execute_many_insert(
        self, tblname, column_def, insert_rows, is_replace_into=True
    ):
        """
        Execute many INSERT INTO SQL
        Args:
            tblname: target table
            column_def: table column definition
            insert_rows(dict[])  rows to be inserted
            is_replace_into: when using replace into: True,
                             when using insert into: False
        """
        if not tblname or not insert_rows:
            raise ValueError("Parameters are missing")
        
        holders = "%s " * len(column_def)
        
        insert_sql = "REPLACE INTO" if is_replace_into else "INSERT INTO"
        numbers_of_columns = int(len(holders) / 3)
        sql = insert_sql + " %s (%s) VALUES (%s)" % (
            tblname,
            # escape all the columns with double quotes
            ",".join('"' + column + '"' for column in column_def),
            ",".join(list(holders.split(' ',numbers_of_columns)[0:numbers_of_columns]))
        )
        self._logger.debug("sql: %s" % sql)
        values = []
        for row in insert_rows:
            vs = []
            for c in column_def:
                vs.append(row.get(c))
            values.append(vs)
            if len(vs) != len(column_def):
                raise ValueError(
                    "The length of insert rows must be equal to the column definition. Column definition: %s, Insert rows: %s"  # noqa
                    % (column_def, vs)
                )
           
        self.__con.cursor().executemany(sql, values)
  
    def add_index(self, tblname, columns):
        """
        Create index
        Args:
            tblname (str): target table
            columns (str[]): index list
        """
        idx_name = tblname + "_" + "".join(columns)
        sql = "CREATE INDEX %s ON %s(%s)" % (idx_name, tblname, ",".join(columns))
        self.execute(sql)

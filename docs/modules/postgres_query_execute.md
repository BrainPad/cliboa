# SqliteQueryExecute
Execute query against postgres table. Basically specify insert or update queries.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|PostgreSQL Server Address|Yes|None
|dbname|PostgreSQL database name|Yes|None||
|tblname|PostgreSQL table name to read|Yes|None||
|password|PostgreSQL Server access password|Yes|None|
|columns|Columns of table to read|No|None|If not specified, read all the columns.|
|raw_query|Raw query to execute against PostgreSQL table|No|None||
|port|PostgreSQL access port|No|None|
|
# Examples
```
scenario:
step:
  class: PostgresQueryExecute
  arguments:
    host: localhost
    user: postgres
    dbname: postgres
    tblname:  test
    password: password
    raw_query: DELETE FROM test_tbl
```

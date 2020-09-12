# PostgresRead
Read record from postgreSQL table. Basically use together with an load module.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|io|Types of I/O. Must specify 'input'.|Yes|None||
|host|PostgreSQL Server Address|Yes|None
|dbname|PostgreSQL database name|Yes|None||
|tblname|PostgreSQL table name to read|Yes|None||
|password|PostgreSQL Server access password|Yes|None|
|columns|Columns of table to read|No|None|If not specified, read all the columns.|
|raw_query|Raw query to execute against PostgreSQL table|No|None||
|port|PostgreSQL access port|No|None|

# Examples
```
scenario:
- step:
  class: PostgresRead
  arguments:
    io: input
    host: localhost
    user: postgres
    dbname: postgres
    tblname: tblname
    raw_query: SELECT * FROM test
    password: password
    
- step:
  class: CsvWrite
  io: output
  arguments:
    dest_path: /usr/local/sample/test_{{ today }}_.csv
    with_vars:
     today: date '+% Y%m%d'
```
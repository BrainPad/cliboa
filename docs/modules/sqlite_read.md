# SqliteRead
deprecated
Please do not use this class.

Read record from sqlite table. Basically use together with an load module.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|io|Types of I/O. Must specify 'input'.|Yes|None||
|dbname|Sqlite database name|Yes|None||
|tblname|Sqlite table name to read|Yes|None||
|columns|Columns of table to read|No|None|If not specified, read all the columns.|
|raw_query|Raw query to execute against sqlite table|No|None||

# Examples
```
scenario:
# Read sqlite table and write result as a csv file.
- step:
  class: SqliteRead
  io: input
  arguments:
    dbname: test.db
    tblname: test_tbl
    columns:
      - column1
      - column2
- step:
  class: CsvWrite
  io: output
  arguments:
    dest_path: /usr/local/test_{{ today }}_.csv
    with_vars:
      today: date '+%Y%m%d'

# Execute row query and  write result as a csv file.
- step:
  class: SqliteRead
  io: input
  arguments:
    dbname: test.db
    tblname: test_tbl
    raw_query: SELECT * FROM test_tbl
- step:
  class: CsvWrite
  io: output
  arguments:
    dest_path: /usr/local/test_{{ today }}_.csv
    with_vars:
      today: date '+%Y%m%d'
```
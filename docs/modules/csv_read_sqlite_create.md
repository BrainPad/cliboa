# SqliteCreation
Read content from a file and insert it into a table of sqlite.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of a file |Yes|None||
|src_pattern|File pattern to read. Regexp is available.|Yes|None||
|dbname|Sqlite database name|Yes|None||
|tblname|Sqlite table name to insert|Yes|None||
|columns|Columns of table to insert|No|None|If not specified, insert contents against all the columns.|

# Examples
```
# Read content from a csv file and insert into sqlite table 
scenario:
- step:
  class: CsvReadSqliteCreate
  arguments:
    src_dir: /tmp
    src_pattern: (.*)\.csv
    dbname: test.db
    tblname: test_tbl
    columns:
      - column1
      - column2
```

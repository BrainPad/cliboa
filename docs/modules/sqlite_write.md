# SqliteWrite
Read content from csv files and insert them into sqlite table.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of a file |Yes|None||
|src_pattern|File pattern to read. Regexp is available.|Yes|None||
|dbname|Sqlite database name|Yes|None||
|tblname|Sqlite table name to insert|Yes|None||
|primary_key| Primary key                                                  |No|None||
|index|Index array|No|[]||
|refresh|Drop table in advance if True.<br />If False, use existence table.|No|True||
|force_insert|If True, plural csv files with different format is allowed to insert.<br />Raise error if False.|No|False|If columns are different between csv files, missing columns will be created to sqlite table automatically, but values are empty|

# Examples
```
# Read content from csv files and insert them into sqlite table
scenario:
- step:
  class: SqliteWrite
  arguments:
    src_dir: /tmp
    src_pattern: (.*)\.csv
    dbname: test.db
    tblname: test_tbl
```

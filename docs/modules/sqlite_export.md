# SqliteExport
Export a table data to csv.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|dbname|Sqlite database name|Yes|None||
|tblname|Sqlite table name to insert|Yes|None||
|dest_path|File path for export data|Yes|None||
|encoding|Encoding|No|utf-8||
|order|orders of exporting data|No|[]|Define db column names if orders are required|
|no_duplicate|Remove duplicate records|No|False|If true is set, get data with "select distinct"|
|vacuum|Vacuum is performed after query processing is finished.|No|False||

# Examples
```
# Read content from csv files and insert them into sqlite table
scenario:
- step:
  class: SqliteExport
  arguments:
    dbname: test.db
    tblname: test_tbl
    dest_path: usr/local/path/result.csv
```

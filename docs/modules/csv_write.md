# CsvWrite
Write a csv file. Basically use together with an extract module.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|io|Types of I/O. Must specify 'output'.|Yes|None||
|dest_path|An absolute path of a csv file to read. Regexp is available.|Yes|None||
|encoding|Character encoding when read a csv file.|No|utf-8||
|mode|File write mode. Can specify any one of w, a, w+ and a+. Same as the C standard library [foepn](http://www.manpagez.com/man/3/fopen/). |No|a||

# Examples
```
scenario:
# Read sqlite table and write result as a csv file.
- step:
  class: SqliteRead
  io: input
  arguments:
    dbname: test_db
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
```
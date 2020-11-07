# PostgresCreation
Read content from a file and insert insert it into a table of PostgreSQL.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of a file |Yes|None||
|src_pattern|File pattern to read. Regexp is available.|Yes|None|
|host|PostgreSQL Server Address|Yes|None||
|dbname|PostgreSQL database name|Yes|None||
|tblname|PostgreSQL table name to read|Yes|None||
|password|PostgreSQL Server access password|Yes|None|
|columns|Columns of table to read|No|None|If not specified, read all the columns.|
|raw_query|Raw query to execute against PostgreSQL table|No|None||
|port|PostgreSQL access port|No|None|
|columns|Columns of table to insert|No|None|If not specified, insert contents against all the columns.|

# Examples
```
# Read content from a csv file and insert into postgres table 
scenario:
- step:
  class: CsvReadPostgresCreate
  arguments:
    host: localhost
    user: postgres
    dbname: postgres
    tblname: test
    password: password
    src_dir: /usr/local
    src_pattern: test.csv
    columns:
      - column1
      - column2
```

# PostgresqlWrite
Read csv files and import them into Postgresql.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|host name or IP address to connect the database.|Yes|None||
|dbname|name of the database to use.|Yes|None||
|user|User name to login the database|Yes|None||
|password|Password to login the database|Yes|None||
|port|Port number for the database|No|5432||
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|tblname|Table name to be imported|Yes|None||
|encoding|Character encoding of csv files|No|UTF-8||
|chunk_size|Number of records to be imported at once|100||


# Examples
```
scenario:
- step: postgresql_insert
  class: PostgresqlWrite
  arguments:
    host: dummy_host
    dbname: sample_db
    user: root
    src_dir: /user/local/data
    src_pattern: query.*\/csv
    tblname: SAMPLE_TABLE
```
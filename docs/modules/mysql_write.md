# MysqlWrite
Read csv files and import them into Mysql.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|host name or IP address to connect the database.|Yes|None||
|dbname|name of the database to use.|Yes|None||
|user|User name to login the database|Yes|None||
|password|Password to login the database|Yes|None||
|port|Port number for the database|No|3306||
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|tblname|Table name to be imported|Yes|None||
|encoding|Character encoding of csv files|No|UTF-8||
|chunk_size|Number of records to be imported at once|No|100||


# Examples
```
scenario:
- step: my_sql_insert
  class: MysqlWrite
  arguments:
    host: dummy_host
    dbname: sample_db
    user: root
    src_dir: /user/local/data
    src_pattern: query.*\/csv
    tblname: SAMPLE_TABLE
```
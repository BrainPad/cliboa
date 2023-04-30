# MysqlRead
Execute a query to MySql server and get result as csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|host name or IP address to connect the database.|Yes|None||
|dbname|name of the database to use.|Yes|None||
|user|User name to login the database|Yes|None||
|password|Password to login the database|Yes|None||
|port|Port number for the database|No|3306||
|query|Sql query string or file path for sql query|Yes|None|Either query or tblname is required|
|tblname|Table name to be export records|Yes|None|Either query or tblname is required|
|dest_path|File path which result of the query will be written|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Result file encoding|No|UTF-8||


# Examples
```
scenario:
- step: my_sql_select
  class: MysqlRead
  arguments:
    host: dummy_host
    dbname: sample_db
    user: root
    password: pass
    query:
      content: SELECT * FROM SAMPLE_TABLE
    dest_path: /user/local/data/query_result.csv

scenario:
- step: my_sql_select
  class: MysqlRead
  arguments:
    host: dummy_host
    dbname: sample_db
    user: root
    password: pass
    query:
      file: /user/local/sql/select.sql
    dest_path: /user/local/data/query_result.csv

scenario:
- step: my_sql_select
  class: MysqlRead
  arguments:
    host: dummy_host
    dbname: sample_db
    user: root
    password: pass
    tblname: SAMPLE_TABLE
    dest_path: /user/local/data/query_result.csv
```
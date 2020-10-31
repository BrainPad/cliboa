# MysqlRead
Execute a query to MySql server and get result as csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|host name or IP address to connect the database.|Yes|None||
|dbname|name of the database to use.|Yes|None||
|user|User name to login the database|Yes|None||
|password|Password to login the database|Yes|None||
|query|Sql query string or file path for sql query|Yes|None|When file path was given, prefix "PATH:" must be added before the path|
|dest_path|File path which result of the query will be written|Yes|None||
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
    query: "PATH:/user/local/data/query.sql"
    dest_path: /user/local/data/query_result.csv
```
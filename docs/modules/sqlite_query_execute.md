# SqliteQueryExecute
Execute query against sqlite table. Basically specify insert or update queries.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|dbname|sqlite database name to read|Yes|None||
|raw_query|Raw query to execute against sqlite table|Yes|None||
|vacuum|Vacuum is performed after query processing is finished.|No|False||

# Examples
```
scenario:
- step: truncate record
  class: SqliteQueryExecute
  arguments:
    dbname: test.db
    raw_query: DELETE FROM test_tbl
```

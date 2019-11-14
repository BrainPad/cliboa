# SqliteReadRow
Execute query and call result handler.

Create a sub-class and inherit from this class.
All subclasses must be implemented those 2 methods.

1. _get_query(self): Must return SQL statement.

2. _callback_handler(self, cursor): This method will be called after SQL executed.

In addition, you can change result type by inheriting additional method.

1. _get_factory(self): Follow the instructions below

 https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory


# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|dbname|Sqlite database name|Yes|None||

# Examples
```
scenario:
- step: Execute select query.
  class: SqliteReadRowSubClass
  arguments:
    dbname: sample.db
```

```
# Implement example
class SqliteReadRowSubClass(SqliteReadRow):
  def _get_query(self):
    return "SELECT * FROM TEMP"

  def _callback_handler(self, cursor):
    for row in cursor:
      print(row)

```
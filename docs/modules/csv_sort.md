# CsvSort
This class allows you to sort large csv that doesn't fit in memory.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that csv file exist|Yes|None||
|src_pattern|File pattern of csv file. Regexp is available.|Yes|None||
|dest_dir|Destination directory for sorted csv files|Yes|None||
|encoding|Character encoding of csv files|No|utf-8||
|order|Csv column names to sort|Yes|[]|Add "desc" to the column name if reverse orders are required|
|quote|quoting for csv file|No|QUOTE_MINIMAL| One of the followings [QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONNUMERIC, QUOTE_NONE]|
|no_duplicate|Whether duplicate records will be removed|No|False||

# Examples
```
scenario:
- step: Sort a large csv
  class: CsvSort
  arguments:
    src_dir: /input
    src_pattern: foo\.csv
    dest_dir: /output
    quote: QUOTE_ALL
    order:
      - id
      - name desc
```
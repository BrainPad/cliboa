# CsvSort
This class allows you to sort large csv that doesn't fit in memory.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding of csv files|No|utf-8||
|order|Csv column names to sort|Yes|[]|Add "desc" to the column name if reverse orders are required|
|quote|quoting for csv file|No|QUOTE_MINIMAL| One of the followings [QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONNUMERIC, QUOTE_NONE]|
|no_duplicate|Whether duplicate records will be removed|No|False||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: Sort a large csv
  class: CsvSort
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    order:
      - id desc

Input: /in/test.csv
id, name
1, one
3, three
2, two

Output: /out/test.csv
id, name
3, three
2, two
1, one
```
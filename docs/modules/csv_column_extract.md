# CsvColumnExtract
Extract specific columns from csv files.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None||
|dest_dir|Destination directory to convert|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||
|columns|Columns that remains for new csv file|No|None|Specify either columns or column_num is essential.|
|column_numbers|Column numbers that remains for new csv file|No|None|Can specify several column number by comma. Specify 1 as the first column number.|


# Example 1
```
scenario:
- step:
  class: CsvColumnExtract
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_dir: /tmp
    columns:
      - column1
      - column2
```

# Example 2
```
scenario:
- step:
  class: CsvColumnExtract
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_dir: /tmp
    column_numbers: 1,3
```

# CsvColsExtract
Remove specific columns from a csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None||
|dest_path|Destination path to convert|Yes|None|
|encoding|Character encoding when read and write|No|utf-8||
|columns|Columns that remains for new csv file|Yes|None||

# Examples
```
scenario:
- step:
  class: CsvColsExtract
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_path: /tmp/test_removed.csv
    columns:
      - column1
      - column2
```
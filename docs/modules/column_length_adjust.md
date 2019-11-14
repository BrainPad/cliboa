# ColumnLengthAdjust
Adjust columns of a csv or a tsv file to the specified length.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None||
|dest_path|Destination path after convert|Yes|None|
|encoding|Character encoding when read and write|No|utf-8||
|adjust|Specify columns and lengths to adjust like 'column: length'|Yes|None||

# Examples
```
- step:
  class: ColumnLengthAdjust
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_path: /tmp/test_adjusted.csv
    adjust:
      column1: 256 
      column2: 128
```

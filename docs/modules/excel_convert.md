# ExcelConvert
Convert a excel file to a csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available|Yes|None||
|dest_dir|Destination directory to convert|Yes|None|
|dest_pattern|Destination of file pattern to convert|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Convert an excel file
  class: ExcelConvert
  arguments:
    src_dir: /root
    src_pattern: test.xlsx
    dest_dir: /tmp
    dest_pattern: test.csv
```
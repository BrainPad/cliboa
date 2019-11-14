# CsvHeaderConvert
Convert headers of a csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None||
|dest_dir|Destination directory to convert|Yes|None|
|dest_pattern|Destination of file pattern to convert|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||
|headers|Specify header to convert by format like 'header before convert: header after convert'||||

# Examples
```
scenario:
- step: Convert headers of a csv file
  class: CsvHeaderConvert
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_dir: /tmp
    dest_pattern: converted_test.csv
    headers:
        - header1: converted_header1
        - header2: converted_header2
```
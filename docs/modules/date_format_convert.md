# DateFormatConvert
Convert date format of columns of a csv file to another date format.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None||
|dest_path|Destination path to conver.|Yes|None|
|encoding|Character encoding when read and write|No|utf-8||
|formatter|Date format to convert|Yes|None|Syntax is same as [strftime](https://www.programiz.com/python-programming/datetime/strftime)|
|columns|Columns to remove|Yes|None||

# Examples
```
scenario:
- step:
  class: DateFormatConvert
  arguments:
    src_dir: /tmp
    src_pattern: test.csv
    dest_path: /tmp/test_date_formatted.csv
    formatter: "%Y-%m-%d %H:%M:00"
    columns:
      - column1
      - column2
```

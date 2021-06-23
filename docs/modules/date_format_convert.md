# DateFormatConvert
Convert date format of columns of a csv file to another date format.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_path|Destination path to convert.|No|None|Deprecated.|
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding when read and write|No|utf-8||
|formatter|Date format to convert|Yes|None|Syntax is same as [strftime](https://www.programiz.com/python-programming/datetime/strftime)|
|columns|Csv column names which change the date format|Yes|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: DateFormatConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    formatter: "%Y-%m-%d %H:%M"
    columns:
      - date

Input: /in/test.csv
id, date
1, 2021/01/01 12:00:00

Output: /out/test.csv
id, date
1, 2021-01-01 12:00
```

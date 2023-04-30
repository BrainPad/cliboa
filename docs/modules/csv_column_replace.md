# CsvColumnReplace
Replace matching regular expression values for a specific column from a csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write.|No|utf-8||
|column|Specify columns to replace.|Yes|None||
|regex_pattern|Pattern when conversion.|Yes|None||
|rep_str|Converted string in the column data.|Yes|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: CsvColumnReplace
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    column: address
    regex_pattern: @aaa
    rep_str: @xyz

Input: /in/test.csv
id, name, address
1, test, test@aaa.com

Output: /out/test.csv
id, name, address
1, test, test@xyz.com
```

# CsvColumnExtract
Extract specific columns from csv files.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write|No|utf-8||
|columns|Columns that remains for new csv file|No|None|Specify either columns or column_num is essential.|
|column_numbers|Column numbers that remains for new csv file|No|None|Can specify several column number by comma. Specify 1 as the first column number.|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||


# Example 1
```
scenario:
- step:
  class: CsvColumnExtract
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    columns:
      - name

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.csv
name
one
two
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

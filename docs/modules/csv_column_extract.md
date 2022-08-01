# CsvColumnExtract
Extract specific columns from csv files.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding when read and write|No|utf-8||
|columns|Columns to keep if extracting columns. If you want to delete columns specify the columns to delete|No|None|Specify either columns or column_num is essential.|
|column_numbers|Column numbers to keep if extracting columns. If you want to delete columns specify the column numbers to delete|No|None|Can specify several column number by comma. Specify 1 as the first column number.|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||
|do_delete|When True is specified, delete the columns specified in columns or column_numbers. When False is specified, leave the columns specified in columns or column_numbers|No|False||


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

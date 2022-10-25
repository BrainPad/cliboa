# CsvColumnDelete
Delete specific columns from csv files.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|regex_pattern|Column and regular expression pair.|Yes|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Example
```
scenario:
- step:
  class: CsvColumnDelete
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    regex_pattern: '^.*_1$'

Input: /in/test.csv
col_1, col_2
1, one
2, two

Output: /out/test.csv
col_2
one
two
```
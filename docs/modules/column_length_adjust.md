# ColumnLengthAdjust
Adjust columns of a csv or a tsv file to the specified length.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_path|Destination path after convert|No|None|Deprecated.|
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding when read and write|No|utf-8||
|adjust|Specify columns and lengths to adjust like 'column: length'|Yes|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
- step:
  class: ColumnLengthAdjust
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    adjust:
      name: 3

Input: /in/test.csv
id, name
1, aaaaa
2, bbbbb

Output: /out/test.csv
id, name
1, aaa
2, bbb
```

# CsvMerge
Merge two csv files into one with join style.
This class behaves in much the same way as the method 'pandas.merge'.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which source files are placed.|Yes|None||
|src_pattern|File pattern of source files to merge. Regexp is available.|Yes|None||
|target_path|File path of target file to merge for source files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|join_on|Column name to join on|Yes|None||
|engine|Can specify to merge engine - pandas or dask|No|pandas||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Merge 2 files to 1 file
  class: CsvMerge
  arguments:
    src_dir: /in
    src_pattern: test.csv
    target_path: /in2/target.csv
    dest_dir: /out
    join_on: id

Input: /in/test.csv
id, name
1, one
2, two

Input: /in2/target.csv
id, memo
1, A
2, B

Output: /out/test.csv
id, name, memo
1, one, A
2, two, B
```

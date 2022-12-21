# CsvMerge
Merge two csv files into one with join style.
This class behaves exactly same with the method 'pandas.merge'.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src1_pattern|File pattern of source to merge. Regexp is available.|Yes|None||
|src2_pattern|File pattern of source to merge. Regexp is available.|Yes|None||
|dest_dir|Path of the directory which is for output files.|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|dest_name|Output file name|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Merge 2 files to 1 file
  class: CsvMerge
  arguments:
    src_dir: /in
    src1_pattern: test1.csv
    src2_pattern: test2.csv
    dest_dir: /out
    dest_name: merge.csv

Input: /in/test1.csv
id, name
1, one
2, two

Input: /in/test1.csv
id, memo
1, A
2, B

Output: /out/merge.csv
id, name, memo
1, one, A
2, two, B
```
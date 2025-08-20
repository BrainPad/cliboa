# CsvSplitGrouped
Split csv files, using the value of a specific column as the output filename.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which input files are places.|Yes|None||
|src_pattern|File pattern of source csv files. Regexp is available.|Yes|None||
|dest_dir|Path of the directory which is for output files.|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|key_column|Column name to use split.|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples

```
scenario:
- step: Split file by class column's value
  class: CsvSplitGrouped
  arguments:
    src_dir: /in
    src_pattern: test.csv
    dest_dir: /out
    key_column: class

Input: /in/test.csv
name, class
alpha, A
beta, B
gamma, A
delta, B
epsilon, C

Output: /out/A.csv
name, class
alpha, A
gamma, A

Output: /out/B.csv
name, class
beta, B
delta, B

Output: /out/C.csv
name, class
epsilon, C
```

# CsvMerge
Merge two csv files to one.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src1_pattern|File pattern of source to merge. Regexp is available.|Yes|None||
|src2_pattern|File pattern of source to merge. Regexp is available.|Yes|None||
|dest_dir|Destination directory to convert|Yes|None|
|dest_pattern|Destination of file pattern to convert|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Merge 2 files to 1 file
  class: CsvMerge
  arguments:
    src_dir: /root
    src_pattern1: test1.csv
    src_pattern2: test2.csv
    dest_dir: /tmp
    dest_pattern: test_merge.csv
```
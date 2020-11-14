# CsvConcat
Concat csv files.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to concat|Yes|None||
|src_pattern|File pattern of source to concat.|No|None|Specify either src_pattern or src_filenames is essential.|
|src_filenames|File names of source to concat.|No|None|Specify either src_pattern or src_filenames is essential.|
|dest_dir|Destination directory to concat|Yes|None|
|dest_pattern|Destination of file pattern to concat|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Concat files
  class: CsvConcat
  arguments:
    src_dir: /root
    src_filenames:
      - file1.csv
      - file2.csv
      - file3.csv
    dest_dir: /tmp
    dest_pattern: concatenated_file.csv
```
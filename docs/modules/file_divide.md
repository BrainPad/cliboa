# FileDivide
A file is divided to plural files by specified number of rows.
New files will be created with the name by original file name or given name which specified by arguments.
Either way index number of divided count will be added for the suffix of the new file names.

Ex. foo.txt -> [ foo.1.txt, foo.2.txt, foo.3.txt ... ]

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Target directory|Yes|None||
|src_pattern|Regex to search files.|Yes|None||
|dest_dir|Directory to output divided files|Yes|None||
|dest_pattern|Output file name|Yes|None|The original file names will be used, if this parameter is not set. Except, divided number of indexes are given to the suffix of the file.|
|divide_rows|Number of the rows of individual files after divided|Yes|None||
|header|Whether if header is added to the divided files|No|False|If True, Original file's header will be added to the all divided files.|
|encoding|Character encoding|No|utf-8|||

# Examples
```
scenario:
- step:
  class: FileDivide
  arguments:
    src_dir: /tmp
    src_pattern: .*\.csv
    dest_dir: work
    dest_pattern: divided_file.csv
    divided_rows: 1000
```

#CsvColumnSelect
Select columns in Csv file in specified order

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write|No|utf-8||
|column_order|Column order after update.|Yes|column_order is have to define target files columns.|

# Example1
```
scenario:
- step: column select
  class: CsvColumnSelect
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    column_order: ['id', 'memo', 'name', 'passwd']

Input: /in/test.csv
id, name, passwd, memo 
1, spam1, spampass1, memo1
2, spam2, spampass2, memo2

Output: /out/test.tsv
id, memo, name, passwd
1, memo1, spam1, spampass1
2, memo2, spam2, spampass2
```

# Example2
```
scenario:
- step: column select
  class: CsvColumnSelect
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    column_order: ['name', 'passwd']

Input: /in/test.csv
id, name, passwd, memo 
1, spam1, spampass1, memo1
2, spam2, spampass2, memo2

Output: /out/test.tsv
name, passwd
spam1, spampass1
spam2, spampass2
```
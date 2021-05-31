# FileDivide
A file is divided to plural files by specified number of rows.
New files will be created with the name by original file name or given name which specified by arguments.
Either way index number of divided count will be added for the suffix of the new file names.

Ex. foo.txt -> [ foo.1.txt, foo.2.txt, foo.3.txt ... ]

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|dest_pattern|Output file name|No|None|Deprecated. The original file names will be used, if this parameter is not set. Except, divided number of indexes are given to the suffix of the file.|
|divide_rows|Number of the rows of individual files after divided|Yes|None||
|header|Whether if header is added to the divided files|No|False|If True, Original file's header will be added to the all divided files.|
|encoding|Character encoding|No|utf-8|||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: FileDivide
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    divided_rows: 2
    header: True

Input: /in/test.csv
id, name
1, one
2, two
3, three
4, four
5, five

Output:
/out/test.1.csv
id, name
1, one
2, two

/out/test.2.csv
id, name
3, three
4, four

/out/test.3.csv
id, name
5, five
```

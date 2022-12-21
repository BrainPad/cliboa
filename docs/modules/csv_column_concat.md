# ColumnConcat
Concat specific columns from csv file.


# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write|No|utf-8||
|columns|target of columns to concat|Yes|None||
|dest_column_name|Output column name|Yes|None||
|sep|Separator between words to be concated|No|""||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Example
```
scenario:
- step:
  class: CsvColumnConcat
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    columns:
      - name
      - data
    dest_column_name: name_data
    sep: " "

Input: /in/test.csv
id, name, data
1, one, first
2, two, second

Output: /out/test.csv
id, name_data
1, one first
2, two second
```


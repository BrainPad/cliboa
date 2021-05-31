# CsvHeaderConvert
Convert headers of a csv file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|dest_pattern|Destination of file pattern to convert|No|None|Deprecated.|
|encoding|Character encoding when read and write|No|utf-8||
|headers|Specify header to convert by format like 'header before convert: header after convert'|Yes|[]||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: Convert headers of a csv file
  class: CsvHeaderConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    headers:
      - id: key

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.csv
key, name
1, one
2, two
```
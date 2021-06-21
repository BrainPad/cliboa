# Convert csv format
Headers, extension and encoding are changeable.
Plural files can be converted at the same time, but format of all files must be the same.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|headers|List of column names which is to be renamed|No|None|Dict of list ex. [{before_column_name1: after_column_name1}, {before_column_name2: after_column_name2}]|
|before_format|File extension before convert|Yes|None|"csv" or "tsv"|
|before_enc|File encoding before convert|Yes|utf-8||
|after_format|File extension after converted|No|Same with before_format|"csv" or "tsv"|
|after_enc|File encoding after converted|Same with before_enc|None||
|after_nl|New line for converted csv|No|LF|"LF" or "CR" or "CRLF"|
|quote|quote type for converted csv|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: format change
  class: CsvConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    before_format: csv
    before_enc: utf-8
    after_format: tsv
    quote: QUOTE_ALL

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.tsv
"key",\t"name"
"1"\t"one"
"2"\t"two"
```
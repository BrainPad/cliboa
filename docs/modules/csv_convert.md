# Convert csv format
Headers, extension and encoding are changeable.
Plural files can be converted at the same time, but format of all files must be the same.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern.|Yes|None||
|headers|List of column names which is to be renamed|No|None|Dict of list ex. [{before_column_name1: after_column_name1}, {before_column_name2: after_column_name2}]|
|before_format|File extension before convert|Yes|None|"csv" or "tsv"|
|before_enc|File encoding before convert|Yes|utf-8||
|after_format|File extension after converted|No|Same with before_format|"csv" or "tsv"|
|after_enc|File encoding after converted|Same with before_enc|None||
|after_nl|New line for converted csv|No|LF|"LF" or "CR" or "CRLF"|
|quote|quote type for converted csv|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|

# Examples
```
scenario:
- step: format change
  class: CsvConvert
  arguments:
    src_dir: /root
    src_pattern: .*\.csv
    before_format: csv
    before_enc: utf-8
    after_format: tsv
```
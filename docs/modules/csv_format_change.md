# Change csv format
Create new csv(tsv) file with given parameters.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern.|Yes|None||
|dest_dir|Directory to output converted file|Yes|None|
|dest_pattern|Output file name|Yes|None||
|before_format|File extension before convert|Yes|None|"csv" or "tsv"|
|before_enc|File encoding before convert|Yes|None||
|after_format|File extension after converted|Yes|None|"csv" or "tsv"|
|after_enc|File encoding after converted|Yes|None||
|after_nl|New line for converted csv|No|LF|"LF" or "CR" or "CRLF"|
|quote|quote type for converted csv|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|

# Examples
```
scenario:
- step: format change
  class: CsvFormatChange
  arguments:
    src_dir: /root
    src_pattern: foo\.csv
    dest_dir: /out
    dest_pattern: out.csv
    before_format: csv
    before_enc: utf-16
    after_format: csv
    after_enc: urf-8
```
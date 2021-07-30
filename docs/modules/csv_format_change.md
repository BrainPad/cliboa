# Change csv format
Create new csv(tsv) file with given parameters.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|dest_pattern|Output file name|No|None|Deprecated.|
|before_format|File extension before convert|Yes|None|"csv" or "tsv"|
|before_enc|File encoding before convert|Yes|None||
|after_format|File extension after converted|Yes|None|"csv" or "tsv"|
|after_enc|File encoding after converted|Yes|None||
|after_nl|New line for converted csv|No|LF|"LF" or "CR" or "CRLF"|
|quote|quote type for converted csv|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: format change
  class: CsvFormatChange
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    before_format: csv
    before_enc: utf-8
    after_format: tsv
    after_enc: utf-8
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
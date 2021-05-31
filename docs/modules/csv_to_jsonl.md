# CsvToJsonl
Convert csv to jsonlins.
Name of new jsonl files will be the same with original csv file names,
except only extension ".jsonl" is different.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding of csv files|No|utf-8||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: Convert csv to jsonlines
  class: CsvToJson
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.jsonl
{"id": "1", "name": "one"}
{"id": "2", "name": "two"}
```
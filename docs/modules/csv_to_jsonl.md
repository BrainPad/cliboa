# CsvToJsonl
Convert csv to jsonlins.
Name of new jsonl files will be the same with original csv file names,
except only extension ".jsonl" is different.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that csv file exist|Yes|None||
|src_pattern|File pattern of csv file. Regexp is available.|Yes|None||
|dest_dir|Destination directory for converted jsonl files|Yes|None||
|encoding|Character encoding of csv files|No|utf-8||

# Examples
```
scenario:
- step: Convert csv to jsonlines
  class: CsvToJson
  arguments:
    src_dir: /input
    src_pattern: foo\.csv
    dest_dir: /output
```
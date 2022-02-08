# CsvValueExtract
Extract a specific column from a CSV file and then replace it with a regular expression.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|column_regex_pattern|Column and regular expression pair.|Yes|None||

# Example
```
scenario:
- step: csv value extract
  class: CsvValueExtract
  arguments:
    src_dir: /in
    src_pattern: foo\.csv
    dest_dir: /out
    column_regex_pattern: {'name': [a-z]*, 'memo': [A-Z]*}

Input: /in/foo.txt
id, name, memo
1, bob, test
2, JANE, MEmo

Output: /out/foo.csv
id, name, memo
1, bob,
2, , , ME
```
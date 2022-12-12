# JsonlToCsv
Transform jsonlines to csv.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write.|No|utf-8||
|after_nl|New line for converted csv.|No|LF|"LF" or "CR" or "CRLF"|
|quote|Quote type for converted csv.|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|
|escape_char|Specify the escape character.|No|None|Parameter's quote have to be "QUOTE_NONE" when used.|

# Examples
```
scenario:
- step: jsonl transform csv
  class: JsonlToCsv
  arguments:
    src_dir: /in
    src_pattern: test\.jsonl
    dest_path: /out/result.csv

Input: /in/test.jsonl
{"id": "1", "value": "foo"}
{"id": "2", "value": [{"key": "test_key","value": 999}, {"key": 'test"01"', "value": "true"}]}

Output: /out/test.csv
id, value
1, foo
2, [{'key': 'test_key', 'value': 999}, {'key': 'test\"01\"', 'value': 'true'}]
```
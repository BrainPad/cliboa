# JsonlAddKeyValue
Add key and value to jsonlines.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|pairs|Key value pairs to add to the files.|Yes|{}|If pairs is not dict, it returns an error.|

# Examples
```
scenario:
- step: jsonl add key value
  class: JsonlAddKeyValue
  arguments:
    src_dir: /in
    src_pattern: test\.json
    dest_dir: /out
    pairs: {"number": "1", "data": "first"}

Input: /in/test.json
{"id": "1", "value": "foo"}
{"id": "2", "value": [{"key": "test_key","value": 999}, {"key": 'test"01"', "value": "true"}]}

Output: /out/test.json
{"id": "1", "value": "foo", "number": 1, "data": "first"}
{"id": "2", "value": [{"key": "test_key","value": 999}, {"key": 'test"01"', "value": "true"}], "number": "1", "data": "first"}
```

# FileExists
Check number of local files in a specified directory.

Also output logs for the meta data of the found files.

If expected file count are not equals to the number of files exists in a local directory, an error will be thrown.

If expected file count is unknown, -1 can be set to the argument[count], in which case it will succeed if more than one file is found.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|count|Expected file count|No|1||
|raise_error|Behavior when an error occurs|No|True|If false is set, step will be end with no errors|


# Examples
```
scenario:
- step: FileExists
  class: FileExists
  arguments:
    src_dir: /in
    src_pattern: .*\.csv
    count: 3
```
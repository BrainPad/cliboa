# FileConvert
Convert file encoding.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding_from|Encoding before convert|Yes|None||
|encoding_to|Encoding after converted|Yes|None||
|errors|How encoding and decoding errors are to be handled|No|None|One of the following is allowed [“strict“, “replace“, “backslashreplace“, “ignore“]|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: File encoding convert
  class: FileConvert
  arguments:
    src_dir: /root
    src_pattern: .*\.txt
    dest_dir: /tmp
    encoding_from: utf-16
    encoding_to: utf-8
    errors: ignore
```
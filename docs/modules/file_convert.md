# FileConvert
Convert file encoding.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding_from|Encoding before convert|Yes|No||
|encoding_to|Encoding after converted|Yes|No||
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
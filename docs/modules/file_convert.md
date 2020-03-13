# FileConvert
Convert file encoding.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern|Yes|None||
|dest_dir|Directory to output converted files|No|None|Overwrite with converted files, if this parameter is not set.|
|encoding_from|Encoding before convert|No|utf-8||
|encoding_to|Encoding after converted|No|utf-8||

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
```
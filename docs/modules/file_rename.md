# Rename file name
Renaming files with adding either prefix or suffix.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern.|Yes|None||
|prefix|Prefix for new file name|No|None||
|suffix|Suffix for new file name|No|None|||

# Examples
```
scenario:
- step: Rename file
  class: FileRename
  arguments:
    src_dir: /root
    src_pattern: foo.*\.txt
    prefix: temp_
```
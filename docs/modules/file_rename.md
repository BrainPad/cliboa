# Rename file name
Renaming files with adding either prefix or suffix.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|prefix|Prefix for new file name|No|None||
|suffix|Suffix for new file name|No|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: Rename file
  class: FileRename
  arguments:
    src_dir: /root
    src_pattern: foo\.txt
    prefix: PRE_
    suffix: _SUF

Input: /root/foo.txt
Output: /root/PRE_foo_SUF.txt
```
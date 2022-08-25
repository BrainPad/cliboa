# Rename file name
Renaming files with adding either prefix or suffix.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|`""`||
|src_pattern|Regex which is to find target files.|Yes|`""`||
|prefix|Prefix for new file name|No|`""`||
|suffix|Suffix for new file name|No|`""`||
|regex_pattern|Pattern when conversion.|No|None|Fails if both regex_pattern and dest_str are not defined.|
|rep_str|Converted string in the file name.|No|None|Fails if both regex_pattern and dest_str are not defined.|
|ext|Converted file extension.|No|`""`|Only change the extension, not the file format.|
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
    regex_pattern: f
    rep_str: h
    ext: csv

Input: /root/foo.txt
Output: /root/PRE_hoo_SUF.csv

scenario:
- step: Rename file (deletion of character)
  class: FileRename
  arguments:
    src_dir: /root
    src_pattern: foo_delete\.txt
    regex_pattern: _delete
    rep_str: ""

Input: /root/foo_delete.txt
Output: /root/foo.csv
```

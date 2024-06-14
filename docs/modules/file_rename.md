# Rename file name
Renaming files with adding either prefix or suffix.

# Parameters
| Parameters    | Explanation                                                                   | Required | Default | Remarks                                                                                                                                                                                                  |
|---------------|-------------------------------------------------------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| src_dir       | Path of the directory which target files are placed.                          | Yes      | None    |                                                                                                                                                                                                          |
| src_pattern   | Regex which is to find target files.                                          | Yes      | None    |                                                                                                                                                                                                          |
| prefix        | Prefix for new file name                                                      | No       | `""`    |                                                                                                                                                                                                          |
| suffix        | Suffix for new file name                                                      | No       | `""`    |                                                                                                                                                                                                          |
| regex_pattern | Pattern when conversion.                                                      | No       | None    | Fails if both regex_pattern and rep_str are not defined.                                                                                                                                                 |
| rep_str       | Converted string in the file name.                                            | No       | None    | Fails if both regex_pattern and rep_str are not defined.                                                                                                                                                 |
| ext           | Converted file extension.                                                     | No       | `""`    | Only change the extension, not the file format.                                                                                                                                                          |
| nonfile_error | Whether an error is thrown when files are not found in src_dir.               | No       | False   |                                                                                                                                                                                                          |
| without_ext   | Include the extension in the range when converting using regular expressions. | No       | False   | Fails if without_ext is True, only one of the regex_pattern or rep_str are specified. Fails if without_ext is False, both regex_pattern and rep_str are not defined, and specify prefix and suffix, ext. |

# Example1
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
```

# Example2
```
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

# Example3
```
scenario:
- step: Rename file（without distinguishing extension）
  class: FileRename
  arguments:
    src_dir: /root
    src_pattern: foo\.1\.txt
    regex_pattern: \.1\.txt
    rep_str: _1.txt
    without_ext: False

Input: /root/foo.1.txt
Output: /root/foo_1.txt
```

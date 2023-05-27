# Rename file name
Copy the specified file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|Yes|None|If this parameter is not set, no action is taken. If a nonexistent directory path is specified, the directory will be created automatically.|

# Examples
```
scenario:
- step: copy file
  class: FileCopy
  arguments:
    src_dir: /in
    src_pattern: foo\.txt
    dest_dir: /out

Input: 
/in/foo.txt
This is test.

Output: 
/in/foo.txt
This is test.

/out/foo.txt
This is test.
```
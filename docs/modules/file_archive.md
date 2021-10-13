# FileArchive
Create an archive object(zip, tar)

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|dest_name|Output file name|Yes|None||
|format|Archive type|Yes|None|One of the followings are allowed [zip, tar]|
|create_dir|Whether create the same directory name for the root path|No|False||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: FileArchive
  class: FileArchive
  arguments:
    src_dir: /in
    src_pattern: test.*\.txt
    dest_dir: /out
    dest_name: archive
    format: zip

Input:
/in/test1.csv
/in/test2.csv

Output: /out/archive.zip
```
# FileArchive
Create an archive object(zip, tar)

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern|Yes|None||
|dest_dir|Output directory|No|None||
|dest_pattern|Output file name|Yes|None||
|format|Archive type|Yes|None|One of the followings are allowed [zip, tar]|
|create_dir|Whether create the same directory name for the root path|No|False||


# Examples
```
scenario:
- step: FileArchive
  class: FileArchive
  arguments:
    src_dir: /root
    src_pattern: .*\.txt
    dest_dir: /tmp
    dest_pattern: out
    format: zip
```
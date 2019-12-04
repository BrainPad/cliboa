# FileCompress
Compress files by any of the following compression type
.gz, .zip, .bz2

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Target directory|Yes|None||
|src_pattern|Regex to search files.|Yes|None||
|dest_dir|Directory to output compressed files|No|None|Compressed files are created in the same directory of the un-compressed files, if this parameter is not set.|
|format|Any of the following. [gz(gzip), bz2(bzip2), zip]|Yes|None||
|encoding|Character encoding|No|utf-8||

# Examples
```
scenario:
- step:
  class: FileCompress
  arguments:
    src_dir: /tmp
    src_pattern: .*\.csv
    ext: zip
```

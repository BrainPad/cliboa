# FileDecompress
Decompress a file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None|Only supports gz(gzip), bz2(bzip2), zip, and tar as extention.|
|dest_dir|Destination directory to convert|Yes|None|
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step:
  class: FileDecompress
  arguments:
    src_dir: /tmp
    src_pattern: test.csv.gz
    dest_dir: /tmp
```

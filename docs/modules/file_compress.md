# FileCompress
Compress files by any of the following compression type
.gz, .zip, .bz2

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Target directory|Yes|None||
|src_pattern|Regex to search files.|Yes|None||
|dest_dir|Directory to output compressed files|No|None|Compressed files are created in the same directory of the un-compressed files, if this parameter is not set.If a non-existent directory path is specified, the directory is automatically created.|
|format|Any of the following. [gz(gzip), bz2(bzip2), zip]|Yes|None||
|chunk_size|The chunk size bytes, to be used for decompressing data streams that won’t fit into memory at once.|No|1048576||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: FileCompress
  arguments:
    src_dir: /tmp
    src_pattern: .*\.csv
    format: zip
```

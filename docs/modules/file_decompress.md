# FileDecompress
Decompress a file.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of source to convert|Yes|None||
|src_pattern|File pattern of source to convert. Regexp is available.|Yes|None|Only supports gz(gzip), bz2(bzip2), zip, and tar as extension.|
|dest_dir|Destination directory to convert|No|None|If not specified, the path is the same as src_dir.If a non-existent directory path is specified, the directory is automatically created.|
|password|Password used to decompress the compressed file.|No|None|Only support zip extension.|
|encoding|Character encoding when read and write|No|utf-8||
|chunk_size|The chunk size bytes, to be used for decompressing data streams that won’t fit into memory at once.|No|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

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

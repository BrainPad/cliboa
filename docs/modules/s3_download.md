# GcsDownload
Download files from S3.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|region|AWS available zone|No|asia-northeast1||
|access_key|AWS access key|No|None||
|secret_key|AWS secret key|No|None||
|profile|Profile name|No|None|Section name of ~/.aws/config|
|bucket|S3 bucket name|Yes|None||
|prefix|Folder prefix used to filter blobs|No|`""`||
|delimiter|Delimiter, used with prefix to emulate hierarchy|No|`""`||
|src_pattern|File pattern of source to download. Regexp is available|Yes|None||
|dest_dir|Destination directory to download|No|`"."`||

# Examples
```
- step:
  class: S3Download
  arguments:
    region: asia-northeast1
    bucket: s3_test
    prefix: test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
```

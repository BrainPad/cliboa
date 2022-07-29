# SftpDownloadFileDelete
Remove files downloaded via class 'S3Download' from S3.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|symbol|Symbol defined against S3Download|Yes|None||

# Examples
```
scenario:
- step: s3_download
  class: S3Download
  arguments:
    region: asia-northeast1
    bucket: s3_test
    prefix: test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp

- step: Delete the csv file downloaded from localhost
  class: S3DownloadFileDelete
  symbol: s3_download
```

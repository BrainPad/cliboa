# S3DownloadFileDelete
Remove files downloaded via class 'S3Download' from S3.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|symbol|Symbol defined against S3Download|Yes|None||
|role_arn|IAM Role ARN to assume (cross-account)|No|None|Cannot be used with access_key/secret_key or profile|
|external_id|External ID for assume role|No|None|Used only when role_arn is specified|

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

- step: Delete the csv file downloaded from S3
  class: S3DownloadFileDelete
  symbol: s3_download
```

## Cross-account example
```
- step: Delete the csv file downloaded from S3 (cross account)
  class: S3DownloadFileDelete
  symbol: s3_download
  arguments:
    role_arn: arn:aws:iam::123456789012:role/CrossAccountRole
    external_id: your-external-id
```

# S3Download
Download files from S3.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|region|AWS available zone|No|asia-northeast1||
|access_key|AWS access key|No|None||
|secret_key|AWS secret key|No|None||
|profile|Profile name|No|None|Section name of ~/.aws/config|
|role_arn|IAM Role ARN to assume (cross-account)|No|None|Cannot be used with access_key/secret_key or profile|
|external_id|External ID for assume role|No|None|Used only when role_arn is specified|
|bucket|S3 bucket name|Yes|None||
|prefix|Folder prefix used to filter blobs|No|`""`||
|delimiter|Delimiter, used with prefix to emulate hierarchy|No|`""`||
|src_pattern|File pattern of source to download. Regexp is available|Yes|None||
|dest_dir|Destination directory to download|No|`"."`|If a non-existent directory path is specified, the directory is automatically created.|

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

## Cross-account example
```
- step:
  class: S3Download
  arguments:
    bucket: s3_test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
    role_arn: arn:aws:iam::123456789012:role/CrossAccountRole
    external_id: your-external-id
```

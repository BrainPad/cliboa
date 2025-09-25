# S3Upload
Upload a file to S3.

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
|key|The name of the key to upload to|No|None||
|src_dir|Directory of source to upload|Yes|None||
|src_pattern|File pattern of source to upload. Regexp is available|Yes|None||

# Examples
``` 
- step:
  class: S3Upload
  arguments:
    region: asia-northeast1
    bucket: s3_test_bucket
    key: test_key/data
    src_dir: /root
    src_pattern: test_(.*)_.csv
```

## Cross-account example
```
- step:
  class: S3Upload
  arguments:
    bucket: s3_test_bucket
    src_dir: /root
    src_pattern: test_(.*)_.csv
    role_arn: arn:aws:iam::123456789012:role/CrossAccountRole
    external_id: your-external-id
```

# S3Upload
Upload a file to S3.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|region|AWS available zone|No|asia-northeast1||
|access_key|AWS access key|No|None||
|secret_key|AWS secret key|No|None||
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

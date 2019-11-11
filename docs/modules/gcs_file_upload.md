# GcsFileUpload
Upload a file to GCS.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A file path of credential for GCP authentication|Yes|None||
|src_dir|Directory of source to upload|Yes|None||
|src_pattern|File pattern of source to upload. Regexp is available|Yes|None||
|dest_dir|Destination directory to upload|No|None||

# Examples
```
- step:
  class: GcsFileUpload
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    src_dir: /root
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
```

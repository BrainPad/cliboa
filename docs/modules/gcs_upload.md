# GcsUpload
Upload files to GCS.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A service account .json file path or a dictionary containing service account info in Google format|Yes|None||
|bucket|GCS bucket name|Yes|None||
|src_dir|Directory of source to upload|Yes|None||
|src_pattern|File pattern of source to upload. Regexp is available|Yes|None||
|dest_dir|Destination directory to upload|No|None||

# Examples
```
- step:
  class: GcsUpload
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    bucker: gcs-test-bucket
    src_dir: /root
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
```

# GcsDownload
Download files from GCS.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A service account .json file path or a dictionary containing service account info in Google format|Yes|None||
|bucket|GCS bucket name|Yes|None||
|prefix|Folder prefix used to filter blobs|No|None||
|delimiter|Delimiter, used with prefix to emulate hierarchy|No|None||
|src_pattern|File pattern of source to download. Regexp is available|Yes|None||
|dest_dir|Destination directory to download|No|None||

# Examples
```
- step:
  class: GcsDownload
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    bucket: gcs_test
    prefix: test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
```

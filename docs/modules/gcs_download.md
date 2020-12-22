# GcsDownload
Download files from GCS.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials.file|A service account .json file path|No|None||
|credentials.content|A dictionary containing service account info in Google format|No|None||
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
    credentials:
      file: /root/gcp_credential.json
    bucket: gcs_test
    prefix: test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp

- step: Embed contents of credentials at scenario.yml
  class: GcsDownload
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      content: |
        {
          "type": "service_account",
          ...
        }
    bucket: gcs_test
    prefix: test
    src_pattern: test_(.*)_.csv
    dest_dir: /tmp
```

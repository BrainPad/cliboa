# GcsDownloadFileDelete
Remove files downloaded via class 'GcsDownload'.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|symbol|Symbol defined against GcsDownload|Yes|None||

# Examples
```
scenario:
- step: gcs_download
  class: GcsDownload
  arguments:
    project_id: gcp_pj;
    location: asia-northeast1
    credentials: /root/gcp_pj.json
    bucket: gcs_bucket
    src_pattern: .*csv
    dest_dir: /tmp

- step: Delete the downloaded files
  class: GcsDownloadFileDelete
  symbol: gcs_download
```

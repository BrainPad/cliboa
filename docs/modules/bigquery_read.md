# BigQueryRead
 data from BigQuery and put them into on-memory or export to a file via GCS.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials.file|A service account .json file path|No|None||
|credentials.content|A dictionary containing service account info in Google format|No|None||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|key|the key of cache|No|None|If specified, load data saved to on-memory. Specifying either key or bucket is essential.|
|bucket|Bucket of GCS to save a temporal data|No|None|If specified, load data saved to a file via GCS. Specifying either key or bucket is essential.|
|dest_dir|Destination directory to download the file|No|None|If bucket is specified, dest_dir is essential as well. If a non-existent directory path is specified, the directory is automatically created.|
|query|Raw query to execute against table|No|None||
|filename|File name to save data fetch from BigQuery. Specifying extension is essential.|No|None||

# Examples
```
# Read contents from BigQuery table and save them as key: 'spam' to fetch.
- step:
  class: BigQueryRead
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      file: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    key: spam
```

```
# Read contents from BigQuery table and save as test.txt on the local machine.
- step:
  class: BigQueryRead
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      file: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    bucket: test
    dest_dir: tmp/test

# Embed contents of credentials at scenario.yml
- step:
  class: BigQueryRead
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      content: |
        {
          "type": "service_account",
          ...
        }
    dataset: test_dataset
    tblname: test_tbl
    bucket: test
    dest_dir: tmp/test
```

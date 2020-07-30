# BigQueryFileDownload
Execute select query and download result as a csv file. When extract data from GCS, it's based on the specification of (google.client.extract_table)[https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.extract_table.html].

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A service account .json file path or a dictionary containing service account info in Google format|Yes|None||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|bucket|Bucket of GCS to save a temporal data|Yes|None||
|dest_dir|Destination directory to download|Yes|None||
|filename|A file name to save on local|No|None||


# Examples
```
- step:
  class: BigQueryFileDownload
  io: output
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    bucket: test
    dest_dir: /tmp
    filename: test.txt
```
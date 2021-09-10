# GcsDownload
Transport files from GCS to BigQuery

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id. Please specify From and To, if different project. |Yes|None||
|location|GCP location. Please specify From and To, if different location. |Yes|None||
|credentials.file|A service account .json file path. Please specify From and To, if different credentials.|No|None||
|credentials.content|A dictionary containing service account info in Google format. Please specify From and To, if different.|No|None||
|bucket|GCS bucket name|Yes|None||
|prefix|Folder prefix used to filter blobs|No|None||
|delimiter|Delimiter, used with prefix to emulate hierarchy|No|None||
|src_pattern|File pattern of source to download. Regexp is available|Yes|None||
|has_header|Csv has header or not. Specify either True or False|No|True||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|replace|BigQuery insert mode. Specify either True or False|No|True||
|table_schema|table schema to insert. Syntax is same as table_schema of [pandas.DataFrame.to_gbq](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_gbq.html).|Yes|None||

# Examples
- same project, location, and credentials
```
- step:
  class: GcsToBigQuery
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      file: /root/gcp_credential.json
    bucket: gcs_test
    prefix: test
    src_pattern: test_(.*)_.csv
```

- different project, location, or credentials
```
- step:
  class: GcsToBigQuery
  arguments:
    project_id:
      From: test_gcp
      To: test_gcp_2
    location:
      From: asia-northeast1
      To: US
    credentials:
      file: /root/gcp_credential.json
    bucket: gcs_test
    prefix: test
    src_pattern: test_(.*)_.csv
```

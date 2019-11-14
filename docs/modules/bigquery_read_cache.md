# BigQueryReadCache
Load data from BigQuery and put them into on-memory as Pandas.dataframes.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|a file path of credential for GCP authentication|Yes|None||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|key|the key of cache|Yes|None||
|query|Raw query to execute against table|No|None||

# Examples
```
# Read contents from a big query table and save as 'spam': recorsds to fetch
- step:
  class: BigQueryReadCache
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    key: spam
```
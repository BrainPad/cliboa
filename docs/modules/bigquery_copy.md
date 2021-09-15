# BigQueryCopy
Copy Bigquery table to another table (or create a new one)

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of files |Yes|None||
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials.file|A service account .json file path|No|None||
|credentials.content|A dictionary containing service account info in Google format|No|None||
|dataset|Copy source BigQuery dataset|Yes|None||
|tblname|Copy source BigQuery table name to insert|Yes|None||
|dest_dataset|Copy destination BigQuery dataset|Yes|None||
|dest_tblname|Copy destination BigQuery table name|Yes|None||

# Examples
```
scenario:
- step:
  class: BigQueryCopy
  arguments:
    project_id: pb-intern-2021-team4
    location: asia-northeast1
    credentials:
      file: /usr/local/cliboa/key.json
    dataset: caffeine_bigquery
    tblname: covid_de_vaccine
    dest_dataset: copy_test_bigquery
    dest_tblname: dest_vacchine_tableaa
```

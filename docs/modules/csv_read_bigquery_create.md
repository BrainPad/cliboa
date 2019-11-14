# BigQueryCreate
Read content from a file and insert it into a bigquery table.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of a file |Yes|None||
|src_pattern|File pattern to read. Regexp is available.|Yes|None||
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|a file path of credential for GCP authentication|Yes|None||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|table_schema|table schema to insert. Syntax is same as table_schema of [pandas.DataFrame.to_gbq](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_gbq.html).|Yes|None||


# Examples
```
# Read content from a csv file and insert it into bigquery table.
- step:
  class: CsvReadBigQueryCreate
  arguments:
    src_dir: /tmp
    src_pattern: (.*)\.csv
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    table_schema:
      - { name: column1, type: NUMERIC }
      - { name: column2, type: STRING }
```

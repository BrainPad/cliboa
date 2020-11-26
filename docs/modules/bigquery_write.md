# BigQueryWrite
Read content from csv files and insert them into a bigquery table.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory of files |Yes|None||
|src_pattern|Csv file pattern to read. Regexp is available.|Yes|None||
|has_header|Csv has header or not. Specify either True or False|No|True||
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials.file|A service account .json file path|No|None||
|credentials.content|A dictionary containing service account info in Google format|No|None||
|dataset|BigQuery dataset|Yes|None||
|tblname|BigQuery table name to insert|Yes|None||
|replace|BigQuery insert mode. Specify either True or False|No|True||
|table_schema|table schema to insert. Syntax is same as table_schema of [pandas.DataFrame.to_gbq](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_gbq.html).|Yes|None||


# Examples
```
# Read content from a csv file and insert it into bigquery table.
- step:
  class: BigQueryWrite
  arguments:
    src_dir: /tmp
    src_pattern: (.*)\.csv
    has_header: False
    project_id: test_gcp
    location: asia-northeast1
    credentials:
      file: /root/gcp_credential.json
    dataset: test_dataset
    tblname: test_tbl
    replace: False
    table_schema:
      - { name: column1, type: NUMERIC }
      - { name: column2, type: STRING }

# Embed contents of credentials at scenario.yml
- step:
  class: BigQueryWrite
  arguments:
    src_dir: /tmp
    src_pattern: (.*)\.csv
    has_header: False
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
    replace: False
    table_schema:
      - { name: column1, type: NUMERIC }
      - { name: column2, type: STRING }
```

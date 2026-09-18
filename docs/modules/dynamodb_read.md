# DynamoDBRead
Reads data from a DynamoDB table and saves it as a CSV or JSONL file.

# Parameters
|Parameter|Description|Required|Default|Remarks|
|---------|-----------|--------|-------|-------|
|table_name|DynamoDB table name|Yes|None||
|dest_dir|Output directory|No|"." (current directory)|If a non-existent directory path is specified, it will be automatically created.|
|file_name|Output file name|Yes|None||
|file_format|Output file format|No|"csv"|Can be either "csv" or "jsonl".|
|partition_key|Partition key attribute name|No|None|If specified together with `partition_value`, a `query` operation is used instead of `scan`.|
|partition_value|Partition key value to match (equality)|No|None|Required together with `partition_key`.|
|sort_key|Sort key attribute name|No|None|Can only be specified together with `partition_key`/`partition_value`.|
|sort_value|Sort key value to match (equality)|No|None|Required together with `sort_key`.|
|region|AWS region|No|None|If not specified, the default region will be used.|
|access_key|AWS access key|No|None|If not specified, environment variables or IAM role will be used.|
|secret_key|AWS secret key|No|None|If not specified, environment variables or IAM role will be used.|
|profile|AWS profile|No|None|Section name of ~/.aws/config|

# Examples
```yaml
# Read the whole table (scan operation)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      region: us-west-2
```

```yaml
# Read items matching a partition key (query operation)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      region: us-west-2
      partition_key: user_id
      partition_value: "12345"
```

```yaml
# Read items matching a partition key and sort key (query operation)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      region: us-west-2
      partition_key: user_id
      partition_value: "12345"
      sort_key: created_at
      sort_value: "2026-01-01"
```


# Notes
- When `partition_key`/`partition_value` are specified, a `query` operation is used for more efficient and lower-cost retrieval than `scan`. Otherwise, the whole table is read via `scan`, matching the previous behavior.
- `sort_key`/`sort_value` narrow a query further, but require `partition_key`/`partition_value` to also be specified.
- Only equality conditions are supported for `partition_value`/`sort_value`. Range conditions (`<`, `>=`, `BETWEEN`, etc.) are not supported yet.
- Conversion to CSV might be complex for certain DynamoDB attribute types (sets, lists, maps, etc.).
- If the output file already exists, it will be overwritten.
- Partition and sort keys are not guaranteed to line up before other attributes.
# DynamoDBRead
Reads data from a DynamoDB table and saves it as a CSV or JSONL file.

# Parameters
|Parameter|Description|Required|Default|Remarks|
|---------|-----------|--------|-------|-------|
|table_name|DynamoDB table name|Yes|None||
|dest_dir|Output directory|No|"." (current directory)|If a non-existent directory path is specified, it will be automatically created.|
|file_name|Output file name|Yes|None||
|file_format|Output file format|No|"csv"|Can be either "csv" or "jsonl".|
|filter_conditions|Filter conditions (dictionary with equality conditions only)|No|None|If partition key is included, query operation is used; otherwise, scan operation is used.|
|region|AWS region|No|None|If not specified, the default region will be used.|
|access_key|AWS access key|No|None|If not specified, environment variables or IAM role will be used.|
|secret_key|AWS secret key|No|None|If not specified, environment variables or IAM role will be used.|
|profile|AWS profile|No|None|Section name of ~/.aws/config|

# Examples
```yaml
# Read all data from table (scan operation)
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
# Read with partition key (query operation - efficient)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.jsonl
      file_format: jsonl
      filter_conditions:
        partition_key_name: partition_key_value
      region: us-west-2
```

```yaml
# Read with partition key and sort key (query operation)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      filter_conditions:
        partition_key_name: partition_key_value
        sort_key_name: sort_key_value
      region: us-west-2
```

```yaml
# Read with partition key and additional filter (query with FilterExpression)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      filter_conditions:
        partition_key_name: partition_key_value
        attribute_name: attribute_value
      region: us-west-2
```

```yaml
# Read with non-key attributes (scan operation with filter)
scenario:
  step:
    class: DynamoDBRead
    arguments:
      table_name: your_dynamodb_table
      dest_dir: /path/to/destination
      file_name: dynamodb_data.csv
      file_format: csv
      filter_conditions:
        attribute_name: attribute_value
      region: us-west-2
```

# Notes
- When `filter_conditions` includes the partition key, the query operation is automatically used for better performance and lower cost.
- When `filter_conditions` does not include the partition key, the scan operation is used with FilterExpression.
- All filter conditions are treated as equality (=) conditions.
- Conversion to CSV might be complex for certain DynamoDB attribute types (sets, lists, maps, etc.).
- If the output file already exists, it will be overwritten.
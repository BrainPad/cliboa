# DynamoDBRead
Reads data from a DynamoDB table and saves it as a CSV or JSONL file.

# Parameters
|Parameter|Description|Required|Default|Remarks|
|---------|-----------|--------|-------|-------|
|table_name|DynamoDB table name|Yes|None||
|dest_dir|Output directory|No|"." (current directory)|If a non-existent directory path is specified, it will be automatically created.|
|file_name|Output file name|Yes|None||
|file_format|Output file format|No|"csv"|Can be either "csv" or "jsonl".|
|region|AWS region|No|None|If not specified, the default region will be used.|
|access_key|AWS access key|No|None|If not specified, environment variables or IAM role will be used.|
|secret_key|AWS secret key|No|None|If not specified, environment variables or IAM role will be used.|
|profile|AWS profile|No|None|Section name of ~/.aws/config|

# Example
```yaml
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


# Notes
- Conversion to CSV might be complex for certain DynamoDB attribute types (sets, lists, maps, etc.).
- If the output file already exists, it will be overwritten.
- Partition and sort keys are not guaranteed to line up before other attributes.
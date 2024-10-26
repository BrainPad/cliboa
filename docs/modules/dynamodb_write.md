# DynamoDBWrite
Writes data to a DynamoDB table.

# Parameters
|Parameter|Description|Required|Default|Remarks|
|---------|-----------|--------|-------|-------|
|table_name|DynamoDB table name|Yes|None||
|src_dir|Path of the directory which target files are placed|Yes|None||
|src_pattern|Regex which is to find target files|Yes|None||
|file_format|input file format|No|"csv"|Can be either "csv" or "jsonl".|
|region|AWS region|No|None|If not specified, the default region will be used.|
|access_key|AWS access key|No|None|If not specified, environment variables or IAM role will be used.|
|secret_key|AWS secret key|No|None|If not specified, environment variables or IAM role will be used.|
|profile|AWS profile|No|None|Section name of ~/.aws/config|

# Example
```yaml
scenario:
  step:
    class: DynamoDBWrite
    arguments:
      table_name: your_dynamodb_table
      src_dir: /path/to/source
      src_pattern: "*.csv"
      file_format: csv
```


# Notes

1. CSV files must have a header row. The header names will be used as attribute names in DynamoDB.

2. There is no data type conversion option. When reading from CSV files, all values are treated as strings, which may not be ideal for numeric or boolean data in DynamoDB.

3. The module currently only supports the standard write mode. Transactional writes are not supported.

4. There is no support for conditional writes. It's not possible to specify conditions when overwriting existing items.

5. For JSONL files, each line must be a valid JSON object, and the keys in these objects will be used as attribute names in DynamoDB.

These limitations may be addressed in future versions of the module.

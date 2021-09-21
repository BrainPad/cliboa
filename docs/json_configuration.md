# Table of Contents
* [JSON Configuration](#json-configuration)
* [Basic Principals](#basic-principals)
* [Syntax](#syntax)
* [Examples](#examples)

# JSON Configuration
To define ETL processing, should create scenario.json

## Two Types of scenario.json
Two types of scenario.json exist in executable environment of cliboa.

See [MANUAL.md](../MANUAL.md#user-content-cliboadmin)

## scenario.json in project/$project_name directory.
It exists in project directory of an executable environment of cliboa. Can define under unique projects for ETL processing. 

## scenario.json in common directory.
It exists under common directory of an executable environment of cliboa. Can define only one.

# Basic Principals 
Basic principals to create scenario.json is below.
- Must write all as English one byte characters except for comments.
- Must define 'scenario: ' key on top of scenario.json

# Syntax
syntax of scenario.json is below.
|Parameters|Explanation|Required|Remarks|
|----------|-----------|--------|-------|
|scenario|Defines the contents of scenario.json as the scenario.|Yes||
|step|Can write description or specify symbol. Those should be unique.|Yes||
|class|Specify a step class name to execute.|Yes||
|arguments|Define values of attrubutes of class by key: value..|No||
|symbol|Specify symbol defined on '- step: ' key.|No||
|with_vars|Can write shell script. It can be referred from elements of arguments by using {{}}.|No||


# Examples
## Download a gzip file from the local sftp server, decompress it, and transfer it to the local sftp server.
See [README.md](../README.md#markdown-header-write-a-scenario-of-etl-processing) regarding directory tree.


## Example 1: Download a gzip file from the local sftp server, decompress it, and upload it to the local sftp server.
```
{
  "scenario": [
    {
      "step": null,
      "class": "SftpDownload",
      "arguments": {
        "host": "$hostname",
        "user": "$username",
        "password": "$password",
        "src_dir": "/usr/local",
        "src_pattern": "test.csv.gz",
        "dest_dir": "/tmp"
      }
    },
    {
      "step": null,
      "class": "FileDecompress",
      "arguments": {
        "src_dir": "/tmp",
        "src_pattern": "test.*\\.csv.*\\.gz"
      }
    },
    {
      "step": null,
      "class": "SftpUpload",
      "arguments": {
        "host": "$hostname",
        "user": "$username",
        "password": "$password",
        "src_dir": "/tmp",
        "src_pattern": "test.*\\.csv",
        "dest_dir": "/usr/local"
      }
    }
  ]
}
```


## Example 2: Fetch a excel file via http, convert it to a csv file, read a csv file and insert record into Sqlite
```
{
  "scenario": [
    {
      "step": "Download a file from Kankotyo site",
      "class": "HttpDownload",
      "arguments": {
        "src_url": "https://www.mlit.go.jp/common",
        "src_pattern": "001302036.xlsx",
        "dest_dir": "data",
        "dest_pattern": "test_{{ today }}_.xlsx",
        "with_vars": {
          "today": "date '+%Y%m%d'"
        }
      }
    },
    {
      "step": "Convert a excel file to a csv file",
      "class": "ExcelConvert",
      "arguments": {
        "src_dir": "data",
        "src_pattern": "test.xlsx",
        "dest_dir": "data",
        "dest_pattern": "test.csv"
      }
    },
    {
      "step": null,
      "class": "CsvRead",
      "io": "input",
      "arguments": {
        "src_path": "data/test.csv"
      }
    },
    {
      "step": null,
      "class": "SqliteImport",
      "arguments": {
        "dbname": "test.db",
        "tblname": "test_table"
      }
    }
  ]
}
```

## Default ETL Modules which can be defined in scenario.json
See [Default ETL Modules](/docs/default_etl_modules.md)

## How to Implement Additional Modules
Can implement additional modules if default modules of cliboa are not enough for ETL Processing which would like to implement.
See [Additional Modules](/docs/additional_modules.md)

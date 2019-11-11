# YAML Configuration
To define ETL processing, should create scenario.yml.

# Two Type of scenario.yml
Two types of scenario.yml exist in executable environment of cliboa.

See [MANUAL.md](../MANUAL.md#user-content-cliboadmin)

## scenario.yml in project/$project_name
It exists in project directory of an executable environment of cliboa. Can define under unique projects for ETL processing. 

## scenario.yml in common
It exists under common directory of an executable environment of cliboa. Can define only one.

# Basic Principls to Create scenario.yml
- Must write all as English one byte characters except for comments.
- Must define 'scenario: ' key on top of scenario.yml

# Syntax of scenario.yml
|Parameters|Explanation|Required|Remarks|
|----------|-----------|--------|-------|
|scenario|Defines the contents of scenario.yml as the scenario.|Yes||
|step|Can write description or specify symbol. Those should be unique.|Yes||
|class|Specify a step class name to execute.|Yes||
|io|Specify 'input' or 'output' when a class when extract or load modules are specified. See details of [Default Modules](/docs/default_modules.md) if io is essential.|No|None||
|arguments|Define values of attrubutes of class by key: value..|No||
|symbol|Specify symbol defined on '- step: ' key.|No||
|with_vars|Can write shell script. It can be referred from elements of arguments by using {{}}.|No||


# Default Modules which can be defined in scenario.yml
See [Default Modules](/docs/default_modules.md)

# Examples
## Download a gzip file from the local sftp server, decompress it, and transfer it to the local sftp server.
See [README.md](../README.md#markdown-header-write-a-scenario-of-etl-processing) regarding directory tree.


## Example1: Download a gzip file from the local sftp server, decompress it, and upload it to the local sftp server.
```
scenario:
- step:
  class: SftpDownload
  arguments:
    host: localhost
    user: root
    password: pass
    src_dir: /usr/local
    src_pattern: test.csv.gz
    dest_dir: /tmp
- step: FileDecompress
  arguments:
    src_dir: /tmp
    src_pattern: test.*\.csv.*\.gz
- step:
  class: SftpUpload
  arguments:
    host: localhost
    user: root
    password: pass
    src_dir: /tmp
    src_pattern: test.*\.csv
    dest_dir: /usr/local
```

## Example2: Download a file via FTP and delete the downloaded file.
```
scenario:
- step: ftp_download
  class: FtpDownload
  arguments: # the below arguments are attributes of FtpDownload class.
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: test_{{ today }}_*\.tsv
    dest_dir: /tmp
    with_vars: 
      today: date '+%Y%m%d'

- step: Delete the tsv file downloaded from localhost
  class: FtpDownloadFileDelete
  symbol: ftp_download
```


## Example3: Fetch a excel file via http, convert it to a csv file, read a csv file and insert record into Sqlite
```
scenario:
- step: Download a file from Kankotyo site
  class: HttpDownload
  arguments:
    src_url: https://www.mlit.go.jp/common
    src_pattern: 001302036.xlsx
    dest_dir: data
    dest_pattern: test_{{ today }}_.xlsx
    with_vars:
        today: date '+%Y%m%d'
- step: Convert a excel file to a csv file
  class: ExcelConvert
  arguments:
    src_dir: data
    src_pattern: test.xlsx
    dest_dir: data
    dest_pattern: test.csv
- step:
  class: CsvRead
  io: input
  arguments:
    src_path: data/test.csv
- step:
  class: SqliteCreation
  io: output
  arguments:
    dbname: test.db
    tblname: test_table
```

# How to Implement Additional Modules
Can implement additional modules if default modules of cliboa are not enough for ETL Processing which would like to implement.
See [Additional Modules](/docs/additional_modules.md)
# Table of Contents
* [Scenario Configuration](#scenario-configuration)
* [File Locations & Scope](#file-locations--scope)
* [Syntax](#syntax)
* [Advanced Configuration](#advanced-configuration)
* [Examples](#examples)

# Scenario Configuration
To define ETL processing in Cliboa, you need to create a configuration file often referred to as a "Scenario".

## Supported Formats
Cliboa supports both **YAML** and **JSON** formats for scenario definitions.
* **YAML:** `scenario.yml`
* **JSON:** `scenario.json`

The internal structure and parameter definitions are identical for both formats.

> [!NOTE]
> For the rest of this document, we will refer exclusively to **`scenario.yml`** for simplicity.

# File Locations & Scope
There are two locations where you can place `scenario.yml`. The location determines the scope of the configuration.

## 1. Project Directory
**Path:** `project/$project_name/scenario.yml`
* This file exists within a specific project directory in the Cliboa executable environment.
* Use this to define ETL processing unique to a specific project.

## 2. Common Directory
**Path:** `common/scenario.yml`
* This file exists under the common directory.
* Although Cliboa supports utilizing multiple common scenario files, the standard configuration uses only one.
* It is used for shared configurations across multiple projects.

# Syntax
The syntax structure for `scenario.yml` is defined below.

| Parameter | Explanation | Required | Remarks |
| :--- | :--- | :---: | :--- |
| **scenario** | The root key defining the contents of the scenario. | **Yes** | |
| **scenario.[].step** | A label or description for the step. Can be a descriptive string or a symbol. Must be unique within the scenario. | **Yes** | |
| **scenario.[].class** | Specifies the Python class name of the step to execute. | **Yes** | |
| **scenario.[].symbol** | Specifies a symbol name if one was defined in the `- step:` key. | No | Used for dependency management or references. |
| **scenario.[].listeners** | Specifies listener classes to execute before/after the step. | No | |
| **scenario.[].arguments** | Defines the attributes (variables) required by the class as key-value pairs. | No | |
| **scenario.[].with_vars** | Allows execution of shell scripts. The output can be referenced in `arguments` using `{{ key }}` syntax. | No | Useful for dynamic values like dates. |

> [!TIP]
> For detailed implementation and strict parameter definitions, refer to the source code: [model.py](/cliboa/core/model.py)

# Advanced Configuration

## Symbol: Reusing Arguments

The `symbol` feature allows you to reuse arguments from another step to avoid duplication. This is particularly useful when multiple steps require the same connection information or configuration.

In the example below, the `file delete` step uses the exact same arguments (host, user, password, etc.) as the `file download` step by referencing its symbol name.

```yaml
scenario:
  - step: file download
    class: SftpDownload
    arguments:
      host: dummyhost.com
      user: root
      password: password
      src_dir: /home/foo
      src_pattern: item\.csv
      dest_dir: /usr/local/data

  - step: file delete
    class: SftpDownloadFileDelete
    symbol: file download
```

# Examples

## Example 1: Local File Operation
**Scenario:** Download a gzip file from a local SFTP server, decompress it, and upload the result back to the server.

```yaml
scenario:
  - step: Download file via SFTP
    class: SftpDownload
    arguments:
      host: $hostname
      user: $username
      password: $password
      src_dir: /usr/local
      src_pattern: test.csv.gz
      dest_dir: /tmp

  - step: Decompress the file
    class: FileDecompress
    arguments:
      src_dir: /tmp
      src_pattern: test.*\.csv.*\.gz

  - step: Upload file via SFTP
    class: SftpUpload
    arguments:
      host: $hostname
      user: $username
      password: $password
      src_dir: /tmp
      src_pattern: test.*\.csv
      dest_dir: /usr/local
```

## Example 2: Web to DB

**Scenario:** Fetch an Excel file via HTTP, convert it to CSV, and insert records into SQLite.

```yaml
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
  class: SqliteImport
  arguments:
    dbname: test.db
    tblname: test_table
```

## Default ETL Modules

For a list of built-in modules available for use in `scenario.yml`, refer to: [Default ETL Modules](/docs/default_etl_modules.md)

## How to Implement Additional Modules

If the default modules do not meet your requirements, you can implement custom modules.
See [Additional Modules](/docs/developers/additional_etl_modules.md)

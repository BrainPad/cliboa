# Table of Contents
* [Scenario Configuration](#scenario-configuration)
* [File Locations & Scope](#file-locations--scope)
* [Syntax](#syntax)
* [Advanced Configuration](#advanced-configuration)
  * [Symbol: Reusing Arguments](#symbol-reusing-arguments)
  * [Recipes: Reusable Scenario Snippets](#recipes-reusable-scenario-snippets)
* [Unsupported Features](#unsupported-features)
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

A `scenario.[]` entry is either a **step** (executes a class) or a **`recipe:` directive** (expands a recipe). The two columns show each key's role for that entry type: **Required**, Optional, or `—` (not used).

| Key | Description | Step | Recipe | Notes |
| :--- | :--- | :---: | :---: | :--- |
| **scenario** | The root key defining the contents of the scenario. | Required | Required | Root-level key, required regardless of entry type. |
| **with_vars** | Root-level shell variables shared by every step. Output can be referenced in `arguments` using `{{ key }}` syntax. | Optional | Optional | Root-level key. A step's own `with_vars` takes precedence on name clashes. |
| **scenario.[].step** | A label or description for the step. Can be a descriptive string or a symbol. Should generally be unique within the scenario so that `symbol` references resolve unambiguously. | Required | — | |
| **scenario.[].class** | Specifies the Python class name of the step to execute. | Required | — | |
| **scenario.[].recipe** | References a reusable recipe file in place of a step definition. See [Recipes](#recipes-reusable-scenario-snippets). | — | Required | |
| **scenario.[].symbol** | Specifies a symbol name if one was defined in the `- step:` key. | Optional | — | Used for dependency management or references. |
| **scenario.[].listeners** | Specifies listener classes to execute before/after the step. | Optional | — | |
| **scenario.[].arguments** | Defines the attributes (variables) required by the class as key-value pairs. | Optional | Optional | For a `recipe:` directive, carries the values passed to the recipe. |
| **scenario.[].with_vars** | Allows execution of shell scripts. The output can be referenced in `arguments` using `{{ key }}` syntax. | Optional | — | Useful for dynamic values like dates. |

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

## Recipes: Reusable Scenario Snippets

A **recipe** is a fragment of a scenario stored in its own file and pulled into a scenario via the `recipe:` directive. Use one when the same group of steps (e.g. SFTP download + unzip) recurs across scenarios: extract it once, then reference it.

### Enabling the Feature

List the directories holding recipe files in the `RECIPE_DIRS` constant of your environment file (`cliboa_environment.py`, copied from [`default_environment.py`](/cliboa/conf/default_environment.py)).

* Entries are **searched in order; the first matching file wins.** Listing a project-local directory before a shared one therefore lets a project supply its own version of a recipe.
* Paths should be absolute, and each listed directory must exist at scenario build time.
* If `RECIPE_DIRS` is empty or undefined, the feature is off and any `recipe:` directive raises `CliboaRuntimeError`.

### Recipe File Structure

A recipe declares the `parameters` it accepts and the `recipe` steps it expands into.

```yaml
# recipe/sftp/download_and_extract.yml
parameters:
  src_path: "Remote path on the SFTP server"   # required (shorthand)
  dest_path:                                    # optional, with default
    description: "Local destination directory"
    default: /tmp

recipe:
  - step: Download from SFTP
    class: SftpDownload
    arguments:
      src_path: "{{ args.src_path }}"
      dest_path: "{{ args.dest_path }}"

  - step: Decompress
    class: FileDecompress
    arguments:
      src_dir: "{{ args.dest_path }}"
      src_pattern: '.*\.gz'
```

* `recipe` (**required**) is a non-empty list of plain steps — the same form as scenario steps, but `recipe:` / `parallel:` directives are not allowed here.
* `parameters` (optional) maps each parameter name to a declaration. The shorthand `name: "text"` is equivalent to `{ description: "text" }`, and `description` is always required. A parameter is **required unless it has a concrete `default`** (`default: null` means no default).

### Calling a Recipe

Reference a recipe with `recipe:` (a path under `RECIPE_DIRS`, no extension) and pass values via `arguments:`.

```yaml
# project/myapp/scenario.yml
scenario:
  - recipe: sftp/download_and_extract
    arguments:
      src_path: "{{ base_dir }}/data.csv.gz"
      dest_path: /tmp/work
```

The recipe's steps are spliced in at the call site, each `{{ args.x }}` replaced by the matching argument (or its `default`). The resolved file path is logged at INFO level. Unknown arguments and missing required parameters both raise an error.

### Variable Resolution

Recipes resolve in two passes: first `{{ args.x }}` references are filled from the call site's `arguments`, then the spliced steps go through normal variable resolution.
Argument values may themselves contain `{{ ... }}` references — those are resolved in the second pass like any other variable.

### Constraints

A recipe cannot reference another recipe (no nesting — `recipe:` is not allowed inside a recipe file's `recipe:` list).

# Unsupported Features

The syntax described in this section exists in the codebase and continues to function for backward compatibility, but is **officially designated as "unsupported"** by the cliboa maintainers.

## Parallel Execution (`parallel:` blocks)

Cliboa accepts a `parallel:` key inside the scenario list that lets multiple steps run concurrently in separate worker processes. The implementation lives in [`ParallelStepModel`](/cliboa/core/model.py) and [`_ParallelProcessor`](/cliboa/core/processor.py).

### Status

This feature is **unsupported**. Concretely:

* It does not receive active maintenance from the core maintainers.
* The project's correctness guarantees (test coverage, behavior across releases, interaction with other features) do **not** extend to scenarios that use `parallel:`.
* It is **not** marked as *deprecated*: no `DeprecationWarning` is emitted at runtime. See [Deprecation Guideline](/docs/developers/deprecation_guidelines.md) for what *deprecated* means in cliboa — "unsupported" is a separate designation.
* Community contributions that improve, test, or document this feature are welcome via Pull Requests.

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

- step: Import the csv into SQLite
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

# Table of Contents

* [Quick Start](#quick-start)
  * [Install cliboa](#install-cliboa)
  * [Configuration of a Simple ETL Processing](#configuration-of-a-simple-etl-processing)
  * [Directory Tree](#directory-tree)
  * [Install PyPI packages](#install-pypi-packages)
  * [Write a Scenario of ETL Processing](#write-a-scenario-of-etl-processing)
  * [Set an environment](#set-an-environment)
  * [Execute a scenario of ETL Processing](#execute-a-scenario-of-etl-processing)

# Quick Start
## Requirements
Available on macOS and any Linux distributions, like Debian, Ubuntu, CentOS, REL, or etc.

## Install cliboa
Python version 3.10 or later and poetry are required. In the environment which pip can be used, execute as below.

```
sudo pip3 install poetry
sudo pip3 install cliboa
```

## Configuration of a Simple ETL Processing
After installed cliboa, 'cliboadmin' can be used as an administrator command. 

Create an executable environment of cliboa by using cliboadmin.

```
$ cd /usr/local
$ sudo cliboadmin init sample
$ cd sample
$ sudo cliboadmin create simple-etl
```

## Directory Tree
Directory tree which was created aforementioned commands is as below.

```
sample
├── pyproject.toml
├── bin
│   └── clibomanager.py
├── cliboa
│   └── conf
├── common
│   ├── __init__.py
│   ├── environment.py
│   └── scenario
├── conf
│   ├── cliboa.ini
│   └── logging.conf
├── logs
├── project
│   └── simple-etl
│       ├── scenario
│       └── scenario.yml
```

## Install PyPI packages
```
$ cd sample
$ poetry install
```

## Write a Scenario of ETL Processing
As a simple ETL processing, write scenario.yml in simple-etl as below.

The following example is just download a gzip file from the local sftp server, decompress it, and upload it to the local sftp server.

See [Examples](docs/yaml_configuration.md#examples)

## Set an Environment
To make the above scenario available, set a local machine as a sftp server according to respective environments. Also, put "test.csv.gz" under /usr/local.

## Execute a Scenario of ETL Processing
After wrote scenario.yml and set the environment, execute a scenario by as below command.
```
cd sample
poetry run python3 bin/clibomanager.py simple-etl
```



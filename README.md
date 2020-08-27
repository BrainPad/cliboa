# Table of Contents
* [Introction](#introduction)
  * [What is cliboa](#what-is-cliboa)
  * [Features](#features)
* [Manual](#manual)
* [How to Contribute](#how-to-contribute)
* [Quick Start](#quick-start)
  * [Install cliboa](#install-cliboa)
  * [Configuration of a Simple ETL Processing](#configuration-of-a-simple-etl-processing)
  * [Directory Tree](#directory-tree)
  * [Install PyPI packages](#install-pypi-packages)
  * [Write a Scenario of ETL Processing](#write-a-scenario-of-etl-processing)
  * [Set an environment](#set-an-environment)
  * [Execute a scenario of ETL Processing](#execute-a-scenario-of-etl-processing)
* [YAML Configuration](#yaml-configuration)
* [Default ETL Modules](#default-etl-modules)
* [How to Implement Additional ETL Modules](#how-to-implement-additional-etl-modules)

# Introduction
## What is cliboa
cliboa is an application framework which can implement ETL(ELT) processing. It eases the implementation of ETL(ELT) processing. In this case, ETL(ELT) Processing means the processings like fetch, transform and transfer of data between various databases, storages, and other services.
![](/cliboa_brief.png)

## Features
- Python based framework.
- ETL(ELT) processing is executable by YAML based configuration.
- Additional modules for ETL(ELT) processing can be implemented by only a few steps if default modules not enough.

# Manual
See [MANUAL.md](/MANUAL.md)

# How to Contribute
See [CONTRIBUTING.md](/CONTRIBUTING.md)


# Quick Start
## Requirements
Available on any Linux distributions, like Debian, Ubuntu, CentOS, REL, or etc.

## Install cliboa
python version 3.5 or later and pipenv are required. In the environemnt which pip can be used, execute as below.

```
sudo pip3 install pipenv
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
├── Pipfile
├── bin
│   └── clibomanager.py
├── common
│   ├── environment.py
│   ├── __init__.py
│   ├── scenario
│   └── scenario.yml
├── conf
├── logs
└── project
    └── simple-etl
            ├── scenario
                    └── scenario.yml
```

## Install PyPI packages
```
$ cd sample
$ pipenv install --dev
```
or
```
$ cd sample
$ pipenv lock -r > requirments.txt
$ sudo pip3 install -r requirements.txt
```

## Write a Scenario of ETL Processing
As a simple etl processing, write scenario.yml in simple-etl as below.

The following example is just download a gzip file from the local sftp server, decompress it, and upload it to the local sftp server.

See [Examples](docs/yaml_configuration.md#examples)

## Set an Environment
To make the above scenario available, set a local machine as a sftp server according to respective environments. Also, put "test.csv.gz" under /usr/local.

## Execute a Scenario of ETL Processing
After wrote scenario.yml and set the environment, execute a scenario by as below command.
```
cd sample
pipenv run python3 bin/clibomanager.py simple-etl
```
or
```
cd sample
python3 bin/clibomanager.py simple-etl
```

# YAML Configuration
see [yaml_configuration.md](/docs/yaml_configuration.md)

# Default ETL Modules
see [default_etl_modules.md](/docs/default_etl_modules.md)

# How to Implement Additional ETL Modules
see [additional_etl_modules.md](/docs/additional_etl_modules.md)

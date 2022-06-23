[![PyPI](https://img.shields.io/pypi/v/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![GitHub Actions](https://github.com/BrainPad/cliboa/workflows/cliboa/badge.svg)](https://github.com/BrainPad/cliboa/actions)
[![Code Style:
black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![Contributions Welcome](https://img.shields.io/static/v1.svg?label=Contributions&message=Welcome&color=0059b3&style=flat-square)](https://github.com/BrainPad/cliboa/blob/master/CONTRIBUTING.md)
[![Repo Size](https://img.shields.io/github/repo-size/BrainPad/cliboa)](https://github.com/BrainPad/cliboa)
[![Gitter](https://badges.gitter.im/cliboa/users.svg)](https://gitter.im/cliboa/users?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)


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
![](/img/cliboa_brief.png)

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
Available on macOS and any Linux distributions, like Debian, Ubuntu, CentOS, REL, or etc.

## Install cliboa
python version 3.6 or later and pipenv are required. In the environemnt which pip can be used, execute as below.

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
sample
├── Pipfile
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
└── requirements.txt
```

## Install PyPI packages
```
$ cd sample
$ pipenv install --dev
```
or
```
$ cd sample
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

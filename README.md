# Table of Contents
* [What is Cliboa](#markdown-header-what-is-cliboa)
* [Features](#markdown-header-features)
* [Manual](#markdown-header-manual)
* [How to Contribute](#markdown-header-how-to-contribute)
* [Quick Start](#markdown-header-quick-start)
* [Install Cliboa](#markdown-header-install-cliboa)
* [Configuration of a Simple ETL Processing](#markdown-header-configuration-of-a-simple-etl-processing)
* [Directory Tree](#markdown-header-directory-tree)
* [Install python modules](#markdown-header-install-python-modules)
* [Write a Scenario of ETL Processing](#markdown-header-write-a-scenario-of-etl-processing)
* [Set an environment](#markdown-header-set-an-environment)
* [Execute a scenario of ETL Processing](#markdown-header-execute-a-scenario-of-etl-processing)

# What is Cliboa
Cliboa is an application framework which can implement ETL processing. It eases the implementation of ETL processing. In this case, ETL Processing means the processings like fetch, transform and transfer of data between various databases, storages, and other services.
![](/img/cliboa_brief.png)

# Features
- Python based framework.
- ETL processing is executable by YAML based configuration.
- Additional modules for ETL processing can be implemented by only a few steps if not enough.

# Manual
See [MANUAL.md](/MANUAL.md)

# How to Contribute
See [CONTRIBUTING.md](/CONTRIBUTING.md)


# Quick Start
## Requirements
Available on any Linux distributions, like Debian, Ubuntu, CentOS, REL, or etc.

## Install Cliboa
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
$ cliboadmin init sample
$ cd sample
$ cliboadmin create simple-etl
```

## Directory Tree
Directory tree which was created aforementioned commands is as below.

```
├── bin
│   └── clibomanager.py
├── common
│   ├── environment.py
│   ├── __init__.py
│   ├── scenario
│   └── scenario.yml
├── conf
├── logs
├── Pipfile
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

See [Example1](docs/yaml_configuration.md)

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


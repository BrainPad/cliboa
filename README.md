[![PyPI](https://img.shields.io/pypi/v/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cliboa?style=flat-square)](https://pypi.org/project/cliboa)
[![GitHub Actions](https://github.com/BrainPad/cliboa/actions/workflows/test.yaml/badge.svg)](https://github.com/BrainPad/cliboa/actions/workflows/test.yaml)
[![Code Style:
black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![Contributions Welcome](https://img.shields.io/static/v1.svg?label=Contributions&message=Welcome&color=0059b3&style=flat-square)](https://github.com/BrainPad/cliboa/blob/master/CONTRIBUTING.md)
[![Repo Size](https://img.shields.io/github/repo-size/BrainPad/cliboa)](https://github.com/BrainPad/cliboa)
[![Gitter](https://badges.gitter.im/cliboa/users.svg)](https://gitter.im/cliboa/users?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

> [!IMPORTANT]
> **Note on Future Development**
> We are preparing for the release of **cliboa v3**.
> New features are no longer being accepted for v2.
> Support for v2 will end immediately upon the release of v3.
> See [Issue #584](https://github.com/BrainPad/cliboa/issues/584) for details.

# Table of Contents
* [Introduction](#introduction)
  * [What is cliboa](#what-is-cliboa)
  * [Features](#features)
* [Documentation](#documentation)

# Introduction
## What is cliboa
cliboa is an application framework which can implement ETL(ELT) pipeline. It eases the implementation of ETL(ELT) pipeline. In this case, ETL(ELT) pipeline means the processings like fetch, transform and transfer of data between various databases, storages, and other services.
![](/img/cliboa_brief.png)

## Features
- Python based framework.
- ETL(ELT) processing is executable by YAML based configuration.
- Additional modules for ETL(ELT) pipeline can be implemented by only a few steps if default modules not enough.

# Documentation

* [QuickStart](/docs/quick_start.md)
* [MANUAL](/docs/manual.md)
  * [YAML Configuration](/docs/yaml_configuration.md)
  * [Default ETL Modules](/docs/default_etl_modules.md)
* [How to Contribute](/CONTRIBUTING.md)
  * [Coding Style Guide](/docs/developers/coding_style_guide.md)
  * [Layered Architecture](/docs/developers/layered_architecture.md)
  * [How to Implement Additional ETL Modules](/docs/additional_etl_modules.md)
    * [Step Class Extention](/docs/developers/step_class_extention.md)

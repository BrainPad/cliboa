# Table of Contents
* [How to Install](#how-to-install)
* [Console Commands](#console-commands)
	* [cliboadmin](#cliboadmin)
		* [Usage](#usage)
		* [Example](#example)
	* [clibomanager](#clibomanager)
		* [Usage](#usage)
		* [Example](#example)
* [YAML Configuration](#yaml-configuration)
* [How to Implement Additional ETL Modules](#how-to-implement-additional-etl-modules)


# How to Install
See [README.md](/README.md#markdown-header-install-cliboa)

# Console Commands
## cliboadmin
cliboadmin is an administrator command of cliboa which can be used after installed cliboa.

### Usage
```
Commands:
	init $dir_name    Create an executable environment of cliboa based on the given directory.
	create $dir_name  Create a project for ETL scenario. Should execute right on the root of a directory tree of cliboa. 

Options:
	-h/--help    Show help
```

### Example
Create 'sample' directory as an executable environment of cliboa.
```
$ cliboadmin init sample
$ tree sample
sample
|-- Pipfile
|-- bin
|   `-- clibomanager.py
|-- cliboa
|   `-- conf
|-- common
|   |-- __init__.py
|   |-- environment.py
|   |-- scenario
|   `-- scenario.yml
|-- conf
|   |-- cliboa.ini
|   `-- logging.conf
|-- logs
`-- project

```

Create simple-etl project as an ETL scenario right on the above.
```
$ cd sample
$ cliboadmin create simple-etl
A project for ETL scenario is created.
$ tree .
.
|-- Pipfile
|-- bin
|   `-- clibomanager.py
|-- cliboa
|   `-- conf
|-- common
|   |-- __init__.py
|   |-- environment.py
|   |-- scenario
|   `-- scenario.yml
|-- conf
|   |-- cliboa.ini
|   `-- logging.conf
|-- logs
`-- project
    `-- simple-etl
        |-- scenario
        `-- scenario.yml

```


## clibomanager
clibomanager.py is a runner of ETL(ELT) scenario implemented by YAML. It can be used after created an executable environment of cliboa by cliboadmin.

### Usage
```
Commands:
    python bin/clibomanager.py $project_name

Options:
    -h/--help    Show help
```

### Example
Execute by using python command under an executable environment of cliboa.
```
cd sample
python bin/cliboa.py simple-etl
```

# YAML Configuration
Should create scenario.yml if make ETL(ELT) processing activate.
See [YAML Configuration](/docs/yaml_configuration.md)

# How to Implement Additional ETL Modules
see [additional_etl_modules.md](/docs/additional_etl_modules.md)

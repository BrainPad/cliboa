# Table of Contents
* [How to Install](#user-content-how-to-install)
* [Console Commands](#user-content-console-commands)
	* [cliboadmin](#user-content-cliboadmin)
		* [Usage](#user-content-usage)
		* [Example](#user-content-example)
	* [cliboa.py](#user-content-cliboa.py)
		* [Usage](#user-content-usage)
		* [Example](#user-content-example)
* [YAML Configuration](#user-content-yaml-configuration)
* [How to Implement Additional Modules](#user-content-how-to-implement-additional-modules)


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
|-- bin
|   `-- cliboa.py
|-- common
|   |-- __init__.py
|   |-- environment.py
|   |-- scenario
|   `-- scenario.yml
|-- conf
|-- logs
|-- project
`-- requirements.txt

6 directories, 5 files
```

Create simple-etl project as an ETL scenario right on the above.
```
$ cd sample
$ cliboadmin create simple-etl
A project for ETL scenario is created.
$ tree .
.
|-- bin
|   `-- cliboa.py
|-- common
|   |-- __init__.py
|   |-- environment.py
|   |-- scenario
|   `-- scenario.yml
|-- conf
|-- logs
|-- project
|   `-- simple-etl
|       |-- scenario
|       `-- scenario.yml
`-- requirements.txt

9 directories, 8 files
```


## cliboa.py
cliboa.py is an executor command of ETL scenario implemented by YAML. It can be used after created an executable environment of cliboa by cliboadmin.

### Usage
```
Commands:
    python bin/cliboa.py $project_name

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
Should create scenario.yml if make ETL processing activate.
See [YAML Configuration](/docs/yaml_configuration.md)


# Table of Contents
* [Additional ETL Modules](#additional-etl-modules)
* [How to Implement Additional ETL Modules](#how-to-implement-additional-etl-modules)
* [How to Make Additional Modules Activate](#how-to-make-additional-modules-activate)

# Additional ETL Modules
Can implement additional modules for ETL(ELT) Processing easily if default prepared modules of cliboa are not enough.

# How to Implement Additional ETL Modules
## Implement an Additional Step Class
If would like to add NewExtract class, inherit BaseStep class and implement attributes and an execute method.

## Example
```
class NewExtract(BaseStep):
 
    def __init__(self):
        super().__init__()
        """
        Implement attributes to be set in scenario.yml
        """
        self._spam = spam
     
    def spam(self, spam):
        self._spam = spam
 
    def execute(self, *args):
        """
        Implement processes which would like to do
        """
```

### Returns
Method 'execute' should return a response.
```
0: Process ends immediately.
others: Process ends immediately(safely returns but output an error to log).
None: Process continue (Default)
```

# How to Make Additional Modules Activate
## Configuration
Put additional ETL modules in the following directories.
- If additional modules are used commonly, put them under common/scenario directory of an executable environment of cliboa.
```
|-- bin
|   `-- clibomanager.py
|-- common
|   |-- scenario # here
```

- If additional modules are used only in each projects, put them under project/scenario directory of an executable environment of cliboa.
```
|-- bin
|   `-- clibomanager.py
`-- project
    `-- simple-etl
        |-- scenario # here
```

See [MANUAL.md](../MANUAL.md#user-content-example), regarding an executable environment of cliboa.

## Add File Paths
Should add paths of additional ETL modules in common/environment.py
```
|-- bin
|   `-- clibomanager.py
|-- common
|   |-- environment.py # here
```

## Example
```
# path of modules which put in a commnon directory
COMMON_CUSTOM_CLASSES = [
    '$module_name.$class_name'
]
 
# path of modules which put in project directories
PROJECT_CUSTOM_CLASSES = [
    '$module_name.$class_name'
]
```

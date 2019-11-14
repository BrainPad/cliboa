# Additional Modules
Can implement additional modules for ETL Processing easily if default modules of cliboa are not enough.

## How to Implement Additional Modules
### Step Class
If would like to add NewExtract class, inherit BaseStep class and implement attributes and an execute method.

### Example
```
class NewExtract(BaseStep):
 
    def __init__(self):
        super()__init__()
        """
        Implement attributes to be set in scenario.yml
        """
        self.__spam = spam
     
    @property
    def spam(self):
        return self.__spam
 
    @spam.setter
    def spam(self, spam):
        self.__spam = spam
 
    def execute(self, *args):
        """
        Implement processes which would like to do
        """
```

## How to Make Addtional Modules Activate
### Configuration
Put additional modules in the following directories.
- If additional modules are used commonly, put under common directory of an executable environment of cliboa.
- If additional modules are used only in each projects, put in project directory of an executable environment of cliboa.

See [MANUAL.md](../MANUAL.md#user-content-example), regarding an executable environment of cliboa.

### Add File Paths
Should add paths of additional modules in common/environment.py

### Returns
Method 'execute' might returns a response.
0: Process ends immediately.
others: Process ends immediately(safely returns but output an error to log).
None: Process continue (Default)

### Example
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
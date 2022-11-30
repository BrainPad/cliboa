# Execute Shell Script
Execute shell script. Can be execute from (.sh) file or inline command.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|command.content|Inline script to run|Yes|None|Required if command.file is not defined|
|command.file|File path to shell script file|Yes|None|Required if command.content is not defined|
|work_dir|Directory path where script will be execute|No|./|Only work when using inline script|

# Examples
## Execute Inline Script
```
scenario:
- step: ExecuteShellScript
  class: ExecuteShellScript
  arguments:
    command:
      content: >
        mkdir awesome-directory &&
        touch awesome-directory/awesome-path
    work_dir: /root/to/awesome-folder    
```

## Execute Inline Script
```
scenario:
- step: ExecuteShellScript
  class: ExecuteShellScript
  arguments:
    command:
      file: /root/to/awesome-folder/awesome-script.sh
```
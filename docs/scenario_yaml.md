# *Chapter1: Create scenario.yml and basic usage*

There are two things that you need to understand scenario.yml for Cliboa is "scenario", and "step"

"scenario" represents the entire process and "step" represents the individual processes.
Cliboa executes all steps defined in the scenario one by one up to the end.
In this page we will guide you on how to create the cenario.yml for Cliboa.

## The first thing you need to do is create a new file named scenario.yml.
As I already mentioned "scenario" stands for the entire process,
which means you must start with "scenario" for the very top of the structure
```
scenario
```
## Under the "scenario",  define the "step" that will actually be executed
You are able to define multiple steps. Do so by defining them as an array.
```
scenario:
  - step:
```
## You can give each step a name or comment so that its easy to understand what it is.
```
scenario:
  - step: execute sample step
```
## There are some required parameters and optional parameters for each step.
All steps must be defined as "class" parameter.
```
scenario:
  - step: execute sample step
    class: SampleStep
```

Now, the simplest scenario is complete.
This scenario primarily functions to execute the SampleStep.
Let's move on to the next step.

Cliboa allows you to give arguments when executing steps.
To do so, add a "arguments" block for the step.
```
scenario:
  - step: execute sample step
    class: SampleStep
    arguments:
```
And then define the arguments you want to place in it.
(Not all arguments are allowed to placed in it, see documentation of individual classes what can be placed)
```
scenario:
  - step: execute sample step
    class: SampleStep
    arguments:
      foo: test value
```
The arguments which are placed, can be used inside the class as you would like to below.
It can NOT be taken from args, or kwargs.
```
print(self._foo)
```
You can also pass multiple arguments.
```
scenario:
  - step: execute sample step
    class: SampleStep
    arguments:
      foo: test value
      num: 12345
      arr:
        - apple
        - orange
        - lemon
```

***

# *Chapter2: Advanced usage*

scenario.yml provides some special features which makes you to use Cliboa more flexibly.

* _listener_
If you want to do some common processing(ex. log output) before and after steps, listener is very suitable.
By default, Cliboa implements StepStatusListener in all steps which is just output log before and after steps.
In addition, you can add any extra listeners like below.
Note: Creating a custom listener does not explain here
```
scenario:
  - step: execute sample step
    listeners: SampleListener
    class: SampleStep
    arguments:
      foo: test value
```

* _symbol_
Allows arguments defined in one step to be used in another step.
This makes you to avoid writing same arguments over and over.
To do so, just set the value defined in one step to the symbol for another step.
Example below is a scenario that is download a file from sftp, and then delete it after that.
Instead of setting arguments for SftpDownloadFileDelete, but symbol is set.
So it can use an exactly the same arguments with SftpDownload and connect the sftp server to delete the file.
```
scenario:
  - step: file download
    class: SftpDownload
    arguments:
      host: dummyhost.com
      user: root
      password: password
      src_dir: /home/foo
      src_pattern: item\.csv
      dest_dir: /usr/local/data

  - step: file delete
    class: SftpDownloadFileDelete
    symbol: file download
```
Note: Arguments set with symbol is abled to get via the method defined in super class
```
host = super().get_step_argument("host")
user = super().get_step_argument("user")
```

* _parallel_
Cliboa basically is a sequential operation of waiting for the step result and then executing the next step.
But this parameter allows it to be executed asynchronously without waiting for the completion of the step.
When executing steps in parallel, define "parallel" for the block above steps.
Fork steps will be executed by multi process operation, NOT executed by multi threading.
(A python process is launched for each step)
Which means more steps in parallel you define is more OS memory is used.
```
scenario:
  parallel:
    - step: file download
      class: SftpDownload
      arguments:
        host: dummyhost.com
        user: root
        password: password
        src_dir: /home/foo
        src_pattern: item\.csv
        dest_dir: /usr/local/data

    - step: file download
      class: SftpDownload
      arguments:
        host: dummyhost.com
        user: root
        password: password
        src_dir: /home/bar
        src_pattern: data\.csv
        dest_dir: /usr/local/data
```

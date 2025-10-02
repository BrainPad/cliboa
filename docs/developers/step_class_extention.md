# *Chapter1: Class extention*

If default classes provided by Cliboa are not enough for you, you can create a new class and use it.
In this part, let us show you how to create a custom step for Cliboa(class extension).

In Cliboa, the module you want to use must be defined as "class".
Also, parent class of all classes must be BaseStep class.
Here, as an example, create a custom class which just outputs the log.

## The first thing you need to do is create a new file and define a custom class which has BaseStep class as parent
・example.py
```
from cliboa.scenario.base import BaseStep
class CustomStep(BaseStep):
```
## Then, override a method "execute" which is actually do something(Also recommend you to override __init__ too).
・example.py
```
from cliboa.scenario.base import BaseStep
class CustomStep(BaseStep):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
```
## Implement in execute method whatever you want.
・example.py
```
from cliboa.scenario.base import BaseStep
class CustomStep(BaseStep):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        print("This is custom class.")
```
## You can also add argument parameters if required.
Parameters are set via the setter method which is the same name as the parameter names.
```
from cliboa.scenario.base import BaseStep
class CustomStep(BaseStep):
    def __init__(self):
        super().__init__()
        self._foo = None

    def foo(self, foo):
        self._foo = foo

    def execute(self, *args):
        print("This is custom class.")
        print(self._foo)
```
## Finally, register this class in environment.py(Either PROJECT_CUSTOM_CLASSES or COMMON_CUSTOM_CLASSES) 
・environment.py
```
COMMON_CUSTOM_CLASSES = ["example.CustomStep"]
```
Now new custom class is available like an example below.
・scenario.yml
```
scenario:
  - step: call custom step
    class: CustomStep
    arguments:
      foo: Hello
```

***

# *Chapter2: Listener extention*
Listeners are also extendable.


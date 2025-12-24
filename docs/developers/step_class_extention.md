# Chapter1: Class extention

If default classes provided by Cliboa are not enough for you, you can create a new class and use it.
In this part, let us show you how to create a custom step for Cliboa(class extension).

In Cliboa, the module you want to use must be defined as "class".
Also, parent class of all classes must be BaseStep class.
Here, as an example, create a custom class which just outputs the log.

## Custom class implementation guide

The first thing you need to do is create a new file and define a custom class which has BaseStep class as parent.
And then, override a method "execute" which is actually do something.

You can implement in execute method whatever you want.

*example.py*
```
from cliboa.scenario.base import BaseStep

class CustomStep(BaseStep):

    def execute(self, *args, **kwargs):
        print("This is custom class.")
```

## How to define arguments

If you want, You can also define Arguments nested class.
The nested **Arguments** class is instantiated with arguments defined in the scenario file. This is automatically handled within cliboa, and the arguments instance become accessible as self.args when the execute method is run.

*example.py*
```
from cliboa.scenario.base import BaseStep
from pydantic import BaseModel

class CustomStep(BaseStep):
    class Arguments(BaseModel):
        foo: str

    def execute(self, *args, **kwargs):
        print("This is custom class.")
        print(self.args.foo)
```

## Please register your custom classes in environment.py(Either PROJECT_CUSTOM_CLASSES or COMMON_CUSTOM_CLASSES) 

*environment.py*
```
COMMON_CUSTOM_CLASSES = ["example.CustomStep"]
```

### Tips: How custom classes are discovered in cliboa

As you know, cliboa reads configuration values from the path defined by the `CLIBOA_ENV` environment variable.
cliboa uses one of the following combinations of defined configuration values to search for custom classes:

1. `COMMON_CUSTOM_ROOT_PATHS` and `COMMON_CUSTOM_CLASSES`
2. `PROJECT_CUSTOM_ROOT_PATHS`, `PROJECT_SCENARIO_DIR_NAME`, and `PROJECT_CUSTOM_CLASSES`

Only the class name is written in the class field of the scenario file.
The discovery process uses the class name specified in the class field as a key, searching first for custom classes, then project classes, and finally cliboa default classes.

While this search order is guaranteed, if multiple classes with the same name exist within either the custom classes or the project classes, the precedence is not guaranteed. It is recommended to adopt unique class names wherever possible.

## Use your custom step

Now new custom class is available like an example below.

*scenario.yml*
```
scenario:
  - step: call custom step
    class: CustomStep
    arguments:
      foo: Hello
```

# Chapter2: Listener extention
Listeners are also extendable.


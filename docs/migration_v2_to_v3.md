# Overview

This guide details the changes and steps required to migrate your application from `v2` to `v3`.

Version 3 introduces significant architectural improvements, focusing on performance, type safety, and ease of use.
While we have tried to minimize breaking changes, some were necessary to support new features.

## Quick Upgrade Guide

For general users, the following steps are expected to be the minimum required for upgrading to v3:

1.  **Upgrade cliboa to v3.**
2.  **Update the constants** defined in `environment.py` (specified by the `CLIBOA_ENV` environment variable). [See details](#changes-to-configuration-definitions)
3.  **Modify custom listener classes**, if you have created any. [See details](#listener-redesign)
4.  **Modify your code** if you are using deprecated or removed features in custom scenario classes. [See details](#breaking-changes)
5.  **Fix implementations relying on `sys.path.append`** in your local code, if any. [See details](#abolishment-of-system_append_paths)
6.  **Rewrite scenario files** if you are using parallel execution. [See details](#changes-to-scenario-file-syntax)
7.  **Run your application and check for warnings.** Since some features are deprecated in v3 even if they maintain backward compatibility, `DeprecationWarning`s may be output. If you wish to suppress them, modify your code according to the warning contents. [See details](#deprecated-features)

---

## What Remains the Same

To help you estimate the migration effort, here are the **key features that remain unchanged** and kept backward compatible:

* **Scenario file basic syntax:** The syntax for scenario files remains compatible, with the exception of parallel execution configuration.
* **How to implement user extension class:** The implementation method for user extension classes—inheriting from `BaseStep` and wrapping the `execute` method—remains unchanged.
* **Entrypoint of `cliboa.interface.run`:** The entrypoint for cliboa has not changed (note that while another entrypoint was added in v3, the existing one is preserved).
* **Definition of user-defined classes:** The handling of `COMMON_CUSTOM_CLASSES` and `PROJECT_CUSTOM_CLASSES` constants, which manage user extension classes, remains unchanged.

## Breaking Changes

This section outlines the breaking changes introduced from v2 to v3.

### Major Rewrite of the Core Layer

We have performed a major rewrite of the core layer. It is safe to assume that almost all code within the core layer has undergone breaking changes.
We did not provide documentation for the core layer in v2, as we assumed general users would not call the core directly. Therefore, we will not provide a detailed explanation here. (If any users were directly invoking the core, they were likely developers who had read the source code thoroughly themselves).
Users who wish to know more should refer directly to the cliboa core layer source code.

### Changes to Configuration Definitions

In v2, configuration values were read as constants from the module specified by the `CLIBOA_ENV` environment variable. While this mechanism remains the same in v3, some constants have been changed.

First, the following constants are no longer needed and have been **removed**:

- `SYSTEM_APPEND_PATHS`
- `SCENARIO_DIR_NAME`
- `COMMON_SCENARIO_DIR`

Additionally, the following **required constants have been added**:

- `COMMON_CUSTOM_ROOT_PATHS`
- `PROJECT_CUSTOM_ROOT_PATHS`

Other changes to required and optional constants are omitted here. For detailed information on v3 configuration, please refer to the code comments in `/cliboa/conf/default_environment.py`.

#### Abolishment of SYSTEM_APPEND_PATHS

Among the removed constants, the abolishment of `SYSTEM_APPEND_PATHS` may have a significant impact depending on your usage.
If your implementation is Pythonic, you likely do not rely on `SYSTEM_APPEND_PATHS`. However, if you did rely on it, Python module imports may fail because the system path will no longer be automatically added.
If this is the case, it is a good opportunity to refactor your code to a Pythonic implementation. We recommend fixing your import statements.

*Note: If fixing the imports is difficult, a workaround would be to execute `sys.path.append` at the beginning of your application code (although cliboa does not officially support this).*

#### Abolishment of cliboa.ini

`cliboa.ini` contained only settings for logging masks.
This has been abolished in v3 and is now included in the constant definitions. If not configured, the default cliboa mask settings will apply.

### Changes to Scenario File Syntax

Regarding the scenario file syntax, basic compatibility is maintained, but some changes have been made.
In particular, there are **breaking changes regarding parallel execution**, so even if it worked in v2, it may not work as intended in v3.

- The location for defining `with_vars` has been moved one level shallower.
- The configuration method for parallel execution has been significantly changed.

The syntax for parallel execution in scenario files was unnecessarily complex and was one of the issues in v2. We decided to make breaking changes in v3 to improve this.
For details on the scenario file syntax in v3, please refer to [scenario configuration guide](/docs/scenario_configuration.md).

### Removal of Property Injection in Step Listener Classes

In v2, step listener classes forcibly injected parent step class properties using methods like `lis.__dict__.update(arguments)`.
Presumably, this was done to provide easy access to the arguments of the parent step instance. However, this was not a good implementation practice and led to unstable behavior.
Step listener class methods receive the step instance itself as an argument. It has been possible since v2 to access the `arguments` of the step instance by referencing its properties, without relying on forcibly injected properties.

In v3, we have removed this **flawed** implementation. Therefore, listener classes that previously relied on their own properties must now be modified to reference the properties of the instance passed as an argument.

### Changes to Authentication File Specification for Some Scenarios

In scenario classes that access services like GCP or SFTP, there was a method to specify authentication information directly as `content`. This has been abolished, and specification is now limited to the path of the authentication file only. For details, please refer to the [documentation for each class](/docs/default_etl_modules.md).

### Removal of Some Classes

Some classes determined to be unnecessary have been removed. Their functionality may have been integrated into other classes.
Although we expect this to be a rare case, action is required if you were using these classes directly.
The classes removed from the scenario and utils layers are as follows:

- `cliboa.scenario.extras.ExceptionHandler`
- `cliboa.util.cache.StepArgument`
- `cliboa.util.class_util.ClassUtil`
- `cliboa.util.exception.SqliteInvalid`
- `cliboa.util.lisboa_log.LisboaLog`
- `cliboa.util.rdbms_util.Rdbms_Util`

### Undocumented Breaking Changes

We have made various modifications to the core features in conjunction with the update to v3. These modifications range from architectural reviews and improved type safety via modeling to fixes for potential bugs.
We have strived to maintain backward compatibility as much as possible and have conducted tests, primarily unit tests.
However, as we are human, we cannot guarantee zero mistakes, and there is a possibility that backward compatibility has been broken unintentionally (or that we missed documenting it).

If you discover any breaking changes between v2 and v3 not listed here, please open an Issue to let us know. We will consider either fixing it as a bug or adding it to this documentation.

## Deprecated Features

### Listener Redesign

A new `listener` layer has been established. The base class for listeners has moved there, and definitions, including method names, have been revamped.
While the legacy listener base class is maintained, the impact on existing listener classes is significant, such as the inability to access parent step instance properties as its own properties (as mentioned in Breaking Changes).
We recommend migrating to an implementation that inherits from the new base class as soon as possible.

### Access to Some Arguments

In some scenario classes, the recommended method for accessing arguments has changed in v3 to use the **Nested Arguments Class**.
To maintain compatibility for child classes inheriting from these, we have provided wrapper methods using `@property` for access.
While this maintains compatibility, a warning will now be displayed upon access.
Note that modifying values via these wrappers is considered an unintended use case for maintaining compatibility and is not supported. If you have a request for this, please open an Issue.

### Major Deprecations and Alternatives

Many classes and functions in the scenario, adapter, and util layers have been deprecated.
It is difficult to list them all here, but executing deprecated features will output a warning to standard error.
The following are some of the most representative deprecated features and properties:

| Deprecated Feature | Alternative |
|:-------------------|-------------|
| `cliboa.scenario.base.BaseStep.get_target_files` | `cliboa.scenario.file.FileRead.get_src_files` |
| `cliboa.scenario.base.BaseStep.get_step_argument` | `cliboa.scenario.base.BaseStep.get_symbol_argument` |
| `cliboa.scenario.base.BaseStep._step` <br> `cliboa.scenario.base.BaseStep._symbol` <br> `cliboa.util.cache.ObjectStore` | `cliboa.scenario.base.BaseStep.put_to_context` <br> and/or <br> `cliboa.scenario.base.BaseStep.get_from_context` |
| `cliboa.util.helper` | `cliboa.scenario.base.BaseStep._set_arguments` |

### Deprecated in Near Future

The legacy method of managing arguments using class properties and assignment functions will be deprecated. `cliboa.scenario.validator.EssentialParameters` is also subject to this.
While compatibility is still maintained as of v3.0, it is decided that these will be deprecated within the v3 lifecycle (in v3.x updates). Compatibility is planned to be removed when updating to v4.0 in the future.

Going forward, please migrate to defining a **Nested Arguments Class** using Pydantic v2 models.

## Additional Notes

### Changes in Log Output

Due to the comprehensive changes in the core layer, the content of logs output by cliboa may differ from previous versions.
We do not classify changes in log output text as "breaking changes," so we are mentioning this here as a supplementary note.

### Notable New Features

Here are some representative new features available in v3. While these are minor improvements, we believe they are "quality of life" updates that address specific user needs:

* **Custom Log Fields:** Added `cliboaState` and `cliboaPath` as cliboa-specific log items.
* **Global `with_vars`:** You can now define `with_vars` at the same level as `scenario`, allowing variables to be applied globally to the entire scenario execution.

For more details on each feature, please refer to the respective documentation.

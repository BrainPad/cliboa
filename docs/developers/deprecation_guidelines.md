# Deprecation Guideline

These guidelines define the policies and procedures for handling features that have been marked as deprecated.

---

## Principle of Backward Compatibility

As a general rule, we maintain backward compatibility except during major version changes.
However, exceptions are recognized (though reluctantly) for bug fixes and within alpha and beta versions.

## Handling of Deprecated Features

### Support Policy

While backward compatibility is maintained for deprecated features, they will no longer be officially supported in versions released after the deprecation.
If a bug is discovered in a deprecated feature, its priority for correction will be low, except for critical issues such as security vulnerabilities.

### User Notification (DeprecationWarning)

When a deprecated function is called, a DeprecationWarning must be output to notify the user.

#### Required Output Destination
The DeprecationWarning must **always** be output to **stderr**.
Additionally, it may be output as a warning-level log through a logger.

> [!NOTE]
> We recommend using a common utility function `cliboa.util.base._warn_deprecated` for outputting DeprecationWarnings to stderr.
> While output control is not currently a priority for the team, we welcome proposals and contributions if there is a demand for it.

#### Content of the Warning
The notification must include the following information:
1.  **The name of the feature** that has been deprecated.
2.  **The version in which it was deprecated** (e.g., `v2.1`).
3.  **The planned version for compatibility removal** (e.g., `v4.0`).
4.  **What to use instead** (if applicable).

Compatibility removal is generally planned for major version upgrades (e.g., round versions like `v4.0` or `v5.0`).

#### Internal Calls of Deprecated Features

There are cases where deprecated features are called internally within the cliboa library, even if the user is not calling them directly. If a user receives a DeprecationWarning that they do not recognize, it may be due to this pattern.

While we strive to avoid calling deprecated functions within the library, to prevent potential difficulties in altering shared functionalities, **it is recommended, but not required, for committers to modify all internal library calls when deprecating a feature.**
The only required rule when deprecating a feature is to maintain backward compatibility. (Strict maintenance of backward compatibility is not required during deprecating a feature with major version upgrades, as they represent an exception to the general rule.)
Note that when a feature is removed, changing all callers is required.

If you discover such a case and wish to remove the DeprecationWarning, this is **a chance to contribute** by submitting a Pull Request. We welcome your PRs.

## Features with Removed Backward Compatibility

If it is possible to detect that a feature with removed backward compatibility is being called, developers may choose to output a warning, but it is not mandatory.
In many cases, it is impossible to detect calls to features with removed compatibility; if detection is possible, outputting a warning is discretionary and not required.
If a warning is output, the rules for the notification must adhere to the rules specified in the "User Notification" section above.

# Deprecation Guideline

These guidelines define the policies and procedures for handling features that have been marked as deprecated.

---

## Principle of Backward Compatibility

As a general rule, we maintain backward compatibility except during major version changes.
However, exceptions are recognized (though reluctantly) for bug fixes and within alpha and beta versions.

## Handling of Deprecated Features

### Support Policy

While backward compatibility is maintained for deprecated features, they will no longer be officially supported in versions released after the deprecation.
If a bug is discovered in a deprecated feature, its priority for correction will be low.

### User Notification (DeprecationWarning)

When a deprecated function is called, a DeprecationWarning must be output to notify the user.

#### Required Output Destination
The DeprecationWarning must **always** be output to **stderr**.
Additionally, it may be output as a warning-level log through a logger.

#### Content of the Warning
The notification must include the following information:
1.  **The name of the feature** that has been deprecated.
2.  **The version in which it was deprecated** (e.g., `v2.1`).
3.  **The planned version for compatibility removal** (e.g., `v4.0`).
4.  **What to use instead** (if applicable).

Compatibility removal is generally planned for major version upgrades (e.g., round versions like `v4.0` or `v5.0`).

## Features with Removed Backward Compatibility

If it is possible to detect that a feature with removed backward compatibility is being called, developers may choose to output a warning, but it is not mandatory.
In many cases, it is impossible to detect calls to features with removed compatibility; if detection is possible, outputting a warning is discretionary and not required.
If a warning is output, the rules for the notification must adhere to the rules specified in the "User Notification" section above.



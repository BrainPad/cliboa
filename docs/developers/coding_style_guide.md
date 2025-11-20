# Coding style guide

The coding style guide of cliboa refers to [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).

Basically, passing checks with black, isort, and flake8 is sufficient.
Minimize and avoid global state as much as possible, and aim for naming conventions where the role is clear from the name.

## Scope rules

Cliboa references [PEP 8](https://peps.python.org/pep-0008/) regarding scope definitions.

The definitions are as follows:

- **Public classes and methods:** Any class or method that is not marked as private.
- **Private classes and methods:** Names starting with an underscore (`_`).

### Access Restrictions

* **Private properties (including methods) of a public class:** For internal Cliboa use only.
* **Private classes and methods:** For internal Cliboa use only.
* **Private properties (including methods) of a private class:** Accessed only from within the class itself.

> **Note on "Internal Cliboa use only":**
> These interfaces are considered non-public APIs. They may be changed without notice or a major version update.

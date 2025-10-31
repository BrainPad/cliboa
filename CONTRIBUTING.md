# How to Contribute to cliboa

## Table of Contents
* [1. Welcome](#1-welcome)
* [2. Contribution Guidelines](#2-contribution-guidelines)
    * [2-1. Search Existing Issues](#2-1-search-existing-issues)
    * [2-2. Open a New Issue](#2-2-open-a-new-issue)
    * [2-3. Submit a Pull Request (PR)](#2-3-submit-a-pull-request-pr)
* [3. Workflow for All Contributors](#3-workflow-for-all-contributors)
    * [3-1. Branch Strategy](#3-1-branch-strategy)
    * [3-2. Architecture and Coding Style Guide](#3-2-architecture-and-coding-style-guide)
    * [3-3. Review Process](#3-3-review-process)
    * [3-4. Core Maintainer Definition](#3-4-core-maintainer-definition)
    * [3-5. Review Process Exception](#3-5-review-process-exception)
* [4. Licensing](#4-licensing)

## 1. Welcome

Thank you for considering contributing to **cliboa**!
We welcome all contributions, whether it's bug reports, feature suggestions, code changes, or documentation improvements.

## 2. Contribution Guidelines

We welcome all types of contributions!  
The general flow for contributing is simple and flexible.

### 2-1. Search Existing Issues
Please start by checking **open Issues** to see if your topic (bug, feature, question, or any other) has already been discussed.  
If you find a relevant Issue, feel free to comment!

### 2-2. Open a New Issue
If you can't find a related discussion, please **open a new Issue**.

### 2-3. Submit a Pull Request
When you're ready to submit code, please create a Pull Request.  
You can submit a PR to resolve an Issue, or even without one. However, please be aware that PRs submitted without an issue may require a longer review time.

## 3. Workflow for all contributors

### 3-1. Branch strategy

Use <a href="https://guides.github.com/introduction/flow/">github flow</a>

![](/img/cliboa_github_flow.png)

1. Fork from https://github.com/BrainPad/cliboa
2. Create your branch for feature
```
$ git checkout -b new-feature
```
3. Commit your changes
```
$ git commit -am 'Add some feature'
```
4. Push to your branch
```
$ git push origin new-feature
```
5. Create new pull request to https://github.com/BrainPad/cliboa, master branch

### 3-2. Architecture and coding style guide

See documents for developers below:

* [Layered Architecture](/docs/developers/layered_architecture.md)
* [Coding style guide](/docs/developers/coding_style_guide.md)

### 3-3. Review process

We aim to review all Pull Requests promptly.  
Please follow these expectations:

1. We require that **all CI checks pass** before a review begins.
    * PRs with failing CI tests may be put **on hold** until all checks are green, unless there are special circumstances.
2. A core maintainer will review your code. This process may involve feedback, discussion, and requests for further revisions or modifications.
3. Your PR will be merged once it receives approval from at least one core maintainer.

### 3-4. Core Maintainer Definition

A **Core Maintainer** is defined as an individual who possesses **Admin** or **Maintain** privileges for this repository on GitHub.
Core Maintainers are responsible for the long-term health and stability of the project.

### 3-5. Review Process Exception

To expedite the merging of low-risk changes, Core Maintainers are permitted to **self-merge (self-approve and merge)** a Pull Request (PR) **without waiting for an additional reviewer** when the core logic remains unchanged.

**Note:** All changes, regardless of self-merge status, **must be submitted via a Pull Request** to maintain a complete modification history.

The details of self-merging are as follows.

1.  **Core Logic Changes (`cliboa/`):**
    * Any changes within the `cliboa/` directory **must** be reviewed and approved by at least one other Core Maintainer. Self-merge is **not** permitted.
    * However, self-merge is **permitted** if the changes involve only code comments, without changing the logic.
2.  **Other Changes:**
    * Changes to all other files are considered low-risk and can be **self-merged**.
    * However if you, as the Core Maintainer, **feel a review is necessary, you may request a review** from another Core Maintainer.
3.  **CI Requirement:**
    * All PRs, regardless of the path, **must have all continuous integration (CI) checks in a passing (green) state** before merging.

## 4. Licensing

By contributing your code to the cliboa project, you agree to license your contributions under the project's [LICENSE](/LICENSE).


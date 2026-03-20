# Quick Start

This guide describes how to set up and run a simple project using **cliboa**.

## Prerequisites

Before getting started, ensure your environment meets the following requirements:

* **OS:** macOS or Linux distribution
* **Package Manager:** [Poetry](https://python-poetry.org/docs/#installation) must be installed.

> [!Note]
> This guide demonstrates the setup using Poetry. If you prefer other package managers (e.g., `uv`, `rye`), please adapt the installation and execution commands to suit your environment.

## Installation & Execution

Follow these steps to set up a sample project and verify the installation.

### 1. Setup Workspace

Create a new directory for your project and navigate into it.

```bash
mkdir cliboa-quickstart
cd cliboa-quickstart
```
### 2. Initialize Poetry

Initialize a new poetry project. You can press *Enter* to accept the default values for the prompts.

```bash
poetry init
```

### 3. Install cliboa

Add cliboa to your project dependencies.

```bash
poetry add cliboa
```

### 4. Initialize your Application using cliboa.

Initialize the directory structure for cliboa. This creates the `cb_app` directory.

```bash
poetry run cliboadmin init -a cb_app
```

### 5. Create a New Project

Create a sample project named `test_project`.

```bash
poetry run cliboadmin pj -p test_project
```

### 6. Run the Project

Execute the project using the `cliboa_run.py` script.

```bash
poetry run python cb_app/cliboa_run.py test_project
```

If the installation and execution were successful, you will see the `Hello cliboa!` message printed to your standard output.

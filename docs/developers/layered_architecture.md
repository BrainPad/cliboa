# Layered Architecture

<img width="452" alt="Layered-Architecture-Image" src="https://github.com/BrainPad/cliboa/refs/heads/master/img/layer.png">

Cliboa uses a layered architecture.

Roles of each layers is below:

1. interface
  * The interface to use cliboa application easily.
2. core
  * Main logic of cliboa.
3. scenario
  * Classes of ETL steps.
4. adapter
  * Adapter classes and functions for external communication.
5. util
  * Reusable helper, or foundational, classes and functions - they are not involved in external communication.
6. conf
  * Load environment variables.

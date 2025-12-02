# CHANGELOG


## v0.10.0 (2025-12-02)

### Features

- Advances in the implementation of Join operation
  ([`39f714f`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/39f714f36cc02f549639ab049f20f8ae03500794))


## v0.9.0 (2025-12-01)

### Features

- Implements basic reduce operation.
  ([`8f20b31`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/8f20b31011d89c119acd4e4e5ad05d455686ceab))

Implements a basic reduce operation within the Spark driver. This involves adding a transformation
  step to the RDD pipeline and executing it on the workers. The changes also include improvements to
  memory estimation within the cache and a diagram explaining the client-driver-worker
  communication. The client code has been updated to use the new reduce functionality.


## v0.8.0 (2025-11-28)

### Features

- Improves RDD caching and task execution
  ([`6465905`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/64659056f855717113f808ba90fc16ee4884bb67))

Introduces a partition cache with memory management and disk spilling to improve performance.

Updates the task execution flow to leverage the cache, and modifies the data types being managed.

Includes large file generation script for testing purposes.


## v0.7.0 (2025-11-27)

### Features

- Refactor driver by separating funcs in different files
  ([`5b9fe06`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/5b9fe06c432b31b9517c02896e49a945de6419c2))


## v0.6.0 (2025-11-26)

### Features

- Jobs status saved in files
  ([`4206666`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/4206666c3befa53e177a528f2be0a22fc4472ecd))


## v0.5.0 (2025-11-25)


## v0.4.0 (2025-11-24)

### Features

- Refactors driver and adds RDD text file reading
  ([`02621b5`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/02621b59d0b2a6947bd5594915c6591782f0c8e3))

Moves driver functionality into a dedicated package.

Adds functionality for reading text files into RDDs, enabling data loading and processing.

Improves code organization and introduces core RDD operations.


## v0.3.0 (2025-11-22)

### Features

- Progress in the collect() func and .bat files fixed
  ([`76462ed`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/76462ed2e051428b4c18f673f941d7edb029d359))

- Refactor file management
  ([`86aa1ad`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/86aa1ad5ce8f587da978ed93daaabf7ed0477002))


## v0.2.0 (2025-11-20)

### Features

- Simplification of code
  ([`44de7d0`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/44de7d0d611c5b1d37eca30e267720c4015a0d9c))

deleted most of the boiler plate to start the actual architecture


## v0.1.2 (2025-11-19)

### Bug Fixes

- Added Comunication.drawio
  ([`70c4708`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/70c47089dfe94d330b7df01da6f9b5ce410c3c79))


## v0.1.1 (2025-11-19)

### Bug Fixes

- Diagrams directory
  ([`cf97676`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/cf9767687090e46f2b702d4eeb5bf22402fa30e5))


## v0.1.0 (2025-11-18)


## v0.0.0 (2025-11-18)

### Features

- Initial commit for distributed processing system
  ([`c2d9235`](https://github.com/Josue-Echeverria/Go-Mini-Spark/commit/c2d92350766f6179dc60f2b1bdf8692b5f588514))

# Project Structure

## Needle

The program logic is stored in `needle/` and split into directories that define the code's scope and responsibility.

- `config/` - Data structures that act as static configuration for needle modules. May also contain some logic attached to the config classes.
- `modules/` - Python-scoped business logic. All code should be runnable natively and include a CLI entrypoint. Doesn't know anything about the orchestrator.
- `tasks/` - Prefect-scoped business logic. Contains Prefect `tasks` that are run by flows. These tasks typically construct data models using `config`s and pass them to `modules` to do work on.
- `flows/` - Prefect-scoped orchestration logic. Flows should focus only on orchestration and as little on logic and work as possible. Flows should call `tasks` and not `modules`.
- `lib/` - Miscellaneous tools for use by all components.
- `docs/` - Contains the project documentation.
- `cli.py` - Used to access the main pipeline's run options.

## Container

Stores Dockerfiles:

- `needle-base.dockerfile` - The base dockerfile that installs fundamental libraries. Mainly `WSClean` and `CASA`.
- `needle.dockerfile` - The runtime dockerfile. Installs the needle module on top of the base. Sets up env variables and CASA config.

The base dockerfile takes a long time to build, which is the main reason it's separated.

## Tests

Contains the unit tests. These are in a flat directory for now.

Tests are minimal for now as Needle is undergoing heavy development.

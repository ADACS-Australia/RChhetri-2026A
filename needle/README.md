# Needle Module

This is the Needle module. This README contains some development information and highlights the design philosophy used to create the module.

## File and Directory breakdown

- `config/` - Data structures that act as static configuration for needle modules.
- `modules/` - Python-scoped business logic. All code should be runnable natively and include a CLI entrypoint.
- `tasks/` - Prefect-scoped business logic. Contains Prefect `tasks` that are run by flows. These tasks typically construct data models and pass them to `modules` to do work on.
- `flows/` - Prefect-scoped orchestration logic. Uses tasks to transform inputs.
- `lib/` - Miscellaneous tools
- `deploy.py` - Deploys the pipeline to the Prefect server.

## Pydantic Model design

Many of the models are either Config or Context models.

Config models are constructed from static data that can be known before runtime.
Context models group the associated Config with runtime-derived variables. These also contain the means for execution of the work. All variables and configuration required to do the work is contained within the Context.

The role of the `task` is to take the Config and use it to construct the Context at runtime, then pass this to the relevant `module` to do work.

In most cases, this is all a task needs to do.

## Modules and CLI

Redeploying and running a flow from scratch can be costly. When developing, we want to do this as little as possible.

Therefore, we should make an entrypoint to all of the Pythonic business logic wherever possible. This is why all modules contain a CLI entrypoint.

## Configurability

We don't want the user to have to make a pull request every time they need to run a module (e.g. flagging or imaging) with a different input parameter.

Therefore, we want to give the user as much control over the static configuration as possible. This is why all Config `models` can be totally customised in the `config.yaml`

# Design

An overview of some of the design features of Needle.

## Pydantic Models

Needle makes heavy use of [Pydantic](https://pydantic.dev/docs/validation/latest/get-started/) as it provides:

- Easy documentation
- Runtime type validation
- Built-in dictionary serialisation
- Data coercion

Needle creates its own [NeedleModel class][needle.config.base.NeedleModel] that builds off the of pydantic base model by adding some features.

### Config and Context

Many of the models are either Config or Context models.

Config models are constructed from static data that can be known before runtime (all stored in `config/`).
This is an important distinction, as it means that config objects should be built once and then treated as immutable.
See [WSCleanConfig][needle.config.clean.WSCleanConfig] for an example.

Context models group the associated Config with runtime-derived variables.
All variables and configuration required to do the work is contained within the Context (contexts are stored in `modules/`)
The [Context class][needle.modules.needle_context.SubprocessExecContext] is designed to be inherited by a module, which will run its `execute()` method in order to do work.
See [WSCleanContext][needle.modules.clean.WSCleanContext] for an example.

## Tasks and Functions

The role of the `task` is to take the Config and use it to construct the Context at runtime, then pass this to the relevant `module` to do work.

We would like as much of the business logic to be assigned to regular python functions rather than Prefect tasks.
This helps with testability and reduces unnecessary abstraction.

## Modules and CLI

Redeploying and running a flow from scratch can be costly. When developing, we want to do this as little as possible.

Therefore, we should make an entrypoint to all of the Pythonic business logic wherever possible.

We also want to give the user the option of doing any of the steps in isolation where feasible.

For these reasons, all modules contain a CLI entrypoint.

## Configurability

We don't want the user to have to make a pull request every time they need to run a module (e.g. flagging or imaging) with a different input parameter.

Therefore, we want to give the user as much control over the static configuration as possible. This is why all Config `models` can be totally customised in the `config.yaml`

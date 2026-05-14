# Design

An overview of some of the design features of Needle.

## Prefect

Needle uses [Prefect](https://www.prefect.io/). Prefect has many possible applications, but for our purposes, it is a pythonic workflow orchestrator.

Prefect encourages the use of its workflow concepts:

- flows: A flow (workflow) is a self-contained pipeline of work. These can be very small or very large and also can call other flows.
- tasks: Tasks are called by flows and are considered a single unit of work.

Needle makes heavy use of tasks and flows, but prefers native python where feasible. This helps with testability, modularity and reduces unnecessary abstraction.
As such, the role of most `task`s is to organise static and runtime configuration into a context and pass this to a function (typically from a `module`) to do work.

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

Context models group the associated config with runtime-derived variables.
All variables and configuration required to do the work is contained within the Context (contexts are stored in `modules/`)
The [Context class][needle.modules.needle_context.SubprocessExecContext] is designed to be inherited by a module, which will run its `execute()` method in order to do work.
See [WSCleanContext][needle.modules.clean.WSCleanContext] for an example.

## Modules and CLI

Redeploying and running a flow from scratch can be costly. When developing, we want to do this as little as possible.

We also want to give the user the option of doing any of the steps in isolation where feasible.

Therefore, we should make an entrypoint to all of the Pythonic business logic wherever possible.

For these reasons, all `modules` contain a CLI entrypoint.

Since the modules leverage the [NeedleModel][needle.config.base.NeedleModel] base class to construct their config, adding these options to the parser is simple.

## Configurability

We don't want the user to have to make a pull request every time they need to run a module (e.g. flagging or imaging) with a different input parameter.

Therefore, we want to give the user as much control over the static configuration as possible. This is why all Config `models` can be totally customised in the `config.yaml`

## Runtime Requirements

### Containerisation

Needle should be as portable as possible. It may need to be run on any kind of system. In order to be interoperable, it must be containerisable.

Needle can run its modules using an optional (.sif) container. When provided, the module commands are run inside this container.

### CASA & Lazy Loading

Needle should be runnable without needing to install CASA libraries onto the system. This is desirable because the CASA libraries are very large and system-dependent. The aforementioned containerisation pattern is a solution to this.

However, some modules use CASA libraries to open and read measurement sets and then do something with that data in Python. This necessarily requires CASA modules installed and imported in the python runtime.

We can get around this since we have access to the needle module via a .sif container. Instead of always loading the CASA modules with the rest of the imported modules, we can instead lazy-load the CASA modules and rerun any module inside itself.

We can lazy-load like this:

```python

def get_table():
    try:
        from casatools import table

        return table()
    except ImportError:
        raise RuntimeError(
            "casatools is required to read an MS directly."
        )
```

Then, when running a module, we get an implementation pattern like this:

```python

class InspectMSContext(SubprocessExecContext):
    ms: Path
    "Path to the measurement set"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    "Log level. Only relevant if runtime is set"

    @property
    def cmd(self) -> list[list[str]]:
        return [["needle-inspect-ms", str(self.ms), "--log-level", self.log_level]]

def inspect_ms(ctx: InspectMSContext) -> MSInfo:
    if ctx.runtime:
        ctx.log_cmd()
        procs = ctx.execute()
        for p in procs:
            if p.stderr:
                logger.warning(p.stderr)
            if p.stdout:
                print(p.stdout)
        return MSInfo.from_json(ctx._output_path)
    msinfo = MSInfo(ms=ctx.ms)
    msinfo.to_json()
    return msinfo
```

The logic flow (when a container runtime is supplied) is as follows:

- Enter `inspect_ms()` function
- See that a container runtime is supplied
- Launch myself inside the container with the same arguments that were supplied to me
  - Enter `inspect_ms()` function
  - See that no container is supplied
  - Generate the MSInfo object using CASA libraries and serialise it to a JSON file
- Deserialise the JSON file into the MSInfo object and return it

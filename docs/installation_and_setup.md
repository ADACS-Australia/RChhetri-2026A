# Installation & Setup

## Python Needle Modules

The python modules and entrypoints in Needle can be installed the regular way as it utilises a `pyproject.toml`

```bash
pip install .

# editable:
pip install -e .

# Include CASA libraries for local environment execution
pip install -e ".[casa]"

# Include test libraries
pip install -e ".[tests]"
```

Modules can be run in the local environment. They each have a CLI entrypoint. See the `pyprojct.toml` for all entrypoint names.

Modules can be run either with the local runtime environment or with a container.
Local environment execution may require the installation of some additional libraries such as CASA (instructions above) and WSClean.
Alternatively, build or pull the [containers](#containers) to utilise Apptainer or Singularity execution.

Standalone modules can be executed as-is without the need to set up Prefect.

To set up the full pipeline, the [Prefect](#prefect-setup) server needs to be set up.

## Containers

There are two custom containers:

- needle-base: Mainly contains [CASA](https://casadocs.readthedocs.io/en/stable/index.html) (modular) and [WSClean](https://wsclean.readthedocs.io/en/latest/)
- needle: Builds from needle-base. Installs the needle python package and sets up the CASA and data directories

There is a makefile to build all of the containers

```bash
# Build everything
make all

# Build base
make base

# Build needle
make needle

# Clean build artifacts
make clean
```

## Prefect setup

The pipeline is orchestrated with [Prefect](https://www.prefect.io/).

- Prefect database (Postgres)
- Redis
- Prefect webserver

These components can be spun up with the provided compose file.

```bash
docker compose -f container/docker-compose.yaml up -d
```

The webserver will be made available at [http://localhost:4200](http://localhost:4200)

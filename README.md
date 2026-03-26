# Needle

## Installation

The python modules and entrypoints in Needle can be installed the regular way as it utilises a pyproject.toml

```bash
pip install .
# editable:
pip install -e .
```

Modules can be run in the local environment. They each have a CLI entrypoint.

To set up the pipeline, the Prefect server needs to be set up. See below.

## Docker

There are three custom containers

- needle-base: Contains `casa` and `wsclean`
- needle: Builds off of needle-base. Installs the needle python package and sets up the casa and data directories
- needle-worker: Is just the prefect container with the prefect-docker python package

There is a makefile to build all of the containers

```bash
# Build everything
make all
# Build base
make base
# Build needle
make needle
# Build worker
make workder
# Clean build artifacts
make clean
```

## Prefect setup

The pipeline is orchestrated with [Prefect](https://www.prefect.io/).

- Prefect database (Postgres)
- Redis
- Prefect webserver
- Worker pool

These components can be spun up with the `docker-compose.yaml` file.

```bash
docker compose up -d
```

This will also deploy the needle-pipeline deployment using an ephemeral container.
To do so, it expects a config file at `${HOME}/needle.yaml`. See below for an example of a valid config file.

The webserver will be made available at [http://localhost:4200](http://localhost:4200)

## Config

The pipeline modules are all configurable via a single config file. Here is an example:

```yaml
flow:
  tgt_pattern: 'SB047529_beam(?P<beam>\d+)\.ms'
  cal_pattern: 'cal_beam(?P<beam>\d+)\.ms'
  local_data_dir: /home/user/needle_data
  overwrite: True
  max_workers: 2
  log_level: DEBUG

flag:
  quack:
    interval: 10.0
  tfcrop: {}

calibrate:
  setjy: {}
  bandpass: {}
  gaincal: {}
  applycal: {}
  split: {}

shallow_clean: {}
source_find: {}
create_mask: {}
deep_clean: {}
interval_clean: {}
```

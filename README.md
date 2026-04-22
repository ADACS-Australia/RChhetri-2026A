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

## Needle Config

The pipeline modules are all configurable via a single config file. Here is an example:

```yaml
flow:
  tgt_pattern: 'SB047529_beam(?P<beam>\d+)\.uvfits'
  cal_pattern: 'cal_beam(?P<beam>\d+)\.uvfits'
  data_dir: /scratch/pawsey0008/ksmith1/needle_data
  overwrite: True
  max_workers: 2
  log_level: DEBUG
  runtime:
    image: /software/projects/pawsey0008/ksmith1/needle.sif
    type: singularity

flag:
  quack:
    enabled: True
  tfcrop:
    enabled: True

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
model_subtract: {}
interval_clean: {}
```

## Cluster Config

If using a SLURM cluster, an additional config file is required. Eg:

```yaml
account: "pawsey0008"
queue: "work"
cores: 2
memory: "8GB"
processes: 1
walltime: "02:00:00"

min_workers: 1
max_workers: 8

local_directory: "/scratch/pawsey0008/ksmith1/needle_data/dask-scratch"
log_directory: "/scratch/pawsey0008/ksmith1/needle_data/logs"

job_script_prologue:
  - "module load singularity/4.1.0-slurm"

job_extra_directives: []
```

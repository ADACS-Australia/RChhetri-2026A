# Configuration

## Needle Pipeline

Needle revolves around its main pipeline. Such is the nature of ETL pipelines that their orchestration should remain largely immutable. The order and structure of processing steps is fixed by design.

However, the user may know more about their dataset or working environment than the pipeline does. Therefore, every module in the pipeline exposes a configuration interface that allows the user to tune its behaviour without modifying the pipeline itself. Parameters such as thresholds, file paths, and processing options can all be adjusted per-module to suit the characteristics of a given dataset.

To see which parameters are available for each module, see the module's appropriate config (example - [clean config][needle.config.clean.WSCleanConfig])

Below is an example configuration:

```yaml
flow:
  overwrite: True
  max_workers: 2
  log_level: DEBUG
  interval_tasks: 10
  runtime:
    image: /path/to/image/needle.sif
    type: singularity

watcher:
  log_level: INFO
  log_file: /path/to/log/output/watcher.log

data:
  source: /path/to/data/source/directory
  staging_dir: /path/to/place/working/files

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

## Cluster

If using a SLURM cluster, an additional (.yaml) config file is required to configure the Dask worker.

This is a Prefect/Dask construct, so is not codified in Needle.
For users familiar with SLURM, the configuration should be fairly intuitive. Note that there are a few dask-specific additions

Use the below example as a reference.

```yaml
account: "pawsey0008"
queue: "work"
# N-Cores tasks will run concurrently in a single dask worker (slurm job)
# Keep this in mind when choosing cores and memory
cores: 2
memory: "64GB"
processes: 1
# Max time per dask worker
walltime: "02:00:00"

# Number of simultaneous dask workers
min_workers: 1
max_workers: 20

# A directory for dask operational stuff
local_directory: "/scratch/pawsey0008/ksmith1/needle_data/dask-scratch"
# A directory for dask to output its logs
log_directory: "/scratch/pawsey0008/ksmith1/needle_data/logs"

# Anything to execute per-job before running the task
job_script_prologue:
  - "module load singularity/4.1.0-slurm"
  - "ssh -f -N -i ~/.ssh/worker-login -o StrictHostKeyChecking=no -o ConnectTimeout=5 -L 4200:localhost:4200 setonix-04"
  - "export PREFECT_API_URL=http://localhost:4200/api"
  - "export PREFECT_LOGGING_EXTRA_LOGGERS=needle"
  - "export PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=DEBUG"
  - "export PREFECT_RESULTS_PERSIST_BY_DEFAULT=true"

job_extra_directives: []
```

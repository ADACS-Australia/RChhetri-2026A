# Needle

This project originated as part of an [ADACS](https://adacs.org.au/) project grant.

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
  interval_tasks: 10
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
memory: "64GB"
processes: 1
walltime: "02:00:00"

min_workers: 1
max_workers: 20

local_directory: "/scratch/pawsey0008/ksmith1/needle_data/dask-scratch"
log_directory: "/scratch/pawsey0008/ksmith1/needle_data/logs"

job_script_prologue:
  - "module load singularity/4.1.0-slurm"
  - "ssh -f -N -i ~/.ssh/worker-login -o StrictHostKeyChecking=no -o ConnectTimeout=5 -L 4200:localhost:4200 setonix-04"
  - "export PREFECT_API_URL=http://localhost:4200/api"
  - "export PREFECT_LOGGING_EXTRA_LOGGERS=needle"
  - "export PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=DEBUG"
  - "export PREFECT_RESULTS_PERSIST_BY_DEFAULT=true"

job_extra_directives: []
```

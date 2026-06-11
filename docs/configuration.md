# Configuration

## Needle Pipeline

Needle revolves around its main pipeline. The order and structure of the pipeline's processing steps is largely fixed by design.

The user may know more about their dataset or working environment than the pipeline does. Therefore, every module in the pipeline exposes a configuration interface that allows the user to tune its behaviour without modifying the pipeline itself. Parameters such as thresholds, file paths, and processing options can all be adjusted per-module to suit the characteristics of a given dataset.

To see which parameters are available for each module, see the module's appropriate config (example - [clean config][needle.config.clean.WSCleanConfig])

### Config Validation

Configuration validation can be done before attempting to run via the CLI entrypoint

```bash
needle-validate -c /path/to/config.yaml
```

Which will validate each subsection of the config.

The config as seen by Needle can also be dumped. This can be useful as a quick and easy way to check what fields are defaulted to when not assigned:

```bash
needle-validate -c /path/to/conifg.yaml -p
```

### Minimal Configuration

The pipeline itself requires very minimal manually set options to function. However, this will not prevent most steps from running, they will simply run with the default settings. As such, it is a good idea to become familiar with the defaults in the [config api reference][needle.config.pipeline.NeedleConfig].

```yaml
flow:
  log_level: DEBUG

watcher:
  log_level: INFO
  # Optional but recommended watcher log file
  log_file: /path/to/log/output/watcher.log

data:
  # Path or local S3 bucket location of source data
  source: /path/to/data/source/directory
  # Local directory to work in
  staging_dir: /path/to/place/working/files

flag:
  # Flag options are DISABLED by default - absent flag steps are ignored
  quack: {}
  tfcrop: {}
```

### Extensive Configuration

For more complex pipelines, you can fine-tune almost every aspect of the processing. This example demonstrates a more involved configuration, including container runtime settings, multiple flagging steps, and specific cleaning parameters.

```yaml
flow:
  overwrite: true
  shm_size: "4gb"
  log_level: DEBUG
  max_workers: 4
  interval_tasks: 2

data:
  # Using S3 as a source
  source: s3://my-observation-bucket/raw-data/
  staging_dir: /local/scratch/needle_work
  stability_check: 120

watcher:
  poll_interval: 60
  log_level: DEBUG
  log_file: /local/scratch/logs/watcher.log

flag:
  # Multiple flagging steps can be configured
  quack:
    interval: 15.0
    mode: beg
  clip:
    min_amp: 0.1
    max_amp: 50.0
    clip_zeros: true
  tfcrop:
    time_cutoff: 5.0
    freq_cutoff: 5.0
    flag_dimension: freqtime
  rflag:
    time_devscale: 4.0
    freq_devscale: 4.0
  extend:
    grow_time: 50.0
    grow_freq: 50.0
  manual:
    antenna: "ea01,ea05"
    timerange: "00:00:00~00:10:00"

calibrate:
  setjy:
    field: "1934-638"
    standard: "Perley-Butler 2017"
  bandpass:
    field: "1934-638"
    solint: "inf"
    refant: "ea01"
    minsnr: 5.0
  gaincal:
    field: "0537-441"
    solint: "int"
    calmode: "ap"
  applycal:
    field: "target_source"
    interp: "linear"
  split:
    field: "target_source"
    datacolumn: "corrected"

shallow_clean:
  size: 4096
  scale: "5asec"
  niter: 5000
  pol: "I"
  weight: briggs
  robust: 0.0

source_find:
  innerclip: 5
  outerclip: 4
  max_summits: 2
  cores: 8

create_mask:
  padding: 10.0

deep_clean:
  size: 8192
  scale: "2asec"
  niter: 100000
  auto_mask: 5.0
  auto_threshold: 1.0
  minuv_l: 500.0

model_subtract:
  tag: "residual"
  subtract_model: true

interval_clean:
  niter: 1000
  auto_threshold: 3.0
  scale: "5asec"
```

## Cluster

If using a Dask cluster, either locally or via Slurm, an additional `.yaml` config file is required at `~/.needle_cluster.yaml`.

For the full picture, please see the [config api reference][needle.config.cluster.ClusterConfig].

### Local Cluster

```yaml
type: local # required - either 'slurm' or 'local'

scaling:
  min_workers: 1
  max_workers: 2
  dashboard_port: 8787 # optional, default 8787

container:
  image: /path/to/image/needle.sif
  type: apptainer # either 'singularity' or 'apptainer'

local:
  cores: 1 # number of cores per worker
  memory: "8GB" # memory per worker
```

### SLURM Cluster

For users familiar with SLURM, the configuration should be fairly intuitive. Note that there are a few Dask-specific additions.

> The bind mount used as an example may be required depending on the setup. **The prefect server and the workers must have the same timezone.**

```yaml
type: slurm # required - either 'slurm' or 'local'

scaling:
  min_workers: 1
  max_workers: 20
  dashboard_port: 8787 # optional, default 8787

container:
  image: /path/to/image/needle.sif
  type: singularity # either 'singularity' or 'apptainer'
  binds:
    - /usr/share/zoneinfo/UTC:/etc/localtime # optional extra bind mounts - this one is highly recommended

slurm:
  account: "pawsey0008"
  queue: "work"
  cores: 1 # Must be 1 for thread safety
  memory: "64GB"
  processes: 2
  walltime: "02:00:00"

  # A directory for Dask operational files
  local_directory: "/scratch/pawsey0008/ksmith1/needle_data/dask-scratch"
  # A directory for Dask to output its logs
  log_directory: "/scratch/pawsey0008/ksmith1/needle_data/logs"

  # Commands to execute per-job before running the task
  job_script_prologue:
    - "module load singularity/4.1.0-slurm"
    - "ssh -f -N -i ~/.ssh/worker-login -o StrictHostKeyChecking=no -o ConnectTimeout=5 -L 4200:localhost:4200 setonix-04"
    - "export PREFECT_API_URL=http://localhost:4200/api"
    - "export PREFECT_LOGGING_EXTRA_LOGGERS=needle"
    - "export PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=DEBUG"
    - "export PREFECT_RESULTS_PERSIST_BY_DEFAULT=true"

  job_extra_directives: []
```

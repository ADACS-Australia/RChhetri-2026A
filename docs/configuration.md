# Configuration

## Environment Variables (.env) and Setup

Prefect requires a few things set in .env to work properly:

```bash
# Set needle as an extra logger so that needle logs appear in the prefect logger UI
export PREFECT_LOGGING_EXTRA_LOGGERS="needle"
# Set the needle log level for prefect
export PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL="INFO"
# Automatically saves task and flow results
export PREFECT_RESULTS_PERSIST_BY_DEFAULT="true"
# IMPORTANT - must be set to the relevant host api endpoint
export PREFECT_API_URL=https://prefect.ips-askap.cloud.edu.au/api
# If the Prefect server has auth enabled, set the string
export PREFECT_API_AUTH_STRING="admin:password"
# Path to the needle config file
export NEEDLE_CONFIG=/home/ksmith/software/configs/needle.yaml
# Path to the needle cluster config file
export NEEDLE_CLUSTER_CONFIG=/home/ksmith/software/configs/needle_cluster.yaml
```

It is good practice to `chmod 600` the .env file to prevent prying eyes.
You could also source the `.env` in your `~/.bashrc`, which would enable you to seamlessly run various needle functions
from anywhere.

## Needle Pipeline

Needle revolves around its main pipeline. The order and structure of the pipeline's processing steps is largely fixed
by design.

The user may know more about their dataset or working environment than the pipeline assumes. Therefore, every module in
the pipeline exposes a configuration interface that allows the user to tune its behaviour without modifying the pipeline
itself. Parameters such as thresholds, file paths, and processing options can all be adjusted per-module to suit the
characteristics of a given dataset.

To see which parameters are available for each module, see the module's appropriate config (example - [clean config][needle.config.clean.WSCleanConfig])

### Config Validation

Configuration validation can be done before attempting to run via the CLI entrypoint

```bash
needle validate -c /path/to/config.yaml
```

Which will validate each subsection of the config.

The config as seen by Needle can also be dumped. This can be useful as a quick and easy way to check what fields are
defaulted to when not assigned:

```bash
needle validate -c /path/to/conifg.yaml -p
```

### Minimal Configuration

The pipeline itself requires very minimal manually set options to function. However, this will not prevent most steps
from running, they will simply run with the default settings. As such, it is a good idea to become familiar with the
defaults in the [config api reference][needle.config.pipeline.NeedleConfig].

```yaml
flow:
  log_level: DEBUG

data:
  # Location of the work directories. Relevant for production only
  work_dir: /fred/oz513/NeedleData/Observations
  # Location of the downloaded observations. Relevant for production only
  download_dir: /fred/oz513/NeedleData/Downloads
  # Location of the CASA measures and logfiles
  casa_dir: /fred/oz513/CASA

flag:
  # Flag options are DISABLED by default - absent flag steps are ignored
  quack: {}
  tfcrop: {}
```

You can also print out the entire rendered config with

```bash
needle display -c /path/to/config.yaml
```

This is a good way to explore the possible values for the config without having to scour the aforementioned api
reference.

### Extensive Configuration

For more complex pipelines, you can fine-tune almost every aspect of the processing. This example demonstrates a more
involved configuration, including container runtime settings, multiple flagging steps, and specific cleaning parameters.

```yaml
flow:
  overwrite: true
  shm_size: "4gb"
  log_level: DEBUG
  max_workers: 4
  interval_tasks: 2
  skip_to_deep_clean: false

data:
  work_dir: /fred/oz513/NeedleData/Observations
  download_dir: /fred/oz513/NeedleData/Downloads
  casa_dir: /fred/oz513/CASA

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

calibrate_solve:
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

calibrate_apply:
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
  minuv_l: 500.0

model_subtract:
  tag: "residual"
  subtract_model: true

interval_clean:
  niter: 1000
  scale: "5asec"

imstack:
  time_interval: 1.0
  stamp_size: 16
  dtype: "float16"
```

### SLURM Cluster

For users familiar with SLURM, the configuration should be fairly intuitive. Note that there are a few Dask-specific
additions.

> The bind mount used as an example may be required depending on the setup. **The prefect server and the workers must
> have the same timezone.**

```yaml
type: slurm # required

scaling:
  min_workers: 1
  max_workers: 40 # Max number of dask workers to run concurrently
  dashboard_port: 8787 # optional, default 8787

container:
  image: /path/to/image/needle.sif
  type: apptainer # either 'singularity' or 'apptainer'
  binds:
    # Make sure you bind mound all of the directories the compute nodes need to work in
    - /usr/share/zoneinfo/UTC:/etc/localtime # optional extra bind mounts - this one is highly recommended
    - /fred/oz513:/fred/oz513
    - /home/ksmith/software:/home/ksmith/software
    - /aphid/scratch-3month/ksmith:/aphid/scratch-3month/ksmith

slurm:
  # Each of the slurm directives are applied to each dask worker
  account: "oz513"
  cores: 1 # Some tools are not thread safe, so it's recommended to keep cores and processes = 1
  processes: 1
  memory: "16GB"
  walltime: "01:00:00"

  # A directory for Dask operational files
  local_directory: /fred/oz513/needle_data/dask-scratch
  # A directory for Dask to output its logs
  log_directory: /aphid/scratch-3month/ksmith/needle_logs

  # Commands to execute per-job before running the task
  job_script_prologue:
    - "source /home/ksmith/.needle.dev.env"
    - "module load apptainer"

  job_extra_directives: []
```

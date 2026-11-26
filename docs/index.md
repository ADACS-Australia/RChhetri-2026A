# Needle

--8<-- "README.md:intro"

## Quickstart - OzStar Dev

### SSH Setup

Before beginning, make sure you have an OzStar account and have set up remote SSH using SSH keys.
You should also set an SSH alias for ozstar that looks something like this:

```bash
Host ozstar
    HostName ozstar.swin.edu.au
    User ozstarusername
    IdentityFile ~/.ssh/ozstar
```

The host name is important as Needle will default to using _osztar_ for its deployment functionality.

If you're unfamiliar with setting up host aliases, Swinburne has a helpful [tip](https://supercomputing.swin.edu.au/docs/1-getting_started/Access.html#access-data-mover-nodes) that explains the method.

You can test that it's setup properly by running `ssh ozstar`. If you can login, you've done it right.

### Local Repository Setup and Installation

Now we need to set up a version of the local repository.

If you intend to make and push changes to the codebase, clone the repository with SSH:

```bash
git clone git@github.com:ADACS-Australia/RChhetri-2026A.git needle
```

If you only require read permissions for deployment purposes, you can clone with HTTPS

```bash
https://github.com/ADACS-Australia/RChhetri-2026A.git needle
```

To check that this worked:

```bash
cd needle
git status
```

Now we can proceed to installation. The recommended installation tool for this is [UV](https://docs.astral.sh/uv/), which can be installed in a single line with

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you're familiar with Python and using virtual environments, feel free to use whatever tool you're comfortable with. This guide will assume the use of UV.

From the repository root, install needle with

```bash
uv sync
```

You can make sure it worked with

```bash
uv run needle
```

Which should display a CLI help menu. From here we're mostly interested in the `needle deploy` capability. But first, we need to create three files.

### Config and Env File Setup

Before deploying the code, we need to create three files that drive how Needle will install and run.

#### Environment File

Create a new file called .needle.dev.env. This contains environment variables that are essential for basic functionality. Here is a guide for what the file should look like:

```bash
export PREFECT_LOGGING_EXTRA_LOGGERS=needle
export PREFECT_API_AUTH_STRING="admin:{PASSWORD}"
export PREFECT_LOGGING_LOGGERS_NEEDLE_LEVEL=DEBUG
export PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
export PREFECT_API_URL=https://prefect.ips-askap.cloud.edu.au/api
export NEEDLE_CONFIG=/home/{USERNAME}/software/configs/needle.dev.yaml # pipeline config
export NEEDLE_CLUSTER_CONFIG=/home/{USERNAME}/software/configs/needle_cluster.dev.yaml # compute cluster
```

Be sure to replace the `{}` braced names with their appropriate values:

- PASSWORD: The password to communicate with the prefect server. Should be given by a system administrator. Do not share this!
- USERNAME: Your OzStar username

Save this file as `.needle.dev.env`

#### Needle Config File

The config file contains all of the configuration options for a run. The pipeline will reference this for things such as the location of the CASA ephemeris files, the pipeline log level and module-specific configuration such as flagging and cleaning.

Below is an example config file, feel free to customise it however you want. When you're done, save it as `.needle.dev.yaml` in the repo root.

```yaml
flow:
  overwrite: False
  max_workers: 120
  log_level: INFO
  interval_tasks: 10
  skip_to_deep_clean: True

data:
  work_dir: /fred/oz513/NeedleData/Observations
  download_dir: /fred/oz513/NeedleData/Downloads
  casa_dir: /fred/oz513/CASA

flag:
  quack:
    enabled: True
  tfcrop:
    enabled: True

calibrate_solve:
  setjy:
    standard: "Perley-Butler 2010"
  bandpass: {}
  gaincal: {}

calibrate_apply:
  applycal: {}
  split: {}

shallow_clean: {}
source_find: {}
create_mask: {}
deep_clean:
  auto_mask: 3
  auto_threshold: 0.3
model_subtract: {}
interval_clean:
  auto_mask: 3
  auto_threshold: 0.3
```

#### Needle Cluster Config File

This config file contains all of the required data for the Dask Cluster to run, including the slurm job configuration.

Take special note of the container.image field, bind mounts, slurm directories and job_script_prologues. These will all be important for understanding how Needle runs, where its functionality lies and where its outputs are.

Below is an example config file, feel free to customise it however you want, but note that any of the working directories from Needle Config must be contained in a bind mount. When you're done, save it as `.needle_cluster.dev.yaml` in the repo root.

```yaml
type: slurm

scaling:
  min_workers: 1
  max_workers: 40
  dashboard_port: 8787

container:
  image: /home/{USER}/software/images/needle.sif
  type: apptainer
  binds:
    - /usr/share/zoneinfo/UTC:/etc/localtime
    - /fred/oz513:/fred/oz513
    - /home/{USER}/software:/home/{USER}/software
    - /aphid/scratch-3month/{USER}:/aphid/scratch-3month/{USER}
slurm:
  account: oz513
  cores: 2
  memory: 16GB
  processes: 1
  walltime: "02:00:00"
  local_directory: /aphid/scratch-3month/{USER}/DaskScratch
  log_directory: /aphid/scratch-3month/{USER}/NeedleLogs
  job_script_prologue:
    - "source /home/{USER}/.needle.dev.env"
    - "module load apptainer"
  job_extra_directives: []
```

### Building the Image

To build the image, we require two things installed:

- Docker
- Apptainer

Please see the appropriate documentation on how to install each of these for your operating system. To check that they're both properly installed, you should be able to run the following without issue:

```bash
docker --help
apptainer --help
```

To build the image, we will simply run this from the project root:

```bash
make all
```

This will take some time, as it builds the base image and then the Needle image on top of it. Future runs will be much faster (although still significant - only rebuild if you need to), as it's unlikely the base image will need to be remade.

If you need to rebuild the Needle image, remove the build marker and rerun make:

```bash
rm .needle_built
make all
```

### Deploying

You should now have the following:

- An `ozstar` SSH hostname alias in your SSH configuration
- A needle repository
  - Inside the repository you should have:
  - `.needle.dev.env`
  - `.needle.dev.yaml`
  - `.needle_cluster.dev.yaml`
  - `needle.sif`

Now we can deploy the installation to OzStar:

```bash
uv run needle deploy dev --upload-image
```

You should be prompted for your 2FA passcode. Then the installation will proceed. This can take a few minutes.

When it's done, SSH into OzStar, then run:

```bash
ls -ltrh software .needle.dev.env
```

You should get an output that lists three directories and the `.needle.dev.env` file:

```
-rw-------  1 ksmith curtin 443 Nov 25 18:03 .needle.dev.env

software/needle:
total 435K
-rw-rw-r-- 1 ksmith curtin 249K Nov 25 12:45 uv.lock
drwxrwxr-x 5 ksmith curtin  12K Nov 25 12:45 tests
drwxrwxr-x 2 ksmith curtin  12K Nov 25 12:45 scripts
-rw-rw-r-- 1 ksmith curtin 1.4K Nov 25 12:45 pyproject.toml
-rw-rw-r-- 1 ksmith curtin 1.1K Nov 25 12:45 prefect.yaml
-rw-rw-r-- 1 ksmith curtin 1.6K Nov 25 12:45 mkdocs.yaml
-rw-rw-r-- 1 ksmith curtin  194 Nov 25 12:45 example.prefect.toml
drwxrwxr-x 5 ksmith curtin  13K Nov 25 12:45 docs
drwxrwxr-x 2 ksmith curtin  12K Nov 25 12:45 container
drwxrwxr-x 2 ksmith curtin  12K Nov 25 12:45 bin
-rw-rw-r-- 1 ksmith curtin  842 Nov 25 12:45 README.md
-rw-rw-r-- 1 ksmith curtin  970 Nov 25 12:45 Makefile
drwxrwxr-x 8 ksmith curtin  12K Nov 25 14:54 needle

software/images:
total 885M
-rw-r--r-- 1 ksmith curtin 906M Nov 25 14:23 needle.sif

software/configs:
total 1.0K
-rw-r--r-- 1 ksmith curtin 627 Nov 25 18:03 needle.yaml
-rw-r--r-- 1 ksmith curtin 688 Nov 25 18:03 needle_cluster.yaml
```

You will also have access to the Needle CLI:

```bash
needle --help
```

Subsequent deploy operations will overwrite these files with your local copies.

> Note that the OzStar remote `needle` repository is **not** copied from your local version, it is **cloned** from the remote source and checks out to the **same branch as your local copy**.
> This means that any changes you'd like to test on OzStar must be pushed to your working branch before running the deploy operation.

### Final Touches

It's recommended to source the environment variables in the `.needle.dev.env` file on OzStar. To do this on login, add this line to your `~/.bashrc`:

```bash
source /home/$USER/.needle.dev.env
```

After setting this for the first time, you can apply the change in your current shell with `source ~/.bashrc`.

Now, you can run a quick validation of your `.needle.dev.yaml` config file with

```bash
needle validate
```

You can also display the rendered contents of your config file with

```bash
needle display
```

This can be useful for finding a setting you wish to change.

You can also check that the cluster configuration is functional and ready to submit jobs with

```bash
needle check-cluster
```

This will run a simple test job on the cluster and log the results to needle.log in the working directory.

### Running the Pipeline

As this is a development deployment, it is only intended to perform one-off runs of the pipeline or components of the pipeline in isolation (as opposed to the production version, which automatically downloads and processes data from CASDA).

As such, this guide will assume you have access to the required target and calibrator data files. The pipeline can work with files of the following format:

- `.uvfits`
- `.ms`
- `.mir`

The pipeline expects to find files with roughly this format:
**Target:** `SBXXXXX*beamXX*.(uvfits/ms/mir)`
**Calibrator:** `cal*SBXXXXX*beamXX*.(uvfits/ms/mir)`
Where X represents a digit and * represents a wildcard (any character, any number of times). There does not need to be exactly 5 digits after the 'SB', but there does need to be exactly two after 'beam'.
It is very important that this format is adhered to!

Once you have all of your files in one directory, simply run:

```bash
needle run /path/to/datafiles/directory
```

This will run the pipeline from end-to-end.

### Post-Run Diagnosis

Hopefully, the pipeline ran without too much issue. It will continue running as long as at least one beam has not failed.

The following log files may be of use for diagnosing issues (`{RUNDIR}` represents `/path/to/datafiles/directory` in the previous step):

- `{RUNDIR}/needle.log` - The flow-level logs. The same as what was emitted by stdout during the run.
- `{RUNDIR}/beamXX/beam.log` - The beam-level logs. Logs the output of each task operation for beamXX.
- `/aphid/scratch-3month/$USER/NeedleLogs` - Or whatever you set `.needle_cluster.dev.yaml`'s slurm.log_directory to. Holds logs of the dask workers. Good for debugging resource issues or anything that doesn't show up as an obvious error in the other logs.
- `/fred/oz513/CASA` - Or whatever you set `.needle.dev.yaml`'s `data.casa_dir` to. Holds the logs for CASA operations performed during the pipeline. CASA failures can be hard to diagnose without these.

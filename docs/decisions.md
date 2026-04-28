# Decisions made during the creation of Needle

## Needle Config Path is a predetermined location

**Context**
Upon importing CASA modules, CASA reads its own config at `"$HOME/.casa/config.py"`.
Upon loading, the CASA variables are instantiated. Among these are the `logfile`, `rundata` and `measurespath` variables.
We can script the creation of these in the casa config, but we would like access to the needle config to determine them.
Since casa imports are done before any runtime logic, the needle config should be in a predetermined location so that the casa config can load it.

**Decision**
The needle config should always be available at `"$HOME/.needle.yaml"`

**Pros**
The needle config encompasses any important CASA runtime configuration.
CASA config.py can use any variable set in the needle config.

**Cons**
The user must know to put .needle in the appropriate location (or symlink it). `setup_env.sh` helps with this.

## Use Modular CASA

**Context**
CASA comes in two varieties - the monolithic version (a CLI), and a set of python libraries (modular).
Monolithic CASA contains the entire suite of tools and can only be used via CLI.
CLI use isn't a problem as we intend to do that anyway with Python subprocesses.
Needle only requires `casatasks` and `casatools`

**Decision**
Use the modular version of CASA, rather than the monolithic one.

**Pros**
Modular CASA is much smaller and faster to install. Making container construction and loading faster.

**Cons**
Since we are using the CLI to execute, we must preface all CASA commands with the correct import. E.g.
`from casatasks import flagdata; flagdata()`

## Interval Cleaning Parallelisation

**Context**
As part of the main pipeline we perform an interval clean, which creates images for each of the individual intervals.
This is (was - before this decision) the longest process of the pipeline, taking up over half the time of the whole pipeline run end-to-end.
[WSClean has an option](https://wsclean.readthedocs.io/en/latest/snapshot_imaging.html) for interval cleaning to make it convenient.

**Decision**
Parallelise the interval cleaning so that one process performs the interval clean on only a subsection of the entire dataset.

**Pros**
Massive reduction in total runtime.

**Cons**
The implementation is conceptually not as straight-forward. It also requires careful calculation of the appropriate intervals and renaming of the resulting files.

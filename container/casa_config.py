# https://casadocs.readthedocs.io/en/v6.4.0/api/configuration.html
import os
from pathlib import Path
import time
import yaml  # Not native - make sure this is installed

_USER = os.getlogin()
_CLUSTER_CONFIG = Path(f"/home/{_USER}/.needle_cluster.yaml")
if not _CLUSTER_CONFIG.exists():
    raise FileNotFoundError(f"Expected file does not exist: {_CLUSTER_CONFIG}")
_NEEDLE_CONFIG = Path(f"/home/{_USER}/.needle.yaml")
if not _NEEDLE_CONFIG.exists():
    raise FileNotFoundError(f"Expected file does not exist: {_NEEDLE_CONFIG}")

## Get cluster information - required for log output
with open(_CLUSTER_CONFIG, "r") as f:
    _CFG = yaml.load(f)
try:
    logs_dir = _CFG["log_directory"]
except KeyError:
    raise KeyError(f"Provided file {_CLUSTER_CONFIG} does not have expected field: 'log_directory'")

## Get needle configuration - required for casa measures output
with open(_NEEDLE_CONFIG, "r") as f:
    _CFG = yaml.load(f)
try:
    data_dir = _CFG["data_dir"]
except KeyError:
    raise KeyError(f"Provided file {_NEEDLE_CONFIG} does not have expected field: 'data_dir'")

## CASA Configuration for Needle ##
logfile = f"{logs_dir}/casalog-%s.log" % time.strftime("%Y%m%d-%H", time.localtime())
rundata = f"{data_dir}/.casa"
nologfile = False
log2term = True  # Print the log output directly to the terminal (so it shows up in SLURM .log)
nologger = True
nogui = True
pipeline = True

# CASA wants this to already exist before doing anything
if not os.path.exists(rundata):
    os.mkdir(rundata)

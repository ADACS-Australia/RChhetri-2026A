# This is the configuration file that is run on every casa execution/import
# https://casadocs.readthedocs.io/en/v6.4.0/api/configuration.html
# CASA will silence exception messages, so we print them explicitly when they occur
try:
    import os
    from pathlib import Path
    import time
    import yaml  # Not native - make sure this is installed

    ## Get needle configuration - required for casa measures output
    _NEEDLE_CONFIG = Path(os.environ.get("NEEDLE_CONFIG", Path.home() / Path(".needle.yaml")))
    if not _NEEDLE_CONFIG or not _NEEDLE_CONFIG.exists():
        raise FileNotFoundError(f"Could not find NEEDLE_CONFIG file: {_NEEDLE_CONFIG}")
    with open(_NEEDLE_CONFIG, "r") as f:
        _CFG = yaml.load(f, Loader=yaml.SafeLoader)

    try:
        data_dir = _CFG["data"]["casa_dir"]
    except KeyError:
        raise KeyError(f"Provided file {_NEEDLE_CONFIG} does not have expected field: 'data.casa_dir'")
    try:
        staging_dir = _CFG["data"]["staging_dir"]
    except KeyError:
        raise KeyError(f"Provided file {_NEEDLE_CONFIG} does not have expected field: 'data.staging_dir'")


    ## CASA Configuration for Needle ##
    logs_dir = f"{staging_dir}/casalogs"
    logfile = f"{logs_dir}/casalog-%s.log" % time.strftime("%Y%m%d-%H", time.localtime())
    rundata = f"{data_dir}/.casa"
    measurespath = f"{data_dir}/casadata"
    nologfile = False
    log2term = True  # Print the log output directly to the terminal (so it shows up in SLURM .log)
    nologger = True
    nogui = True
    pipeline = True

    # Group-friendly umask so the group can read/write casa dirs
    os.umask(0o002)
    # Create the working dirs if needed
    for p in (Path(rundata), Path(measurespath), Path(logs_dir)):
        p.mkdir(mode=0o770, parents=True, exist_ok=True)
except Exception as e:
    print("----------------------------------------------")
    print(f"ERROR loading casa config file: {e}")
    print("CASA will attempt to use default settings...")
    print("----------------------------------------------")
    raise (e)

# https://casadocs.readthedocs.io/en/v6.4.0/api/configuration.html
# CASA will silence exception messages, so we print them explicitly when they occur
try:
    import os
    from pathlib import Path
    import time
    import yaml  # Not native - make sure this is installed

    _USER = os.getlogin()
    ## Get needle configuration - required for casa measures output
    _NEEDLE_CONFIG = Path(f"/home/{_USER}/.needle.yaml")

    if not _NEEDLE_CONFIG.exists():
        raise FileNotFoundError(f"Expected file does not exist: {_NEEDLE_CONFIG}")

    with open(_NEEDLE_CONFIG, "r") as f:
        _CFG = yaml.load(f, Loader=yaml.SafeLoader)

    try:
        data_dir = _CFG["flow"]["data_dir"]
    except KeyError:
        raise KeyError(f"Provided file {_NEEDLE_CONFIG} does not have expected field: 'flow.data_dir'")
    logs_dir = f"{data_dir}/logs"

    ## CASA Configuration for Needle ##
    logfile = f"{logs_dir}/casalog-%s.log" % time.strftime("%Y%m%d-%H", time.localtime())
    rundata = f"{data_dir}/.casa"
    measurespath = f"{data_dir}/data"
    nologfile = False
    log2term = True  # Print the log output directly to the terminal (so it shows up in SLURM .log)
    nologger = True
    nogui = True
    pipeline = True

    # Create the working dirs if needed
    for p in (rundata, measurespath, logs_dir):
        if not os.path.exists(p):
            os.mkdir(p)
except Exception as e:
    print(f"Error loading casa config file: {e}")
    raise (e)

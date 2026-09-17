# https://casadocs.readthedocs.io/en/v6.4.0/api/configuration.html
# CASA will silence exception messages, so we print them explicitly when they occur
try:
    import os
    from pathlib import Path
    import time
    import yaml  # Not native - make sure this is installed

    ## Get needle configuration - required for casa measures output
    _USER = os.environ.get("USER") or os.path.pwd.getpwuid(os.getuid()).pw_name
    _HOME = os.environ.get("HOME") or Path(f"/home/{_USER}")
    _NEEDLE_CONFIG = os.environ.get("NEEDLE_CONFIG") or Path(f"{_HOME}/.needle.yaml")
    if not _NEEDLE_CONFIG or not _NEEDLE_CONFIG.exists():
        raise FileNotFoundError(f"Could not find NEEDLE_CONFIG file: {_NEEDLE_CONFIG}")
    with open(_NEEDLE_CONFIG, "r") as f:
        _CFG = yaml.load(f, Loader=yaml.SafeLoader)

    try:
        data_dir = _CFG["data"]["casa_dir"]
    except KeyError:
        raise KeyError(f"Provided file {_NEEDLE_CONFIG} does not have expected field: 'data.casa_dir'")

    ## CASA Configuration for Needle ##
    logs_dir = f"{data_dir}/casalogs"
    logfile = f"{logs_dir}/casalog-%s.log" % time.strftime("%Y%m%d-%H", time.localtime())
    rundata = f"{data_dir}/.casa"
    measurespath = f"{data_dir}/casadata"
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
    print("----------------------------------------------")
    print(f"ERROR loading casa config file: {e}")
    print("CASA will attempt to use default settings...")
    print("----------------------------------------------")
    raise (e)

# --- CASA Configuration for High-Performance Computing (HPC) ---

# In a cluster, point this to a shared, read-only location to save space.
# measurespath = "/shared/reference/casadata"
CASA_HOME = "~/.casa"

# Disable the GUI logger (which fails in headless environments)
nologger = True
# Print the log output directly to the terminal (so it shows up in SLURM .log)
log2term = True
# Define a standard naming convention for the log file
logfile = f"{CASA_HOME}/.casa/logs/casa_runtime.log"

pipeline = True
# Turn off automatic updating
# measures_auto_update = False
# data_auto_update = False


# Disable telemetry to prevent nodes from trying to "call home" via internet
# telemetry_enabled = False
# crashreporter_enabled = False

# Set a temporary directory for large scratch files (use fast local SSD if available)
# ipython_dir = '/tmp/casa_work'

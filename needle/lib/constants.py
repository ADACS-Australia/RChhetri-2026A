# File to store any constants required at the orchestrator level
WORKFLOW_UMASK = 0o002  # Umask to allow group write/exec access


# Log/Print formatting constants
DIM = "\033[2m"
BOLD = "\033[1m"
FMT_RST = "\033[0m"  # Format Reset
# Colours
CYAN = "\033[36m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"


# Key used to store if the model subtraction has been executed
MODEL_SUBTRACT_KEY = "MODEL_SUBTRACTED"

# Needle flow level logger name
NEEDLE_LOGGER = "needle"
NEEDLE_WATCHER_LOGGER = "needle.watcher"
NEEDLE_BEAM_LOGGER_FMT = "needle.beam.{beam_dir}"
PREFECT_RUN_LOGGERS = ("prefect.flow_runs", "prefect.task_runs")

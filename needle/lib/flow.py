from pathlib import Path
import hashlib
import json
from datetime import timedelta

from prefect.cache_policies import TASK_SOURCE, CacheKeyFnPolicy


CONTAINER_DATA_DIR = Path("/data")


def _pydantic_cache_key(_, arguments) -> str:
    """Stable cache key that handles Pydantic models, Paths, and primitives."""

    def serialise(v):
        if hasattr(v, "model_dump"):
            return v.model_dump(mode="json")
        if isinstance(v, Path):
            return str(v)
        return v

    stable = {k: serialise(v) for k, v in arguments.items()}
    key = hashlib.md5(json.dumps(stable, sort_keys=True).encode()).hexdigest()
    return key


# Cache results based on their INPUTS and the SOURCE code of the task
PYDANTIC_MODEL_CACHE = CacheKeyFnPolicy(cache_key_fn=_pydantic_cache_key)
CACHE_STRATEGY = PYDANTIC_MODEL_CACHE + TASK_SOURCE
CACHE_EXPIRATION = timedelta(days=1)

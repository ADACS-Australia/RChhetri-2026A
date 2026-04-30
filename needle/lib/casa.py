from contextlib import contextmanager
from pathlib import Path


def get_table():
    """Imports casatools and returns a table object. This exists to avoid top-level CASA importing - thereby
    allowing this module to be loaded without having CASA installed. This is useful for orchestration."""
    try:
        from casatools import table

        return table()
    except ImportError:
        raise RuntimeError(
            "casatools is required to read an MS directly. Install it or load from JSON via MSInfo.from_json()."
        )


@contextmanager
def open_table(path: Path | str):
    tb = get_table()
    tb.open(str(path))
    try:
        yield tb
    finally:
        tb.close()


def get_msmetadata():
    """Imports casatools and returns a msmetadata object. This exists to avoid top-level CASA importing - thereby
    allowing this module to be loaded without having CASA installed. This is useful for orchestration."""
    try:
        from casatools import msmetadata

        return msmetadata()
    except ImportError:
        raise RuntimeError(
            "casatools is required to read an MS directly. Install it or load from JSON via MSInfo.from_json()."
        )


@contextmanager
def open_msmetadata(path: Path | str):
    md = get_msmetadata()
    md.open(str(path))
    try:
        yield md
    finally:
        md.close()

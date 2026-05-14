from contextlib import contextmanager
from pathlib import Path


def get_table():
    """Imports casatools and returns a table object. This exists to avoid top-level CASA importing - thereby
    allowing this module to be loaded without having CASA installed.

    :raises ImportError: Raised if casatools is not able to be imported
    """
    try:
        from casatools import table

        return table()
    except ImportError:
        raise ImportError("casatools is required to read an MS directly.")


@contextmanager
def open_table(path: Path | str):
    """Context manager that opens and yields a CASA table object

    :param path: The path to the measurement set to open as a table object
    """
    tb = get_table()
    tb.open(str(path))
    try:
        yield tb
    finally:
        tb.close()


def get_msmetadata():
    """Imports casatools and returns a msmetadata object. This exists to avoid top-level CASA importing - thereby
    allowing this module to be loaded without having CASA installed.

    :raises ImportError: Raised if casatools is not able to be imported
    """
    try:
        from casatools import msmetadata

        return msmetadata()
    except ImportError:
        raise ImportError("casatools is required to read an MS directly.")


@contextmanager
def open_msmetadata(path: Path | str):
    """Context manager that opens and yields a CASA table object

    :param path: The path to the measurement set to open as a msmetadata object
    """
    md = get_msmetadata()
    md.open(str(path))
    try:
        yield md
    finally:
        md.close()

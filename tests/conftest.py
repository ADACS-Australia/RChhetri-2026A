import os

import pytest


@pytest.fixture
def no_umask():
    """Temporarily clears the process umask so mkdir(mode=...) isn't masked,
    letting tests assert the exact permission bits the code requests."""
    old_umask = os.umask(0)
    yield
    os.umask(old_umask)

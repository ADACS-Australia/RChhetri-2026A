import pytest

# Adjust to wherever the function lives
from needle.lib.slurm import memory_string_to_int


@pytest.mark.parametrize(
    "mem_str, expected",
    [
        ("4GB", 4 * 1024**3),
        ("256M", 256 * 1024**2),
        ("2kb", 2 * 1024),  # lowercase handled
        ("1.5 GB", int(1.5 * 1024**3)),  # decimals and inner spaces
        ("  8G  ", 8 * 1024**3),  # surrounding whitespace
        ("10B", 10),  # explicit bytes
        ("512", 512),  # no unit defaults to bytes
    ],
)
def test_memory_string_to_int_valid(mem_str, expected):
    assert memory_string_to_int(mem_str) == expected


@pytest.mark.parametrize("mem_str", ["abcGB", "GB", "1TB", "four gigs"])
def test_memory_string_to_int_invalid_raises(mem_str):
    with pytest.raises(ValueError, match="Could not convert memory string"):
        memory_string_to_int(mem_str)

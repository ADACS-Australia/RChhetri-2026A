import pytest

from needle.config.imstack import ImageStackConfig


def test_defaults():
    cfg = ImageStackConfig()
    assert cfg.suffixes == ["image"]
    assert cfg.pols == ["XX"]
    assert cfg.stamp_size == 16
    assert cfg.start == 0
    assert cfg.bands is None


def test_empty_bands_become_none():
    assert ImageStackConfig(bands=[]).bands is None


def test_duplicate_bands_raise():
    with pytest.raises(ValueError, match="Duplicate bands"):
        ImageStackConfig(bands=["low", "low"])


def test_invalid_start_raises():
    with pytest.raises(ValueError, match="start"):
        ImageStackConfig(start=-1)


def test_invalid_time_interval_raises():
    with pytest.raises(ValueError, match="time_interval"):
        ImageStackConfig(time_interval=0)


def test_duplicate_suffixes_raise():
    with pytest.raises(ValueError, match="Duplicate suffixes"):
        ImageStackConfig(suffixes=["image", "image"])


def test_reserved_suffix_raises():
    with pytest.raises(ValueError, match="reserved dataset names"):
        ImageStackConfig(suffixes=["beam"])


def test_invalid_stamp_size_raises():
    with pytest.raises(ValueError, match="stamp_size"):
        ImageStackConfig(stamp_size=0)


def test_unsupported_channel_count_raises():
    with pytest.raises(ValueError, match="single channel"):
        ImageStackConfig(n_channels=2)

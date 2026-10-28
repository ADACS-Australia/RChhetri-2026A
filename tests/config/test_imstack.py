import pytest

from needle.config.imstack import ImageStackConfig


def test_defaults():
    cfg = ImageStackConfig()
    assert cfg.stamp_size == 16
    assert cfg.pb_threshold == 0.1
    assert cfg.time_interval == 1.0
    assert cfg.dtype == "float16"
    assert cfg.cache_size_mb == 1024
    assert cfg.hdu == 0


def test_invalid_hdu_raises():
    with pytest.raises(ValueError, match="hdu"):
        ImageStackConfig(hdu=-1)


def test_invalid_time_interval_raises():
    with pytest.raises(ValueError, match="time_interval"):
        ImageStackConfig(time_interval=0)


def test_invalid_pb_threshold_raises():
    with pytest.raises(ValueError, match="pb_threshold"):
        ImageStackConfig(pb_threshold=1.1)


def test_invalid_stamp_size_raises():
    with pytest.raises(ValueError, match="stamp_size"):
        ImageStackConfig(stamp_size=0)


def test_invalid_cache_size_raises():
    with pytest.raises(ValueError, match="cache_size_mb"):
        ImageStackConfig(cache_size_mb=0)

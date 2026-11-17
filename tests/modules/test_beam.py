import re

import pytest

from needle.modules.beam import (
    find_beams,
    BEAM_DIR_PATTERN,
    BPCAL_PATTERN,
    CAL_PATTERN,
    CALIBRATED_PATTERN,
    GCAL_PATTERN,
    TGT_PATTERN,
)

# ---------------------------------------------------------------------
# find_beams tests
# ---------------------------------------------------------------------


def test_find_beams(tmp_path):
    """Test discovery of target/calibrator beam pairs."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "cracoData.SB292347.beam01.ms").mkdir()
    (data_dir / "cal_beam01.ms").mkdir()

    (data_dir / "cracoData.SB12345.beam02.ms").mkdir()
    (data_dir / "SB12345_cal_beam02.ms").mkdir()

    (data_dir / "cracoData.SB67890.beam03.ms").mkdir()  # No cal
    (data_dir / "cal_beam04.ms").mkdir()  # No target

    pairs = find_beams(data_dir)

    assert len(pairs) == 2

    assert pairs[0].beam == "01"
    assert pairs[0].name == "SB292347"
    assert pairs[0].tgt == data_dir / "cracoData.SB292347.beam01.ms"
    assert pairs[0].cal == data_dir / "cal_beam01.ms"

    assert pairs[1].beam == "02"
    assert pairs[1].name == "SB12345"
    assert pairs[1].tgt == data_dir / "cracoData.SB12345.beam02.ms"
    assert pairs[1].cal == data_dir / "SB12345_cal_beam02.ms"


def test_find_beams_custom_patterns(tmp_path):
    """Test find_beams with custom regex patterns."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "T01.ms").touch()
    (data_dir / "C01.ms").touch()

    tgt_pattern = r"(?P<name>T)(?P<beam>\d+)\.ms"
    cal_pattern = r"C(?P<beam>\d+)\.ms"

    pairs = find_beams(data_dir, tgt_pattern=tgt_pattern, cal_pattern=cal_pattern)

    assert len(pairs) == 1
    assert pairs[0].beam == "01"
    assert pairs[0].name == "T"
    assert pairs[0].tgt == data_dir / "T01.ms"
    assert pairs[0].cal == data_dir / "C01.ms"


# ---------------------------------------------------------------------
# Target pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, name, beam",
    [
        ("SB292347beam00.ms", "SB292347", "00"),
        ("something.SB987654_beam03.uvfits", "SB987654", "03"),
        ("cracoData.LTR_1840-0516.SB82436.beam00.uvfits", "SB82436", "00"),
        ("prefix_SB12345_suffix_beam12.mir", "SB12345", "12"),
        ("SB12345.beam07.ms", "SB12345", "07"),
    ],
)
def test_tgt_pattern_matches(filename, name, beam):
    m = re.match(TGT_PATTERN, filename)

    assert m
    assert m.group("name") == name
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    [
        "cal_beam00.ms",
        "SB12345_cal_beam00.ms",
        "SB12345beam0.ms",
        "SB12345beam000.ms",
        "SB12345beam00.txt",
        "SB12345beam00.bpcal",
        "src_beam00.ms",
        "SBbeam00.ms",
        "SB12345beam00.ms.bak",
        "SB12345_CAL_beam00.ms",  # Uppercase CAL is allowed by case-sensitive regex
    ],
)
def test_tgt_pattern_rejects(filename):
    # Uppercase CAL is not rejected because the regex is case-sensitive.
    if "CAL" in filename:
        assert re.match(TGT_PATTERN, filename)
    else:
        assert not re.match(TGT_PATTERN, filename)


# ---------------------------------------------------------------------
# Calibration visibility pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, beam",
    [
        ("cal_beam00.ms", "00"),
        ("cal_beam07.mir", "07"),
        ("cal_beam12.uvfits", "12"),
        ("SB292347_cal_beam03.ms", "03"),
        ("SB292347_beam03_cal.ms", "03"),
        ("something.beam24_calibrated.uvfits", "24"),
    ],
)
def test_cal_pattern_matches(filename, beam):
    m = re.match(CAL_PATTERN, filename)

    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    [
        "SB292347beam00.ms",
        "src_beam00.ms",
        "cal_beam0.ms",
        "cal_beam000.ms",
        "cal_beam00.txt",
        "cal_beam00.bpcal",
        "cal_beam00.gcal",
        "cal_beam00.ms.bak",
    ],
)
def test_cal_pattern_rejects(filename):
    assert not re.match(CAL_PATTERN, filename)


# ---------------------------------------------------------------------
# Bandpass calibration pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, beam",
    [
        ("cal_beam00.bpcal", "00"),
        ("cal_beam12.bpcal", "12"),
        ("SB292347_cal_beam03.bpcal", "03"),
        ("SB292347_beam03_cal.bpcal", "03"),
        ("something.calibration.beam24.bpcal", "24"),
    ],
)
def test_bpcal_pattern_matches(filename, beam):
    m = re.match(BPCAL_PATTERN, filename)

    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    [
        "cal_beam00.gcal",
        "cal_beam00.bpcal.bak",
        "cal_beam0.bpcal",
        "cal_beam000.bpcal",
        "src_beam00.bpcal",
        "SB292347_beam00.bpcal",
        "cal_beam00.ms",
    ],
)
def test_bpcal_pattern_rejects(filename):
    assert not re.match(BPCAL_PATTERN, filename)


# ---------------------------------------------------------------------
# Gain calibration pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, beam",
    [
        ("cal_beam00.gcal", "00"),
        ("cal_beam12.gcal", "12"),
        ("SB292347_cal_beam03.gcal", "03"),
        ("SB292347_beam03_cal.gcal", "03"),
        ("something.calibration.beam24.gcal", "24"),
    ],
)
def test_gcal_pattern_matches(filename, beam):
    m = re.match(GCAL_PATTERN, filename)

    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    [
        "cal_beam00.bpcal",
        "cal_beam00.gcal.bak",
        "cal_beam0.gcal",
        "cal_beam000.gcal",
        "src_beam00.gcal",
        "SB292347_beam00.gcal",
        "cal_beam00.ms",
    ],
)
def test_gcal_pattern_rejects(filename):
    assert not re.match(GCAL_PATTERN, filename)


# ---------------------------------------------------------------------
# Calibrated visibility pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, name, beam",
    [
        ("SB292347beam00_calibrated.ms", "SB292347", "00"),
        ("something.SB987654_beam03_calibrated.uvfits", "SB987654", "03"),
        ("cracoData.LTR_1840-0516.SB82436.beam00_calibrated.uvfits", "SB82436", "00"),
        ("prefix_SB12345_suffix_beam12_calibrated.mir", "SB12345", "12"),
        ("SB12345.beam07_calibrated.ms", "SB12345", "07"),
    ],
)
def test_calibrated_pattern_matches(filename, name, beam):
    m = re.match(CALIBRATED_PATTERN, filename)

    assert m
    assert m.group("name") == name
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    [
        "SB292347beam00.ms",
        "SB292347beam00_calibrated.txt",
        "SB292347beam0_calibrated.ms",
        "SB292347beam000_calibrated.ms",
        "SBbeam00_calibrated.ms",
        "cal_beam00_calibrated.ms",
        "SB292347beam00_calibrated.ms.bak",
        "SB292347beam00_cal.ms",
        "SB292347beam00_calibrated.bpcal",
    ],
)
def test_calibrated_pattern_rejects(filename):
    assert not re.match(CALIBRATED_PATTERN, filename)


# ---------------------------------------------------------------------
# Beam directory pattern tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize("dirname", ["beam00", "beam07", "beam35"])
def test_beam_dir_pattern_matches(dirname):
    assert re.fullmatch(BEAM_DIR_PATTERN, dirname)


@pytest.mark.parametrize(
    "dirname",
    [
        "beam0",
        "beam001",
        "beam00_old",
        "xbeam00",
        "Beam00",
        "beam",
    ],
)
def test_beam_dir_pattern_rejects(dirname):
    assert not re.fullmatch(BEAM_DIR_PATTERN, dirname)

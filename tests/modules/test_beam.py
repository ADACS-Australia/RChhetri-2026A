import pytest
import re

from needle.modules.beam import find_beam_pairs, BEAM_DIR_PATTERN, BPCAL_PATTERN, CAL_PATTERN, GCAL_PATTERN, TGT_PATTERN


def test_find_beam_pairs(tmp_path):
    """Test discovery of beam pairs from directory patterns."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "target_beam01.ms").mkdir()
    (data_dir / "cal_beam01.ms").mkdir()
    (data_dir / "target_beam02.ms").mkdir()
    (data_dir / "cal_beam02.ms").mkdir()
    (data_dir / "target_beam03.ms").mkdir()  # No cal
    (data_dir / "cal_beam04.ms").mkdir()  # No target

    pairs = find_beam_pairs(data_dir)
    assert len(pairs) == 2
    assert pairs[0].beam == "01"
    assert pairs[0].tgt == data_dir / "target_beam01.ms"
    assert pairs[0].cal == data_dir / "cal_beam01.ms"
    assert pairs[1].beam == "02"
    assert pairs[1].tgt == data_dir / "target_beam02.ms"
    assert pairs[1].cal == data_dir / "cal_beam02.ms"


def test_find_beam_pairs_custom_patterns(tmp_path):
    """Test find_beam_pairs with custom regex patterns."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "T01.ms").touch()
    (data_dir / "C01.ms").touch()

    tgt_pattern = r"T(?P<beam>\d+)\.ms"
    cal_pattern = r"C(?P<beam>\d+)\.ms"

    pairs = find_beam_pairs(data_dir, tgt_pattern=tgt_pattern, cal_pattern=cal_pattern)
    assert len(pairs) == 1
    assert pairs[0].beam == "01"
    assert pairs[0].tgt == data_dir / "T01.ms"
    assert pairs[0].cal == data_dir / "C01.ms"


## Regex tests
@pytest.mark.parametrize(
    "filename, name, beam",
    [
        ("src_beam00.ms", "src", "00"),
        ("src_beam07.mir", "src", "07"),
        ("src_beam12.uvfits", "src", "12"),
        ("my_src_beam03.ms", "my_src", "03"),
    ],
)
def test_tgt_pattern_matches(filename, name, beam):
    m = re.match(TGT_PATTERN, filename)
    assert m
    assert (m.group("name"), m.group("beam")) == (name, beam)


@pytest.mark.parametrize(
    "filename",
    ["cal_beam00.ms", "src_beam00.ms.bak", "src_beam0.ms", "src_beam000.ms", "src_beam00.txt", "src_beam00.bpcal"],
)
def test_tgt_pattern_rejects(filename):
    assert not re.match(TGT_PATTERN, filename)


@pytest.mark.parametrize(
    "filename, beam", [("cal_beam00.ms", "00"), ("cal_beam07.mir", "07"), ("cal_beam12.uvfits", "12")]
)
def test_cal_pattern_matches(filename, beam):
    m = re.match(CAL_PATTERN, filename)
    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename",
    ["src_beam00.ms", "xcal_beam00.ms", "cal_beam00.ms.bak", "cal_beam0.ms", "cal_beam00.bpcal", "cal_beam00.gcal"],
)
def test_cal_pattern_rejects(filename):
    assert not re.match(CAL_PATTERN, filename)


@pytest.mark.parametrize("filename, beam", [("cal_beam00.bpcal", "00"), ("cal_beam12.bpcal", "12")])
def test_bpcal_pattern_matches(filename, beam):
    m = re.match(BPCAL_PATTERN, filename)
    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename", ["cal_beam00.gcal", "cal_beam00.bpcal.bak", "cal_beam0.bpcal", "src_beam00.bpcal", "cal_beam00.ms"]
)
def test_bpcal_pattern_rejects(filename):
    assert not re.match(BPCAL_PATTERN, filename)


@pytest.mark.parametrize("filename, beam", [("cal_beam00.gcal", "00"), ("cal_beam12.gcal", "12")])
def test_gcal_pattern_matches(filename, beam):
    m = re.match(GCAL_PATTERN, filename)
    assert m
    assert m.group("beam") == beam


@pytest.mark.parametrize(
    "filename", ["cal_beam00.bpcal", "cal_beam00.gcal.bak", "cal_beam0.gcal", "src_beam00.gcal", "cal_beam00.ms"]
)
def test_gcal_pattern_rejects(filename):
    assert not re.match(GCAL_PATTERN, filename)


@pytest.mark.parametrize("dirname", ["beam00", "beam07", "beam35"])
def test_beam_dir_pattern_matches(dirname):
    assert re.fullmatch(BEAM_DIR_PATTERN, dirname)


@pytest.mark.parametrize("dirname", ["beam0", "beam001", "beam00_old", "xbeam00", "Beam00", "beam"])
def test_beam_dir_pattern_rejects(dirname):
    assert not re.fullmatch(BEAM_DIR_PATTERN, dirname)

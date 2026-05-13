from needle.modules.beam import find_beam_pairs


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

import pytest

from needle.config.beam import BeamPair
from needle.config.calibrate import CalibrationSolution


@pytest.fixture
def mock_tgt(tmp_path):
    tgt = tmp_path / "tgt.ms"
    tgt.mkdir()
    return tgt


@pytest.fixture
def mock_cal_path(tmp_path):
    cal = tmp_path / "cal.ms"
    cal.mkdir()
    return cal


@pytest.fixture
def mock_cal_solution(tmp_path):
    gcal = tmp_path / "cal.gcal"
    bpcal = tmp_path / "cal.bpcal"
    gcal.mkdir()
    bpcal.mkdir()
    return CalibrationSolution(gcal=gcal, bpcal=bpcal)


def test_move_tgt(tmp_path, mock_tgt, mock_cal_path):
    pair = BeamPair(beam="00", tgt=mock_tgt, cal=mock_cal_path)
    new_dir = tmp_path / "new_location"

    pair.move_tgt(new_dir)

    assert pair.tgt == new_dir / "tgt.ms"
    assert pair.tgt.exists()
    assert not mock_tgt.exists()


def test_move_cal_with_path(tmp_path, mock_tgt, mock_cal_path):
    pair = BeamPair(beam="00", tgt=mock_tgt, cal=mock_cal_path)
    new_dir = tmp_path / "new_location"

    pair.move_cal(new_dir)

    assert pair.cal == new_dir / "cal.ms"
    assert pair.cal.exists()
    assert not mock_cal_path.exists()


def test_move_cal_with_solution(tmp_path, mock_tgt, mock_cal_solution):
    pair = BeamPair(beam="00", tgt=mock_tgt, cal=mock_cal_solution)
    new_dir = tmp_path / "new_location"
    orig_gcal, orig_bpcal = mock_cal_solution.gcal, mock_cal_solution.bpcal

    pair.move_cal(new_dir)

    assert pair.cal.gcal == new_dir / "cal.gcal"
    assert pair.cal.bpcal == new_dir / "cal.bpcal"
    assert pair.cal.gcal.exists()
    assert pair.cal.bpcal.exists()
    assert not orig_gcal.exists()
    assert not orig_bpcal.exists()


def test_move_files_moves_both_tgt_and_cal(tmp_path, mock_tgt, mock_cal_path):
    pair = BeamPair(beam="00", tgt=mock_tgt, cal=mock_cal_path)
    new_dir = tmp_path / "new_location"

    pair.move_files(new_dir)

    assert pair.tgt == new_dir / "tgt.ms"
    assert pair.cal == new_dir / "cal.ms"
    assert pair.tgt.exists()
    assert pair.cal.exists()

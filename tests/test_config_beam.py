import pytest
from needle.config.beam import BeamPair, MSBeamPair


def test_beam_pair_move_files(tmp_path):
    """Test BeamPair file movement."""
    tgt = tmp_path / "tgt.ms"
    cal = tmp_path / "cal.ms"
    tgt.touch()
    cal.touch()
    bp = BeamPair(beam="01", tgt=tgt, cal=cal)

    new_dir = tmp_path / "beam01"
    bp.move_files(new_dir)

    assert (new_dir / "tgt.ms").exists()
    assert (new_dir / "cal.ms").exists()
    assert bp.tgt == new_dir / "tgt.ms"
    assert bp.cal == new_dir / "cal.ms"


def test_ms_beam_pair_validation(tmp_path):
    """Test validation logic for MSBeamPair."""
    tgt = tmp_path / "tgt.ms"
    cal = tmp_path / "cal.ms"
    tgt.mkdir()
    cal.mkdir()

    # Valid
    MSBeamPair(beam="01", tgt=tgt, cal=cal)

    # Missing suffix
    tgt_bad = tmp_path / "tgt.fits"
    tgt_bad.touch()
    with pytest.raises(ValueError, match="target must be a measurement set"):
        MSBeamPair(beam="01", tgt=tgt_bad, cal=cal)

    # Missing file
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="does not exist"):
        MSBeamPair(beam="01", tgt=tmp_path / "missing.ms", cal=cal)

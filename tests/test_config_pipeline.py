import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from needle.config.pipeline import PipelineFlowConfig, NeedleConfig
from needle.config.beam import BeamPair, MSBeamPair


def test_beam_pair_dir(tmp_path):
    """Test BeamPair directory generation and setup."""
    parent = tmp_path / "output"
    parent.mkdir()
    bp = BeamPair(beam="01", tgt=Path("tgt.ms"), cal=Path("cal.ms"), parent_dir=parent)
    assert bp.beam_dir == parent / "beam01"

    assert not bp.beam_dir.exists()
    bp.setup_beam_dir()
    assert bp.beam_dir.exists()


def test_ms_beam_pair_validation(tmp_path):
    """Test validation logic for MSBeamPair."""
    tgt = tmp_path / "tgt.ms"
    cal = tmp_path / "cal.ms"
    tgt.mkdir()
    cal.mkdir()

    # Valid
    MSBeamPair(beam="01", tgt=tgt, cal=cal, parent_dir=tmp_path)

    # Missing suffix
    tgt_bad = tmp_path / "tgt.fits"
    tgt_bad.touch()
    with pytest.raises(ValueError, match="target must be a measurement set"):
        MSBeamPair(beam="01", tgt=tgt_bad, cal=cal, parent_dir=tmp_path)

    # Missing file
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="does not exist"):
        MSBeamPair(beam="01", tgt=tmp_path / "missing.ms", cal=cal, parent_dir=tmp_path)


def test_pipeline_flow_config_beam_pairs(tmp_path):
    """Test discovery of beam pairs from directory patterns."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "target_01.ms").mkdir()
    (data_dir / "cal_01.ms").mkdir()
    (data_dir / "target_02.ms").mkdir()
    (data_dir / "cal_02.ms").mkdir()
    (data_dir / "target_03.ms").mkdir()  # No cal

    config = PipelineFlowConfig(
        tgt_pattern=r"target_(?P<beam>\d+)\.ms",
        cal_pattern=r"cal_(?P<beam>\d+)\.ms",
        data_dir=data_dir,
        overwrite=False,
    )

    pairs = config.beam_pairs
    assert len(pairs) == 2
    assert pairs[0].beam == "01"
    assert pairs[1].beam == "02"
    assert config.beams_dir == data_dir / "beams"


def test_pipeline_flow_config_invalid_log_level():
    """Test validation of log level in PipelineFlowConfig."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="log_level must be one of"):
        PipelineFlowConfig(tgt_pattern=".*", cal_pattern=".*", data_dir=Path("."), overwrite=False, log_level="INVALID")


def test_get_config_no_file():
    """Test error handling when the needle config file is missing."""
    # Mock Path.home to return a specific directory
    mock_home = Path("/mock/home")

    with patch("pathlib.Path.home", return_value=mock_home):
        # We also need to ensure exists() returns False for this path
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Expected file /mock/home/.needle.yaml does not exist"):
                NeedleConfig.get_config()


def test_get_config_success():
    """Test successful loading of the needle pipeline configuration."""
    mock_home = Path("/mock/home")
    mock_pipeline_config = MagicMock(spec=NeedleConfig)

    with patch("pathlib.Path.home", return_value=mock_home):
        with patch("pathlib.Path.exists", return_value=True):
            with patch("needle.config.pipeline.NeedleConfig.from_yaml", return_value=mock_pipeline_config):
                config = NeedleConfig.get_config()
                assert config == mock_pipeline_config

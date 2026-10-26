from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits

from needle.config.imstack import ImageStackConfig
from needle.modules.imstack import (
    ImageStack,
    ImageStackContext,
    create_image_stack,
    pix2stamp,
    sault_beam,
)


def _header() -> fits.Header:
    header = fits.Header()
    header["BMAJ"] = 0.01
    header["BMIN"] = 0.01
    header["CDELT1"] = -0.001
    header["CDELT2"] = 0.001
    header["CRVAL1"] = 10.0
    header["CRVAL2"] = -45.0
    header["CRPIX1"] = 4.0
    header["CRPIX2"] = 4.0
    header["CTYPE1"] = "RA---TAN"
    header["CTYPE2"] = "DEC--TAN"
    return header


@pytest.fixture
def stack_dir(tmp_path: Path) -> Path:
    """A directory of two timesteps of 8x8 images and a matching primary beam."""
    header = _header()
    for t in range(2):
        data = np.full((8, 8), float(t + 1), dtype=np.float32)
        h = header.copy()
        h["DATE-OBS"] = f"2020-01-01T00:00:0{t}.000"
        fits.writeto(tmp_path / f"obs-t{t:04d}-image.fits", data, h, overwrite=True)
    fits.writeto(tmp_path / "obs-XX-beam.fits", np.ones((8, 8), dtype=np.float32), header, overwrite=True)
    return tmp_path


def _ctx(stack_dir: Path, **kwargs) -> ImageStackContext:
    return ImageStackContext(
        cfg=ImageStackConfig(suffixes=["image"], pols=["XX"], stamp_size=4),
        obsid="obs",
        input_dir=stack_dir,
        n_timesteps=2,
        **kwargs,
    )


def test_create_and_query(stack_dir):
    output = create_image_stack(_ctx(stack_dir))
    assert output.exists()

    with ImageStack(output) as stack:
        assert stack.data.shape == (1, 8, 8, 1, 2)
        assert stack.n_pol == 1
        assert np.allclose(stack.pix2ts(4, 4), [1.0, 2.0])
        assert np.allclose(stack.pix2ts(4, 4, correct=False), [1.0, 2.0])
        assert stack.pix2cube(4, 4, 3).shape == (3, 3, 2)
        assert stack.get_beam().shape == (8, 8)
        assert stack.get_interval() == 1.0


def test_check_filenames_only_does_not_write(stack_dir):
    output = create_image_stack(_ctx(stack_dir, check_filenames_only=True))
    assert not output.exists()


def test_allow_missing_truncates(stack_dir):
    (stack_dir / "obs-t0001-image.fits").unlink()
    ctx = ImageStackContext(
        cfg=ImageStackConfig(suffixes=["image"], pols=["XX"], stamp_size=4, allow_missing=True),
        obsid="obs",
        input_dir=stack_dir,
        n_timesteps=2,
    )
    output = create_image_stack(ctx)
    with ImageStack(output) as stack:
        assert stack.data.shape[-1] == 1


def test_missing_file_raises(stack_dir):
    (stack_dir / "obs-t0001-image.fits").unlink()
    with pytest.raises(FileNotFoundError, match="obs-t0001-image.fits"):
        create_image_stack(_ctx(stack_dir))


def test_existing_output_raises_without_overwrite(stack_dir):
    ctx = _ctx(stack_dir)
    create_image_stack(ctx)
    with pytest.raises(FileExistsError):
        create_image_stack(ctx)


def test_skip_beam_queries_without_correction(stack_dir):
    ctx = ImageStackContext(
        cfg=ImageStackConfig(suffixes=["image"], pols=["XX"], stamp_size=4, skip_beam=True),
        obsid="obs",
        input_dir=stack_dir,
        n_timesteps=2,
    )
    output = create_image_stack(ctx)
    with ImageStack(output) as stack:
        assert not stack.has_beam
        assert np.allclose(stack.pix2ts(4, 4), [1.0, 2.0])


def test_config_start_and_interval(stack_dir):
    cfg = ImageStackConfig(suffixes=["image"], pols=["XX"], stamp_size=4, start=1, time_interval=4.0)
    output = create_image_stack(ImageStackContext(cfg=cfg, obsid="obs", input_dir=stack_dir, n_timesteps=1))
    with ImageStack(output) as stack:
        assert np.allclose(stack.pix2ts(4, 4), [2.0])
        assert stack.get_interval() == 4.0


def test_config_prefix_changes_filenames(stack_dir):
    for t in range(2):
        (stack_dir / f"obs-t{t:04d}-image.fits").rename(stack_dir / f"obs_p-t{t:04d}-image.fits")
    (stack_dir / "obs-XX-beam.fits").rename(stack_dir / "obs_p-XX-beam.fits")
    cfg = ImageStackConfig(suffixes=["image"], pols=["XX"], stamp_size=4, prefix="p")
    output = create_image_stack(ImageStackContext(cfg=cfg, obsid="obs", input_dir=stack_dir, n_timesteps=2))
    with ImageStack(output) as stack:
        assert np.allclose(stack.pix2ts(4, 4), [1.0, 2.0])


def test_pix2stamp_doctest_cases():
    assert pix2stamp(1.6, 3.2, 1) == (slice(2, 3), slice(3, 4))
    assert pix2stamp(1.6, 3.2, 2) == (slice(1, 3), slice(3, 5))
    assert pix2stamp(1.6, 3.2, 3) == (slice(1, 4), slice(2, 5))


def test_sault_beam_preserves_spatial_axes_for_single_pol():
    beam = np.ones((1, 8, 8))
    result = sault_beam(beam, np.ones(1))
    assert result.shape == (8, 8)

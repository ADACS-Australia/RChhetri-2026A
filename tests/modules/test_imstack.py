from pathlib import Path

import h5py
import numpy as np
import pytest
from astropy.io import fits

from needle.config.imstack import ImageStackConfig
from needle.lib.imstack import ImageStack, pix2stamp, sault_beam, sault_weight
from needle.modules.imstack import ImageStackContext, create_image_stack

PREFIX = "beam00_calibrated_intervals"
SIZE = 8


def _header(date_obs: str = "2020-01-01T00:00:00.000") -> fits.Header:
    header = fits.Header()
    header["BMAJ"] = 0.01
    header["BMIN"] = 0.01
    header["CDELT1"] = -0.001
    header["CDELT2"] = 0.001
    header["CRVAL1"] = 10.0
    header["CRVAL2"] = -45.0
    header["CRPIX1"] = 4.5
    header["CRPIX2"] = 4.5
    header["CTYPE1"] = "RA---TAN"
    header["CTYPE2"] = "DEC--TAN"
    header["DATE-OBS"] = date_obs
    return header


def _write(path: Path, value: float, date_obs: str = "2020-01-01T00:00:00.000", **header_items) -> None:
    header = _header(date_obs)
    for key, item in header_items.items():
        header[key] = item
    fits.writeto(path, np.full((SIZE, SIZE), value, dtype=np.float32), header, overwrite=True)


def _image_name(time: int) -> str:
    return f"obs_{PREFIX}-t{time:04d}-image.fits"


def _beam_name() -> str:
    return f"obs_{PREFIX}-beam.fits"


def _beam_path(directory: Path) -> Path:
    return directory / "beams" / _beam_name()


@pytest.fixture
def stack_dir(tmp_path: Path) -> Path:
    """Two timesteps of 8x8 images and a primary beam outside the image directory."""
    for t in range(2):
        _write(tmp_path / _image_name(t), t + 1, f"2020-01-01T00:00:0{t}.000")
    beam = _beam_path(tmp_path)
    beam.parent.mkdir()
    _write(beam, 1.0)
    return tmp_path


def _ctx(
    directory: Path,
    *,
    beam: Path | None = None,
    overwrite: bool = False,
    outfile: Path | None = None,
    **cfg,
) -> ImageStackContext:
    return ImageStackContext(
        cfg=ImageStackConfig(**{"stamp_size": 4, "time_interval": 1.0, **cfg}),
        obsid="obs",
        input_dir=directory,
        beam=beam or _beam_path(directory),
        overwrite=overwrite,
        outfile=outfile,
    )


def test_create_and_query(stack_dir):
    output = create_image_stack(_ctx(stack_dir))
    assert output == stack_dir / "obs.hdf5"

    with ImageStack(output) as stack:
        assert stack.data.shape == (1, SIZE, SIZE, 1, 2)
        assert stack.n_pol == 1
        assert np.allclose(stack.pix2ts(4, 4), [1.0, 2.0])
        assert np.allclose(stack.pix2ts(4, 4, correct=False), [1.0, 2.0])
        assert stack.pix2ts(4, 4, avg_pol=False).shape == (2,)
        assert stack.pix2cube(4, 4, 3).shape == (3, 3, 2)
        assert stack.get_beam().shape == (SIZE, SIZE)
        assert stack.get_interval() == 1.0
        assert np.allclose(stack.get_intervals(), [0.0, 1.0])
        assert stack.world2pix(10.0, -45.0) == (4, 3)


def test_images_are_stacked_alphabetically(stack_dir):
    for path in stack_dir.glob("*t*-image.fits"):
        path.unlink()
    for t in (10, 2, 1):
        _write(stack_dir / _image_name(t), float(t), f"2020-01-01T00:00:{t:02d}.000")

    output = create_image_stack(_ctx(stack_dir))
    with ImageStack(output) as stack:
        assert np.allclose(stack.pix2ts(4, 4), [1.0, 2.0, 10.0])


def test_no_images_raises(stack_dir):
    for path in stack_dir.glob("*t*-image.fits"):
        path.unlink()
    with pytest.raises(FileNotFoundError, match="No FITS images"):
        create_image_stack(_ctx(stack_dir))


def test_missing_beam_raises(stack_dir):
    _beam_path(stack_dir).unlink()
    with pytest.raises(ValueError, match="Primary beam does not exist"):
        _ctx(stack_dir)


def test_supplied_beam_is_used(stack_dir):
    _write(stack_dir / "decoy-beam.fits", 5.0)
    output = create_image_stack(_ctx(stack_dir))
    with ImageStack(output) as stack:
        assert np.isclose(stack.pix2beam(4, 4), 1.0)


def test_single_pol_beam_correction(stack_dir):
    _write(_beam_path(stack_dir), 2.0)
    output = create_image_stack(_ctx(stack_dir))
    with ImageStack(output) as stack:
        assert np.isclose(stack.pix2beam(4, 4), 2.0)
        assert np.allclose(stack.pix2ts(4, 4), [0.5, 1.0])
        assert np.allclose(stack.pix2ts(4, 4, avg_pol=False), [0.5, 1.0])


def test_nan_below_beam_threshold_is_masked(stack_dir):
    beam = np.ones((SIZE, SIZE), dtype=np.float32)
    beam[0, 0] = 0.0
    fits.writeto(_beam_path(stack_dir), beam, _header(), overwrite=True)

    output = create_image_stack(_ctx(stack_dir))
    with ImageStack(output) as stack:
        assert np.isnan(stack.data[0, 0, 0, 0, 0])


def test_custom_outfile(stack_dir):
    outfile = stack_dir / "nested" / "custom.h5"
    output = create_image_stack(_ctx(stack_dir, outfile=outfile))
    assert output == outfile
    assert output.exists()


def test_existing_output_raises_without_overwrite(stack_dir):
    ctx = _ctx(stack_dir)
    create_image_stack(ctx)
    with pytest.raises(FileExistsError):
        create_image_stack(ctx)
    assert create_image_stack(_ctx(stack_dir, overwrite=True)).exists()


def test_failed_write_leaves_no_partial_output(stack_dir, monkeypatch):
    ctx = _ctx(stack_dir)

    def fail(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(ImageStackContext, "_write_group", fail)
    with pytest.raises(RuntimeError, match="write failed"):
        create_image_stack(ctx)

    tmp = ctx.output.with_name(f".{ctx.output.name}.tmp")
    assert not ctx.output.exists()
    assert not tmp.exists()


def test_image_size_must_divide_stamp_size(stack_dir):
    with pytest.raises(ValueError, match="divisible by stamp_size"):
        create_image_stack(_ctx(stack_dir, stamp_size=3))


def test_custom_time_interval(stack_dir):
    output = create_image_stack(_ctx(stack_dir, time_interval=4.0))
    with ImageStack(output) as stack:
        assert stack.get_interval() == 4.0


def test_beam_files_in_input_dir_are_not_stacked(stack_dir):
    _write(stack_dir / "decoy-beam.fits", 5.0)
    output = create_image_stack(_ctx(stack_dir))
    with h5py.File(output) as df:
        assert df["image"].shape == (1, SIZE, SIZE, 1, 2)


def test_non_image_suffixes_are_ignored(stack_dir):
    _write(stack_dir / f"obs_{PREFIX}-t0000-model.fits", 99.0)
    output = create_image_stack(_ctx(stack_dir))
    with ImageStack(output) as stack:
        assert stack.data.shape[-1] == 2


def test_pix2stamp_doctest_cases():
    assert pix2stamp(1.6, 3.2, 1) == (slice(2, 3), slice(3, 4))
    assert pix2stamp(1.6, 3.2, 2) == (slice(1, 3), slice(3, 5))
    assert pix2stamp(1.6, 3.2, 3) == (slice(1, 4), slice(2, 5))


def test_sault_beam_preserves_spatial_axes_for_single_pol():
    beam = np.ones((1, SIZE, SIZE))
    result = sault_beam(beam, np.ones(1))
    assert result.shape == (SIZE, SIZE)


def test_sault_weight_corrects_for_beam():
    data = np.array([[[2.0, 4.0]], [[3.0, 9.0]]])
    beam = np.array([[[2.0, 2.0]], [[1.0, 1.0]]])
    sigma = np.ones(2)
    expected = np.sum(data * beam, axis=0) / np.sum(beam**2, axis=0)
    assert np.allclose(sault_weight(data, beam, sigma, correct=True), expected)
    assert np.allclose(sault_weight(data, beam, sigma, correct=False), expected * sault_beam(beam, sigma))

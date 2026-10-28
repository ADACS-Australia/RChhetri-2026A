"""
Builds HDF5 image stacks from a directory of WSClean FITS images.

An image stack stores a sequence of images with a shared celestial WCS, along with the primary
beam, timestamps and per-timestep metadata. It is intended for fast time-domain queries over a
single field.

Images are identified by a four-digit timestep token (e.g. ``-t0000-``) and are stacked in
alphabetical filename order. The primary beam is supplied at runtime.

This module was adapted with permission from John Morgan's code: https://github.com/johnsmorgan/imstack
"""

import getpass
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import h5py
import numpy as np
from astropy.io import fits
from pydantic import field_validator

from needle.config.base import needle_module_args
from needle.config.imstack import ImageStackConfig
from needle.lib.validate import validate_path_fits
from needle.modules.needle_context import NeedleContext

logger = logging.getLogger(__name__)


IMAGE_GLOB = "*t[0-9][0-9][0-9][0-9]*-image.fits"
"WSClean images, e.g. obs-t0000-image.fits"


def _header_attrs(header: fits.Header) -> dict:
    """Convert a FITS header into a dict that h5py can store as attributes."""
    attrs = {}
    for key, item in header.items():
        if not key or item is None:
            continue
        attrs[key] = item.decode("utf-8", "replace") if isinstance(item, bytes) else item
    return attrs


def _read_fits(path: Path, hdu: int) -> tuple[np.ndarray, fits.Header]:
    """Read a FITS image and squeeze it to two dimensions.

    :param path: Path to the FITS file
    :param hdu: HDU index to read
    :raises ValueError: Raised if the HDU has no data or cannot be squeezed to two dimensions
    :return: The image data and its header
    """
    with fits.open(path, memmap=True) as hdul:
        data = hdul[hdu].data
        if data is None:
            raise ValueError(f"HDU {hdu} in {path} contains no data")
        data = np.squeeze(data)
        while data.ndim > 2:
            data = data[0]
        if data.ndim != 2:
            raise ValueError(f"Expected a 2D image in {path}, got shape {data.shape}")
        return np.array(data), hdul[hdu].header.copy()


class ImageStackContext(NeedleContext):
    """The runtime context required to build an HDF5 image stack."""

    cfg: ImageStackConfig
    "Static configuration for image stack creation"

    obsid: str
    "Observation identifier used in filenames"

    beam_file: Path
    "Path to the primary beam FITS image"

    input_dir: Path = Path(".")
    "Directory containing the FITS images"

    overwrite: bool = False
    "Overwrite an existing output file"

    @field_validator("input_dir")
    @classmethod
    def _valid_input_dir(cls, v: Path) -> Path:
        if not v.is_dir():
            raise ValueError(f"Input directory does not exist: {v}")
        return v

    @field_validator("beam")
    @classmethod
    def _valid_beam(cls, v: Path) -> Path:
        validate_path_fits(v)
        if not v.is_file():
            raise ValueError(f"Primary beam does not exist: {v}")
        return v

    @property
    def output(self) -> Path:
        "The output HDF5 path"
        return self.input_dir / f"{self.obsid}.hdf5"

    def _images(self) -> list[Path]:
        """Find the FITS images to stack, in filename order.

        :raises FileNotFoundError: Raised if the directory contains no images
        :return: The ordered FITS image paths
        """
        images = sorted(path for path in self.input_dir.glob(IMAGE_GLOB) if path != self.beam_file)
        if not images:
            raise FileNotFoundError(f"No FITS images matching '{IMAGE_GLOB}' in {self.input_dir}")
        return images

    def _image_size(self, path: Path) -> int:
        data, _ = _read_fits(path, self.cfg.hdu)
        if data.shape[0] != data.shape[1]:
            raise ValueError(f"Expected a square image, got shape {data.shape} in {path}")
        if data.shape[0] % self.cfg.stamp_size:
            raise ValueError(f"Image size {data.shape[0]} must be divisible by stamp_size {self.cfg.stamp_size}")
        return data.shape[0]

    def _write_beam(self, group: h5py.Group, image_size: int) -> np.ndarray:
        """Write the primary beam and return the boolean mask of usable pixels.

        :param group: The HDF5 group to write to
        :param image_size: The image size in pixels
        :return: The boolean mask of usable pixels
        """
        beam = group.create_dataset(
            "beam", (1, image_size, image_size, 1, 1), dtype=np.float32, compression="lzf", shuffle=True
        )
        data, header = _read_fits(self.beam_file, self.cfg.hdu)
        if data.shape != (image_size, image_size):
            raise ValueError(f"Primary beam shape {data.shape} does not match image shape {image_size}")
        beam[0, :, :, 0, 0] = data
        for key, item in _header_attrs(header).items():
            beam.attrs[key] = item

        pb_sum = np.abs(np.asarray(data, dtype=np.float64))
        if np.isnan(pb_sum).any():
            logger.warning("NaNs in primary beam")
        pb_max = np.nanmax(pb_sum)
        if not np.isfinite(pb_max) or pb_max <= 0:
            logger.warning("Primary beam is empty or invalid. All data will be masked")
            return np.zeros((image_size, image_size), dtype=bool)
        return pb_sum > self.cfg.pb_threshold * pb_max

    def _write_group(self, df: h5py.File, images: list[Path]) -> None:
        """Write the image stack.

        :param df: The open HDF5 file
        :param images: The ordered FITS image paths
        """
        group = df["/"]
        group.attrs["TIME_INTERVAL"] = self.cfg.time_interval
        image_size = self._image_size(images[0])
        n_timesteps = len(images)
        logger.debug(f"data shape: {(1, image_size, image_size, 1, n_timesteps)}")

        pb_mask = self._write_beam(group, image_size)

        timestamp = group.create_dataset("timestamp", (n_timesteps,), dtype="S64")

        _, header = _read_fits(images[n_timesteps // 2], self.cfg.hdu)
        header_ds = group.create_dataset("header", data=[], dtype=np.dtype(self.cfg.dtype))
        for key, item in _header_attrs(header).items():
            header_ds.attrs[key] = item

        data_shape = (1, image_size, image_size, 1, n_timesteps)
        chunks = (1, self.cfg.stamp_size, self.cfg.stamp_size, 1, 1)
        filename_len = max(len(str(path).encode("utf-8")) for path in images)
        dtype = np.dtype(self.cfg.dtype)

        logger.info("Writing image data")
        data_ds = group.create_dataset("image", data_shape, chunks=chunks, dtype=dtype, compression="lzf", shuffle=True)
        filenames = group.create_dataset(
            "image_filenames", (1, 1, n_timesteps), dtype=f"S{filename_len}", compression="lzf"
        )

        for t, path in enumerate(images):
            logger.debug(f"processing {path}")
            image, image_header = _read_fits(path, self.cfg.hdu)
            if image.shape != (image_size, image_size):
                raise ValueError(f"Image shape {image.shape} does not match expected {image_size} in {path}")
            image = np.asarray(image, dtype=dtype)
            image[~pb_mask] = np.nan
            data_ds[0, :, :, 0, t] = image
            filenames[0, 0, t] = str(path).encode("utf-8")

            date_obs = image_header.get("DATE-OBS")
            if date_obs is None:
                raise ValueError(f"Missing DATE-OBS in {path}")
            timestamp[t] = str(date_obs).encode("utf-8")

    def execute(self) -> Path:
        """Build the image stack and write it to the output path.

        :raises FileExistsError: Raised if the output exists and overwrite is False
        :return: The image stack output
        """
        output = self.output
        if output.exists() and not self.overwrite:
            raise FileExistsError(f"Output file already exists: {output}")

        images = self._images()
        logger.info(f"Found {len(images)} images in {self.input_dir}")
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o770)
        tmp = output.with_name(f".{output.name}.tmp")
        tmp.unlink(missing_ok=True)
        logger.info(f"Writing image stack to {output}")
        try:
            with h5py.File(tmp, "w", rdcc_nbytes=self.cfg.cache_size_mb * 1024 * 1024) as df:
                df.attrs["USER"] = getpass.getuser()
                df.attrs["DATE_CREATED"] = datetime.now(timezone.utc).isoformat()
                self._write_group(df, images)
            tmp.replace(output)
        finally:
            tmp.unlink(missing_ok=True)
        logger.info(f"Image stack written to {output}")
        return output


def create_image_stack(ctx: ImageStackContext) -> Path:
    """Build an HDF5 image stack from WSClean FITS images.

    :param ctx: The ImageStackContext object
    :return: The path to the image stack
    """
    logger.info(f"Building image stack for observation {ctx.obsid}")
    return ctx.execute()


@needle_module_args(
    ImageStackConfig,
    name="create",
    help="Build an HDF5 image stack from WSClean FITS images.",
)
@click.option("--obsid", type=str, required=True, help="Observation identifier used in filenames")
@click.option(
    "--input-dir",
    "-i",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
    help="Directory containing the FITS images",
)
@click.option(
    "--beam",
    "-b",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the primary beam FITS image",
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing output file")
def _create(cfg: ImageStackConfig, obsid: str, input_dir: Path, beam: Path, overwrite: bool):
    ctx = ImageStackContext(cfg=cfg, obsid=obsid, input_dir=input_dir, beam=beam, overwrite=overwrite)
    create_image_stack(ctx)


entrypoint = click.Group(
    name="imstack",
    help="Build HDF5 image stacks from WSClean FITS images.",
)
entrypoint.add_command(_create)


if __name__ == "__main__":
    entrypoint()

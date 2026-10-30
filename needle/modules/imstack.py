"""
Builds HDF5 image stacks from a directory of WSClean FITS images.

An image stack stores a sequence of images with a shared celestial WCS, timestamps and
per-timestep metadata. It is intended for fast time-domain queries over a single field.

Images are identified by a four-digit timestep token (e.g. ``-t0000-``) and are stacked in
alphabetical filename order.

This module was adapted with permission from John Morgan's code: https://github.com/johnsmorgan/imstack
"""

import getpass
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import click
import h5py
import numpy as np
from astropy.io import fits
from pydantic import field_validator

from needle.config.base import needle_module_args
from needle.config.imstack import ImageStackConfig
from needle.modules.needle_context import NeedleContext

logger = logging.getLogger(__name__)


IMAGE_GLOB = "*t[0-9][0-9][0-9][0-9]*-image.fits"
"WSClean images, e.g. obs-t0000-image.fits"

CHECKPOINT_IMAGES = 50
"Number of images between HDF5 flushes and progress checkpoints"

MIN_CHUNK_SIZE = 128
"Minimum HDF5 chunk size in pixels. Small chunks make full-image writes very slow."


def _images_hash(images: list[Path]) -> str:
    """Stable hash of the ordered image paths, used to validate a resumed stack."""
    return hashlib.sha256("\n".join(str(path) for path in images).encode("utf-8")).hexdigest()


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

    @property
    def output(self) -> Path:
        "The output HDF5 path"
        return self.input_dir / f"{self.obsid}.hdf5"

    def _images(self) -> list[Path]:
        """Find the FITS images to stack, in filename order.

        :raises FileNotFoundError: Raised if the directory contains no images
        :return: The ordered FITS image paths
        """
        images = sorted(self.input_dir.glob(IMAGE_GLOB))
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

    @property
    def _partial(self) -> Path:
        "Partially written image stack, kept between worker retries"
        return self.output.with_name(f".{self.output.name}.partial")

    @property
    def _progress(self) -> Path:
        "Checkpoint file recording how many images are safely written"
        return self.output.with_name(f".{self.output.name}.progress")

    def _create_group(self, df: h5py.File, images: list[Path], image_size: int) -> None:
        """Create the datasets for a new image stack.

        :param df: The open HDF5 file
        :param images: The ordered FITS image paths
        :param image_size: The image size in pixels
        """
        group = df["/"]
        group.attrs["TIME_INTERVAL"] = self.cfg.time_interval
        n_timesteps = len(images)
        logger.debug(f"data shape: {(1, image_size, image_size, 1, n_timesteps)}")

        group.create_dataset("timestamp", (n_timesteps,), dtype="S64")

        _, header = _read_fits(images[n_timesteps // 2], self.cfg.hdu)
        header_ds = group.create_dataset("header", data=[], dtype=np.dtype(self.cfg.dtype))
        for key, item in _header_attrs(header).items():
            header_ds.attrs[key] = item

        chunk_size = min(image_size, max(MIN_CHUNK_SIZE, self.cfg.stamp_size))
        data_shape = (1, image_size, image_size, 1, n_timesteps)
        chunks = (1, chunk_size, chunk_size, 1, 1)
        filename_len = max(len(str(path).encode("utf-8")) for path in images)
        dtype = np.dtype(self.cfg.dtype)

        logger.info("Writing image data")
        group.create_dataset("image", data_shape, chunks=chunks, dtype=dtype, compression="lzf", shuffle=True)
        group.create_dataset(
            "image_filenames", (1, 1, n_timesteps), dtype=f"S{filename_len}", compression="lzf"
        )

    def _write_image(self, df: h5py.File, path: Path, t: int) -> None:
        """Write one image to the stack.

        :param df: The open HDF5 file
        :param path: The FITS image path
        :param t: The timestep index
        """
        data_ds = df["image"]
        filenames = df["image_filenames"]
        timestamp = df["timestamp"]

        logger.debug(f"processing {path}")
        image, image_header = _read_fits(path, self.cfg.hdu)
        expected = (data_ds.shape[1], data_ds.shape[2])
        if image.shape != expected:
            raise ValueError(f"Image shape {image.shape} does not match expected {expected} in {path}")
        data_ds[0, :, :, 0, t] = np.asarray(image, dtype=data_ds.dtype)
        filenames[0, 0, t] = str(path).encode("utf-8")

        date_obs = image_header.get("DATE-OBS")
        if date_obs is None:
            raise ValueError(f"Missing DATE-OBS in {path}")
        timestamp[t] = str(date_obs).encode("utf-8")

    def _write_progress(self, n_done: int, images: list[Path], image_size: int) -> None:
        """Atomically record how many images are safely written."""
        data = {
            "n_done": n_done,
            "n_images": len(images),
            "image_size": image_size,
            "dtype": self.cfg.dtype,
            "stamp_size": self.cfg.stamp_size,
            "paths_hash": _images_hash(images),
        }
        progress = self._progress
        tmp = progress.with_name(f"{progress.name}.tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(progress)

    def _resume_start(self, df: h5py.File, images: list[Path], image_size: int) -> int:
        """Return the number of images already written, or 0 if the stack cannot be resumed."""
        progress = self._progress
        try:
            data = json.loads(progress.read_text())
        except (OSError, json.JSONDecodeError):
            return 0

        expected = {
            "n_images": len(images),
            "image_size": image_size,
            "dtype": self.cfg.dtype,
            "stamp_size": self.cfg.stamp_size,
            "paths_hash": _images_hash(images),
        }
        if any(data.get(key) != value for key, value in expected.items()):
            return 0

        n_done = data.get("n_done")
        if not isinstance(n_done, int) or not 0 <= n_done <= len(images):
            return 0
        if "image" not in df or "timestamp" not in df or "image_filenames" not in df:
            return 0
        if df["image"].shape != (1, image_size, image_size, 1, len(images)):
            return 0
        return n_done

    def _write_images(self, df: h5py.File, images: list[Path], start: int) -> None:
        """Write images from ``start`` to the end, checkpointing as it goes."""
        n_timesteps = len(images)
        image_size = df["image"].shape[1]
        for t in range(start, n_timesteps):
            self._write_image(df, images[t], t)
            if (t + 1) % CHECKPOINT_IMAGES == 0:
                df.flush()
                self._write_progress(t + 1, images, image_size)
        df.flush()
        self._write_progress(n_timesteps, images, image_size)

    def execute(self) -> Path:
        """Build the image stack and write it to the output path.

        Partial stacks are checkpointed so a worker killed mid-write resumes instead of
        rebuilding all images from scratch.

        :raises FileExistsError: Raised if the output exists and overwrite is False
        :return: The image stack output
        """
        output = self.output
        if output.exists() and not self.overwrite:
            raise FileExistsError(f"Output file already exists: {output}")

        images = self._images()
        logger.info(f"Found {len(images)} images in {self.input_dir}")
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o770)
        partial = self._partial
        progress = self._progress
        if self.overwrite:
            partial.unlink(missing_ok=True)
            progress.unlink(missing_ok=True)

        image_size = self._image_size(images[0])
        start = 0
        if partial.exists() and progress.exists():
            try:
                with h5py.File(partial, "r", rdcc_nbytes=self.cfg.cache_size_mb * 1024 * 1024) as df:
                    start = self._resume_start(df, images, image_size)
            except OSError as e:
                logger.warning(f"Could not resume partial image stack: {e}")
                start = 0

        if start == 0:
            partial.unlink(missing_ok=True)
            progress.unlink(missing_ok=True)
            mode = "w"
        else:
            logger.info(f"Resuming image stack at {start}/{len(images)} images")
            mode = "r+"

        logger.info(f"Writing image stack to {output}")
        with h5py.File(partial, mode, rdcc_nbytes=self.cfg.cache_size_mb * 1024 * 1024) as df:
            if mode == "w":
                df.attrs["USER"] = getpass.getuser()
                df.attrs["DATE_CREATED"] = datetime.now(timezone.utc).isoformat()
                self._create_group(df, images, image_size)
            self._write_images(df, images, start)

        partial.replace(output)
        progress.unlink(missing_ok=True)
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
@click.option("--overwrite", is_flag=True, help="Overwrite an existing output file")
def _create(cfg: ImageStackConfig, obsid: str, input_dir: Path, overwrite: bool):
    ctx = ImageStackContext(cfg=cfg, obsid=obsid, input_dir=input_dir, overwrite=overwrite)
    create_image_stack(ctx)


entrypoint = click.Group(
    name="imstack",
    help="Build HDF5 image stacks from WSClean FITS images.",
)
entrypoint.add_command(_create)


if __name__ == "__main__":
    entrypoint()

"""
Builds and reads HDF5 image stacks from WSClean FITS images.

An image stack stores a sequence of images with a shared celestial WCS, along with the primary
beam, timestamps and per-timestep metadata. It is intended for fast time-domain queries over a
single field.

This module was adapted with permission from John Morgan's code: https://github.com/johnsmorgan/imstack
"""

import getpass
import logging
from datetime import datetime, timezone
from math import ceil, pi, sqrt
from pathlib import Path
from typing import Literal, Optional

import click
import h5py
import numpy as np
from astropy import wcs
from astropy.io import fits
from pydantic import field_validator

from needle.config.base import needle_module_args
from needle.config.imstack import ImageStackConfig
from needle.modules.needle_context import NeedleContext

logger = logging.getLogger(__name__)

IMAGE_STACK_VERSION = "0.2"
IMAGE_TYPES = ("image", "dirty", "model", "moment")


def pix2stamp(x, y, n):
    """Produce slices for a square centred on x, y.

    :param x: x pixel coordinate. May be fractional
    :param y: y pixel coordinate. May be fractional
    :param n: Side length of the square in pixels
    :return: The x and y slices

    >>> pix2stamp(1.6, 3.2, 1)
    (slice(2, 3, None), slice(3, 4, None))
    >>> pix2stamp(1.6, 3.2, 2)
    (slice(1, 3, None), slice(3, 5, None))
    >>> pix2stamp(1.6, 3.2, 3)
    (slice(1, 4, None), slice(2, 5, None))
    """
    if n % 2:
        lower, upper = (n - 1) // 2, (n + 1) // 2
        return tuple(slice(int(round(i)) - lower, int(round(i)) + upper) for i in (x, y))
    return tuple(slice(int(ceil(i)) - n // 2, int(ceil(i)) + n // 2) for i in (x, y))


def semihex(data, axis=None):
    """Calculate the standard deviation via the semi-interhexile range."""
    h1, h5 = np.percentile(data, (100 / 6.0, 500 / 6.0), axis=axis)
    return (h5 - h1) / 2.0


def hypotn(d, axis=0):
    """Similar to hypot, but not restricted to two dimensions."""
    return np.sqrt(np.sum(d**2, axis=axis))


def sault_beam(beam, sigma):
    """Combine beams with Sault weighting."""
    beam = np.asarray(beam)
    sigma = np.asarray(sigma)
    while beam.ndim > sigma.ndim:
        sigma = sigma[..., np.newaxis]
    return np.sqrt(np.sum(beam**2 / sigma**2, axis=0) / np.sum(sigma**-2, axis=0))


def sault_weight(data, beam, sigma, correct=False):
    """Combine data from multiple pointings or polarisations using Sault weighting.

    See 1996A&AS..120..375S Equation 1. Pointing or polarisation must be the first axis for
    both data and beam.

    :param data: Data to combine
    :param beam: Primary beam corresponding to the data
    :param sigma: Noise level of each pointing or polarisation
    :param correct: If True, return the weighted average without the beam correction
    :return: The combined data
    """
    while len(data.shape) > len(beam.shape):
        beam = beam[..., np.newaxis]
    while len(data.shape) > len(sigma.shape):
        sigma = sigma[..., np.newaxis]
    d = np.sum(data * beam / sigma**2, axis=0) / np.sum(beam**2 / sigma**2, axis=0)
    if correct is True:
        return d
    return d * sault_beam(beam, sigma)


def _decode_header(header) -> dict:
    """Convert an HDF5 FITS header attribute map into a WCS-compatible dictionary."""
    return {k: v.decode("ascii") if isinstance(v, bytes) else v for k, v in header.items()}


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
    "Directory containing the WSClean FITS images and primary beam files"

    n_timesteps: int
    "Number of timesteps to process"

    check_filenames_only: bool = False
    "Validate that all expected files exist, then skip writing"

    outfile: Path | None = None
    "Output HDF5 path. Defaults to <input_dir>/<obsid>.hdf5"

    overwrite: bool = False
    "Overwrite an existing output file"

    @field_validator("input_dir")
    @classmethod
    def _valid_input_dir(cls, v: Path) -> Path:
        if not v.is_dir():
            raise ValueError(f"Input directory does not exist: {v}")
        return v

    @field_validator("n_timesteps")
    @classmethod
    def _valid_n_timesteps(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"n_timesteps must be positive, got {v}")
        return v

    @property
    def stem(self) -> str:
        "The observation id with the optional prefix appended"
        return f"{self.obsid}_{self.cfg.prefix}" if self.cfg.prefix else self.obsid

    def stem_band(self, band: str) -> str:
        "The observation id with the optional band and prefix appended"
        base = f"{self.obsid}_{band}"
        return f"{base}_{self.cfg.prefix}" if self.cfg.prefix else base

    @property
    def output(self) -> Path:
        "The output HDF5 path"
        return self.outfile or self.input_dir / f"{self.obsid}.hdf5"

    def _format(self, template: str, band: str | None, **kwargs) -> Path:
        return self.input_dir / template.format(
            stem=self.stem,
            stem_band=self.stem_band(band) if band is not None else "",
            obsid=self.obsid,
            prefix=self.cfg.prefix,
            band=band,
            **kwargs,
        )

    def image_path(self, time: int, pol: str, suffix: str, band: str | None = None) -> Path:
        """Build the path to a WSClean image.

        :param time: Absolute timestep index
        :param pol: Polarisation
        :param suffix: WSClean output suffix e.g. 'image'
        :param band: Frequency band, or None for unbanded data
        """
        template = self.cfg.filename_band if band is not None else self.cfg.filename
        return self._format(template, band, time=time, pol=pol, suffix=suffix)

    def pb_path(self, pol: str, band: str | None = None) -> Path:
        """Build the path to a primary beam image.

        :param pol: Polarisation
        :param band: Frequency band, or None for unbanded data
        """
        template = self.cfg.pb_filename_band if band is not None else self.cfg.pb_filename
        return self._format(template, band, pol=pol)

    def _resolve_n_timesteps(self) -> int:
        """Resolve the number of timesteps to process, truncating at the first missing timestep.

        :raises FileNotFoundError: Raised if a required file is missing and allow_missing is False
        :return: The number of usable timesteps
        """
        if not self.cfg.skip_beam:
            for band in self.cfg.bands or [None]:
                for pol in self.cfg.pols:
                    pb_path = self.pb_path(pol, band)
                    if not pb_path.exists():
                        raise FileNotFoundError(f"Could not find required file {pb_path}")

        n_timesteps = self.n_timesteps
        while n_timesteps > 0:
            missing = [
                p
                for t in range(self.cfg.start, self.cfg.start + n_timesteps)
                for band in self.cfg.bands or [None]
                for suffix in self.cfg.suffixes
                for pol in self.cfg.pols
                if not (p := self.image_path(t, pol, suffix, band)).exists()
            ]
            if not missing:
                return n_timesteps
            if not self.cfg.allow_missing:
                raise FileNotFoundError(f"Could not find required file {missing[0]}")
            timesteps = [
                t
                for t in range(self.cfg.start, self.cfg.start + n_timesteps)
                if any(
                    not self.image_path(t, pol, suffix, band).exists()
                    for band in self.cfg.bands or [None]
                    for suffix in self.cfg.suffixes
                    for pol in self.cfg.pols
                )
            ]
            new_n = (timesteps[0] - self.cfg.start) if timesteps else 0
            logger.info(f"Couldn't find file {missing[0]}: reducing n from {n_timesteps} to {new_n}")
            n_timesteps = new_n
        raise FileNotFoundError(f"No complete timesteps found from {self.cfg.start}")

    def _image_size(self, band: str | None) -> int:
        path = self.image_path(self.cfg.start, self.cfg.pols[0], self.cfg.suffixes[0], band)
        data, _ = _read_fits(path, self.cfg.hdu)
        if data.shape[0] != data.shape[1]:
            raise ValueError(f"Expected a square image, got shape {data.shape} in {path}")
        if data.shape[0] % self.cfg.stamp_size:
            raise ValueError(f"Image size {data.shape[0]} must be divisible by stamp_size {self.cfg.stamp_size}")
        return data.shape[0]

    def _write_beam(self, group, band: str | None, image_size: int) -> np.ndarray:
        """Write the primary beam and return the boolean mask of usable pixels."""
        if self.cfg.skip_beam:
            return np.ones((image_size, image_size), dtype=bool)

        beam_shape = (len(self.cfg.pols), image_size, image_size, self.cfg.n_channels, 1)
        beam = group.create_dataset("beam", beam_shape, dtype=np.float32, compression="lzf", shuffle=True)
        pb_sum_sq = np.zeros((image_size, image_size), dtype=np.float64)
        for p, pol in enumerate(self.cfg.pols):
            data, header = _read_fits(self.pb_path(pol, band), self.cfg.hdu)
            if data.shape != (image_size, image_size):
                raise ValueError(f"Primary beam shape {data.shape} does not match image shape {image_size}")
            beam[p, :, :, 0, 0] = data
            pb_sum_sq += np.asarray(data, dtype=np.float64) ** 2
            for key, item in header.items():
                beam.attrs[key] = item

        pb_sum = np.sqrt(pb_sum_sq / len(self.cfg.pols))
        if np.isnan(pb_sum).any():
            logger.warning("NaNs in primary beam")
        pb_max = np.nanmax(pb_sum)
        if not np.isfinite(pb_max) or pb_max <= 0:
            logger.warning("Primary beam is empty or invalid. All data will be masked")
            return np.zeros((image_size, image_size), dtype=bool)
        return pb_sum > self.cfg.pb_threshold * pb_max

    def _write_group(self, df: h5py.File, band: str | None, n_timesteps: int):
        group = df["/"] if band is None else df.create_group(band)
        group.attrs["TIME_INTERVAL"] = self.cfg.time_interval
        image_size = self._image_size(band)
        logger.debug(f"data shape: {(len(self.cfg.pols), image_size, image_size, self.cfg.n_channels, n_timesteps)}")

        pb_mask = self._write_beam(group, band, image_size)

        timestep_start = group.create_dataset("timestep_start", (n_timesteps,), dtype=np.int32)
        timestep_stop = group.create_dataset("timestep_stop", (n_timesteps,), dtype=np.int32)
        timestamp = group.create_dataset("timestamp", (n_timesteps,), dtype="S64")

        header_path = self.image_path(self.cfg.start + n_timesteps // 2, self.cfg.pols[0], self.cfg.suffixes[0], band)
        _, header = _read_fits(header_path, self.cfg.hdu)
        header_ds = group.create_dataset("header", data=[], dtype=np.dtype(self.cfg.dtype))
        for key, item in header.items():
            header_ds.attrs[key] = item

        data_shape = (len(self.cfg.pols), image_size, image_size, self.cfg.n_channels, n_timesteps)
        chunks = (len(self.cfg.pols), self.cfg.stamp_size, self.cfg.stamp_size, self.cfg.n_channels, 1)
        filename_len = max(
            len(str(self.image_path(t, pol, suffix, band)))
            for t in range(self.cfg.start, self.cfg.start + n_timesteps)
            for pol in self.cfg.pols
            for suffix in self.cfg.suffixes
        )
        dtype = np.dtype(self.cfg.dtype)

        for s, suffix in enumerate(self.cfg.suffixes):
            logger.info(f"Processing segment {suffix}")
            data_ds = group.create_dataset(
                suffix, data_shape, chunks=chunks, dtype=dtype, compression="lzf", shuffle=True
            )
            filenames = group.create_dataset(
                f"{suffix}_filenames",
                (len(self.cfg.pols), self.cfg.n_channels, n_timesteps),
                dtype=f"S{filename_len}",
                compression="lzf",
            )

            for t in range(n_timesteps):
                for p, pol in enumerate(self.cfg.pols):
                    path = self.image_path(self.cfg.start + t, pol, suffix, band)
                    logger.debug(f"processing {path}")
                    image, image_header = _read_fits(path, self.cfg.hdu)
                    if image.shape != (image_size, image_size):
                        raise ValueError(f"Image shape {image.shape} does not match expected {image_size} in {path}")
                    image = np.asarray(image, dtype=dtype)
                    image[~pb_mask] = np.nan
                    data_ds[p, :, :, 0, t] = image
                    filenames[p, 0, t] = str(path).encode("utf-8")

                    date_obs = image_header.get("DATE-OBS")
                    if date_obs is None:
                        raise ValueError(f"Missing DATE-OBS in {path}")
                    if s == 0 and p == 0:
                        timestamp[t] = str(date_obs).encode("utf-8")
                        if self.cfg.old_wcs_timesteps:
                            timestep_start[t] = int(image_header["WSCTIMES"])
                            timestep_stop[t] = int(image_header["WSCTIMEE"])
                        else:
                            timestep_start[t] = t
                            timestep_stop[t] = t + 1
                    elif str(date_obs) != timestamp[t].decode("utf-8"):
                        raise ValueError(f"Timesteps do not match {self.cfg.suffixes[0]} in {path}")

    def execute(self) -> Path:
        """Build the image stack and write it to the output path.

        :raises FileExistsError: Raised if the output exists and overwrite is False
        :return: The image stack output
        """
        n_timesteps = self._resolve_n_timesteps()
        output = self.output
        logger.info(f"Resolved {n_timesteps} timesteps from {self.cfg.start}")
        if self.check_filenames_only:
            return output
        if output.exists() and not self.overwrite:
            raise FileExistsError(f"Output file already exists: {output}")

        output.parent.mkdir(parents=True, exist_ok=True, mode=0o770)
        logger.info(f"Writing image stack to {output}")
        with h5py.File(output, "w", rdcc_nbytes=self.cfg.cache_size_mb * 1024 * 1024) as df:
            df.attrs["VERSION"] = IMAGE_STACK_VERSION
            df.attrs["USER"] = getpass.getuser()
            df.attrs["DATE_CREATED"] = datetime.now(timezone.utc).isoformat()
            for band in self.cfg.bands or [None]:
                self._write_group(df, band, n_timesteps)
        logger.info(f"Image stack written to {output}")
        return output


class ImageStack:
    """Reads and queries an HDF5 image stack.

    Images have shape (pol, y, x, channel, time). The primary beam, when present, has shape
    (pol, y, x, channel, 1). Time-series, cube and continuum queries are corrected for the
    primary beam and averaged over polarisations by default.
    """

    def __init__(
        self,
        path: Path,
        image_type: Optional[str] = "image",
        freq: Optional[str] = None,
        steps: Optional[tuple[int, Optional[int]]] = None,
        mode: Literal["r", "r+"] = "r",
    ):
        """Open an image stack.

        :param path: Path to the HDF5 image stack
        :param image_type: Image dataset to read e.g. 'image', 'dirty', 'model' or 'moment'
        :param freq: Frequency group to read. None reads the root group
        :param steps: (start, end) timestep range. The end may be None to read to the last timestep
        :param mode: HDF5 file mode
        :raises ValueError: Raised if image_type is unsupported
        """
        self.path = Path(path)
        self.df = h5py.File(self.path, mode)
        self.channel = 0
        self._group = None
        self._data = None
        self._image_type = None
        self.freq = None
        self.steps = None
        self.n_pol = None
        self._load(image_type=image_type, freq=freq, steps=steps)

    def _load(self, image_type: Optional[str], freq: Optional[str], steps: Optional[tuple]):
        if image_type is not None and image_type not in IMAGE_TYPES:
            raise ValueError(f"Unsupported image_type {image_type}")
        self.freq = freq
        self._group = self.df["/"] if freq is None else self.df[freq]
        self._image_type = image_type
        self.header = self._group["header"].attrs
        self.wcs = wcs.WCS(_decode_header(self.header))
        self.has_beam = "beam" in self._group

        self.sigma = None
        self.scale = None
        if self.has_beam:
            beam_attrs = self._group["beam"].attrs
            if "SIGMA" in beam_attrs:
                self.sigma = np.asarray(beam_attrs["SIGMA"])
            if "SCALE" in beam_attrs:
                self.scale = np.asarray(beam_attrs["SCALE"])

        if image_type is None:
            self._data = None
            self.n_pol = self._group["beam"].shape[0] if self.has_beam else None
            self.steps = None
        else:
            self._data = self._group[image_type]
            self.n_pol = self._data.shape[0]
            if steps is None:
                self.steps = [0, self._data.shape[-1]]
            else:
                self.steps = [steps[0], steps[1] if steps[1] is not None else self._data.shape[-1]]

        if self.sigma is None or self.n_pol is None:
            self.sigma = np.ones(self.n_pol or 1)
        elif self.sigma.size == 1:
            self.sigma = np.full(self.n_pol, float(np.squeeze(self.sigma)))
        elif self.sigma.size != self.n_pol:
            self.sigma = np.ones(self.n_pol)
        if self.scale is None or self.n_pol is None or self.scale.size not in (1, self.n_pol):
            self.scale = np.ones(self.n_pol or 1)

    @property
    def data(self) -> h5py.Dataset:
        if self._data is None:
            raise RuntimeError("No image dataset is loaded. Open the stack with an image_type")
        return self._data

    @property
    def image_type(self) -> Optional[str]:
        return self._image_type

    @property
    def group(self) -> h5py.Group:
        return self._group

    def update(self, freq: Optional[str] = None, image_type: Optional[str] = None, steps: Optional[tuple] = None):
        """Switch the loaded frequency group, image type or timestep range."""
        self._load(
            image_type=self.image_type if image_type is None else image_type,
            freq=self.freq if freq is None else freq,
            steps=self.steps if steps is None else steps,
        )

    def close(self):
        self.df.close()

    def __enter__(self) -> "ImageStack":
        return self

    def __exit__(self, *exc):
        self.close()

    def check_slice(self, x, y, margin=0) -> bool:
        """Check that a stamp is valid (not off the edge of the image)."""
        sqrt_beamsize = sqrt(self.get_pixel_beam())
        x_slice, y_slice = pix2stamp(x, y, margin + int(round(10 * max(1, sqrt_beamsize))))
        if not all(0 < s < self.data.shape[2] for s in (x_slice.start, x_slice.stop)):
            return False
        if not all(0 < s < self.data.shape[1] for s in (y_slice.start, y_slice.stop)):
            return False
        return True

    def get_interval(self) -> float:
        """Get the interval between neighbouring images."""
        return self.group.attrs["TIME_INTERVAL"]

    def get_intervals(self) -> np.ndarray:
        """Get the series of intervals."""
        return self.group.attrs["TIME_INTERVAL"] * np.arange(self.steps[1] - self.steps[0])

    def world2pix(self, ra: float, dec: float, floor: bool = True):
        """Convert world coordinates to pixel coordinates.

        x is the fastest varying axis.

        :param ra: Right ascension in degrees
        :param dec: Declination in degrees
        :param floor: Round the result to integer pixels
        :return: The x and y pixel coordinates
        """
        pixcoord = self.wcs.celestial.wcs_world2pix(np.array([[ra, dec]]), 0)
        if floor:
            return int(np.rint(pixcoord[0, 0])), int(np.rint(pixcoord[0, 1]))
        return float(pixcoord[0, 0]), float(pixcoord[0, 1])

    def pix2world(self, x: float, y: float):
        """Convert pixel coordinates to world coordinates.

        x is the fastest varying axis.

        :param x: x pixel coordinate
        :param y: y pixel coordinate
        :return: The right ascension and declination in degrees
        """
        ra_dec = self.wcs.celestial.wcs_pix2world(np.array([[x, y]]), 0)
        return float(ra_dec[0, 0]), float(ra_dec[0, 1])

    def get_pixel_beam(self) -> float:
        """Get the size of the synthesised beam in pixels.

        Assumes square pixels and a roughly circular synthesised beam.
        """
        return abs(
            pi * 0.25 * self.header["BMAJ"] * self.header["BMIN"] / (self.header["CDELT1"] * self.header["CDELT2"])
        )

    def get_scale(self) -> np.ndarray:
        if self.has_beam and "SCALE" in self.group["beam"].attrs:
            return self.group["beam"].attrs["SCALE"]
        return np.array([1.0])

    def scale_beam(self, beam: np.ndarray) -> np.ndarray:
        """Apply the primary beam scale factors to a beam array."""
        scale = np.asarray(self.get_scale()).squeeze()
        if scale.ndim == 0:
            return beam * scale
        if beam.ndim == 1:
            if scale.size != beam.size:
                raise RuntimeError(f"Cannot scale beam of shape {beam.shape} with scale {scale.shape}")
            return beam * scale
        return beam * scale.reshape((-1,) + (1,) * (beam.ndim - 1))

    def _require_beam(self):
        if not self.has_beam:
            raise ValueError("Image stack has no primary beam. Cannot correct for it")

    def _average(self, data: np.ndarray, beam: Optional[np.ndarray], correct: bool) -> np.ndarray:
        """Average polarisations, optionally correcting for the primary beam."""
        if self.has_beam:
            return sault_weight(data, beam, self.sigma, correct)
        if self.n_pol == 1:
            return data[0]
        return np.mean(data, axis=0)

    def pix2beam(self, x, y, avg_pol: bool = True, scale: bool = True):
        """Get the beam corresponding to x, y.

        x and y may be slices, in which case an array of beams is returned.
        """
        if not self.has_beam:
            raise ValueError("Image stack has no primary beam")
        beam = self.group["beam"][:, y, x, self.channel, 0]
        if scale is True:
            beam = self.scale_beam(beam)
        if avg_pol is True:
            if not np.any(beam):
                return 0.0
            return sault_beam(np.squeeze(beam), np.squeeze(self.sigma))
        return beam

    def world2beam(self, ra: float, dec: float, avg_pol: bool = True, scale: bool = True):
        """Get the beam corresponding to ra, dec."""
        x, y = self.world2pix(ra, dec)
        return self.pix2beam(x, y, avg_pol, scale=scale)

    def pix2ts(self, x: int, y: int, avg_pol: bool = True, correct: bool = True) -> np.ndarray:
        """Produce a time series for a given pixel.

        avg_pol=True, correct=True averages polarisations after correcting for the primary beam.
        avg_pol=True, correct=False averages polarisations weighting for the primary beam.
        avg_pol=False, correct=True returns polarisations corrected for the primary beam.
        avg_pol=False, correct=False returns polarisations without primary beam correction.
        """
        ts = np.asarray(self.data[:, y, x, self.channel, self.steps[0] : self.steps[1]], dtype=float)
        if avg_pol is True:
            beam = self.pix2beam(x, y, avg_pol=False) if self.has_beam else None
            return self._average(ts, beam, correct)
        if correct is True and self.has_beam:
            return ts / self.pix2beam(x, y, avg_pol=False)[:, np.newaxis]
        return ts if self.n_pol > 1 else ts[0]

    def world2ts(self, ra: float, dec: float, avg_pol: bool = True, correct: bool = True) -> np.ndarray:
        """Produce a time series for a given ra, dec."""
        x, y = self.world2pix(ra, dec)
        return self.pix2ts(x, y, avg_pol, correct)

    def slice2cube(self, x_slice: slice, y_slice: slice, avg_pol: bool = True, correct: bool = True) -> np.ndarray:
        """Produce a cube from slices.

        avg_pol=True, correct=True averages polarisations after correcting for the primary beam.
        avg_pol=True, correct=False averages polarisations weighting for the primary beam.
        avg_pol=False, correct=True returns polarisations corrected for the primary beam.
        avg_pol=False, correct=False returns polarisations without primary beam correction.
        """
        ts = np.asarray(self.data[:, y_slice, x_slice, self.channel, self.steps[0] : self.steps[1]], dtype=float)
        if avg_pol is True:
            beam = self.pix2beam(x_slice, y_slice, avg_pol=False) if self.has_beam else None
            return self._average(ts, beam, correct)
        if correct is True and self.has_beam:
            return ts / self.pix2beam(x_slice, y_slice, avg_pol=False)[..., np.newaxis]
        return ts

    def pix2cube(self, x: float, y: float, n: int, m: Optional[int] = None, avg_pol=True, correct=True) -> np.ndarray:
        """Produce an n x m cube centred on a given pixel.

        n is required. m defaults to n.
        """
        if m is None:
            m = n
        x_slice = pix2stamp(x, y, n)[0]
        y_slice = pix2stamp(x, y, m)[1]
        return self.slice2cube(x_slice, y_slice, avg_pol, correct)

    def pix2rms(self, x: float, y: float, avg_pol: bool = True, correct: bool = True) -> np.ndarray:
        """Calculate the local RMS centred on x, y for each timestep."""
        size = int(round(10 * max(1, sqrt(self.get_pixel_beam()))))
        cube = self.pix2cube(x, y, size, avg_pol=avg_pol, correct=correct)
        if avg_pol is True:
            return semihex(cube.reshape(cube.shape[0] * cube.shape[1], cube.shape[2]), axis=0)
        return semihex(cube.reshape(cube.shape[0], cube.shape[1] * cube.shape[2], cube.shape[3]), axis=1)

    def get_continuum(self, avg_pol: bool = True, correct: bool = True, sigma_weight: bool = True, scale: bool = True):
        """Generate a continuum image."""
        cont = np.asarray(self.group["continuum"][:, :, :, self.channel, 0], dtype=float)
        if avg_pol is True:
            if self.has_beam:
                beam = self.group["beam"][:, :, :, self.channel, 0]
                if scale is True:
                    beam = self.scale_beam(beam)
                sigma = self.sigma if sigma_weight is True else np.ones(self.n_pol)
                return sault_weight(cont, beam, sigma, correct)
            return self._average(cont, None, correct)
        if correct is True and self.has_beam:
            beam = self.group["beam"][:, :, :, self.channel, 0]
            if scale is True:
                beam = self.scale_beam(beam)
            return cont / beam
        return cont

    def get_beam(self, sigma: bool = True) -> np.ndarray:
        """Get the properly weighted beam."""
        self._require_beam()
        beam = self.group["beam"][:, :, :, self.channel, 0]
        weighting = self.sigma if sigma is True else np.ones(self.n_pol)
        while len(beam.shape) > len(weighting.shape):
            weighting = weighting[..., np.newaxis]
        return sault_beam(beam, weighting)


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
    help="Directory containing the FITS images and primary beam files",
)
@click.option("--n-timesteps", "-n", type=int, required=True, help="Number of timesteps to process")
@click.option(
    "--outfile",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output HDF5 path. Defaults to <input-dir>/<obsid>.hdf5",
)
@click.option("--check-filenames-only", is_flag=True, help="Validate that all expected files exist, then quit")
@click.option("--overwrite", is_flag=True, help="Overwrite an existing output file")
def _create(
    cfg: ImageStackConfig,
    obsid: str,
    input_dir: Path,
    n_timesteps: int,
    outfile: Optional[Path],
    check_filenames_only: bool,
    overwrite: bool,
):
    ctx = ImageStackContext(
        cfg=cfg,
        obsid=obsid,
        input_dir=input_dir,
        n_timesteps=n_timesteps,
        outfile=outfile,
        check_filenames_only=check_filenames_only,
        overwrite=overwrite,
    )
    create_image_stack(ctx)


entrypoint = click.Group(
    name="imstack",
    help="Build and query HDF5 image stacks made from WSClean FITS images.",
)
entrypoint.add_command(_create)


if __name__ == "__main__":
    entrypoint()

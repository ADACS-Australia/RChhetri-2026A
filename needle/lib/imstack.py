"""
Reads and queries HDF5 image stacks produced by the imstack module.

An image stack stores a sequence of images with a shared celestial WCS, timestamps and
per-timestep metadata. Older stacks may also contain a primary beam. It is intended for fast
time-domain queries over a single field.

This module was adapted with permission from John Morgan's code: https://github.com/johnsmorgan/imstack
"""

import logging
from math import ceil, pi, sqrt
from pathlib import Path
from typing import Optional, Union

import h5py
import numpy as np
from astropy import wcs

logger = logging.getLogger(__name__)


def pix2stamp(x: float, y: float, n: int) -> tuple[slice, slice]:
    """Produce pixel slices for an n x n stamp centred on x, y.

    >>> pix2stamp(1.6, 3.2, 1)
    (slice(2, 3, None), slice(3, 4, None))
    >>> pix2stamp(1.6, 3.2, 2)
    (slice(1, 3, None), slice(3, 5, None))
    >>> pix2stamp(1.6, 3.2, 3)
    (slice(1, 4, None), slice(2, 5, None))
    """
    if n < 1:
        raise ValueError(f"Stamp size must be positive, got {n}")
    if n % 2:
        half = (n - 1) // 2
        return tuple(slice(int(round(i)) - half, int(round(i)) + half + 1) for i in (x, y))
    half = n // 2
    return tuple(slice(int(ceil(i)) - half, int(ceil(i)) + half) for i in (x, y))


def semihex(data: np.ndarray, axis: Optional[int] = None) -> np.ndarray:
    """Calculate the standard deviation via the semi-interhexile range."""
    h1, h5 = np.percentile(data, (100 / 6.0, 500 / 6.0), axis=axis)
    return (h5 - h1) / 2.0


def hypotn(data: np.ndarray, axis: int = 0) -> np.ndarray:
    """Like hypot but not restricted to two dimensions."""
    return np.sqrt(np.sum(data**2, axis=axis))


def sault_beam(beam: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    """Combine beams with Sault weighting.

    See 1996A&AS..120..375S Equation 1. Pointing/polarisation is the first axis.
    """
    beam = np.atleast_1d(np.asarray(beam, dtype=float))
    sigma = np.atleast_1d(np.asarray(sigma, dtype=float))
    while beam.ndim > sigma.ndim:
        sigma = sigma[..., np.newaxis]
    return np.sqrt(np.sum(beam**2 / sigma**2, axis=0) / np.sum(sigma**-2, axis=0))


def sault_weight(data: np.ndarray, beam: np.ndarray, sigma: np.ndarray, correct: bool = False) -> np.ndarray:
    """Combine data from multiple pointings or polarisations with Sault weighting.

    See 1996A&AS..120..375S Equation 1. Pointing/polarisation must be the first axis for both
    data and beam.

    :param data: The data to combine
    :param beam: The primary beam, or a scalar/array broadcastable against data
    :param sigma: The noise per pointing/polarisation
    :param correct: Return the weighted mean without applying the beam correction
    """
    while data.ndim > beam.ndim:
        beam = beam[..., np.newaxis]
    while data.ndim > sigma.ndim:
        sigma = sigma[..., np.newaxis]
    weight = np.sum(beam**2 / sigma**2, axis=0)
    combined = np.sum(data * beam / sigma**2, axis=0) / weight
    if correct:
        return combined
    return combined * np.sqrt(weight / np.sum(sigma**-2))


class ImageStack:
    """Access an HDF5 image stack written by the imstack module.

    The stack axes are (polarisation, y, x, channel, time) for image data and, when present,
    (polarisation, y, x, channel, 1) for the primary beam. x is the fastest varying axis.
    """

    def __init__(
        self,
        h5filename: Union[Path, str],
        image_type: Optional[str] = "image",
        freq: Optional[str] = None,
        steps: Optional[tuple[int, Optional[int]]] = None,
        mode: str = "r",
    ):
        if image_type is not None:
            image_type = self._validate_image_type(image_type)

        self.df = h5py.File(h5filename, mode)
        self.image_type = image_type
        self.group = self.df["/"] if freq is None else self.df[freq]
        self.data = self.group[image_type] if image_type is not None else None
        self.n_pol = self.data.shape[0] if self.data is not None else None
        self.header = self.group["header"].attrs
        self.wcs = wcs.WCS(self._decoded_header())
        self.channel = 0
        self.beam = "beam" in self.group
        self._refresh_beam()

        if steps is None:
            self.steps = None if self.data is None else [0, self.data.shape[-1]]
        else:
            self.steps = [steps[0], steps[1]]
            if self.steps[1] is None:
                if self.data is None:
                    raise ValueError("Cannot infer the upper step without image data")
                self.steps[1] = self.data.shape[-1]

    @staticmethod
    def _validate_image_type(image_type: str) -> str:
        supported = ("image", "dirty", "model", "moment")
        if image_type not in supported:
            raise ValueError(f"Unsupported image_type {image_type}. Expected one of {supported}")
        return image_type

    def _decoded_header(self) -> dict:
        return {k: v.decode("ascii") if isinstance(v, bytes) else v for k, v in self.header.items()}

    def _refresh_beam(self) -> None:
        if self.beam:
            sigma = self.group["beam"].attrs.get("SIGMA", np.ones(self.n_pol or 1))
            self.sigma = np.atleast_1d(np.asarray(sigma, dtype=float))
            if self.n_pol is not None and self.sigma.size not in (1, self.n_pol):
                raise ValueError(f"Beam SIGMA has {self.sigma.size} entries but the stack has {self.n_pol} pols")
            if "SCALE" in self.group["beam"].attrs:
                self.scale = self.group["beam"].attrs["SCALE"]
            else:
                self.scale = np.array([1.0])
        else:
            self.sigma = np.ones(self.n_pol or 1)
            self.scale = np.array([1.0])

    def close(self) -> None:
        """Close the underlying HDF5 file."""
        self.df.close()

    def __enter__(self) -> "ImageStack":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def update(
        self,
        freq: Optional[str] = None,
        image_type: Optional[str] = None,
        steps: Optional[tuple[int, Optional[int]]] = None,
    ) -> None:
        """Point the stack at a different group, image type or time range."""
        if freq is not None:
            self.group = self.df[freq]
            self.header = self.group["header"].attrs
            self.wcs = wcs.WCS(self._decoded_header())
            self.beam = "beam" in self.group
            self.data = self.group[self.image_type] if self.image_type is not None else None
            self.n_pol = self.data.shape[0] if self.data is not None else None
            self._refresh_beam()

        if image_type is not None:
            self.image_type = self._validate_image_type(image_type)
            self.data = self.group[image_type]
            self.n_pol = self.data.shape[0]

        if steps is not None:
            self.steps = [steps[0], steps[1]]
            if self.steps[1] is None:
                self.steps[1] = self.data.shape[-1]

    def check_slice(self, x: float, y: float, margin: int = 0) -> bool:
        """Check that a stamp centred on x, y lies entirely within the image."""
        margin_pixels = margin + int(round(10 * max(1, sqrt(self.get_pixel_beam()))))
        x_slice, y_slice = pix2stamp(x, y, margin_pixels)
        return (
            0 <= x_slice.start
            and x_slice.stop <= self.data.shape[2]
            and 0 <= y_slice.start
            and y_slice.stop <= self.data.shape[1]
        )

    def get_interval(self) -> float:
        """Get the interval between neighbouring images."""
        return self.group.attrs["TIME_INTERVAL"]

    def get_intervals(self) -> np.ndarray:
        """Get the series of intervals for the currently selected steps."""
        return self.group.attrs["TIME_INTERVAL"] * np.arange(self.steps[1] - self.steps[0])

    def world2pix(self, ra: float, dec: float, floor: bool = True) -> tuple:
        """Return pixel coordinates x, y for a sky position. x is the fastest varying axis."""
        pixcoord = self.wcs.celestial.wcs_world2pix(np.array([[ra, dec]]), 0)
        if floor:
            pixcoord = np.round(pixcoord).astype(int)
        return pixcoord[0, 0], pixcoord[0, 1]

    def pix2world(self, x: float, y: float) -> tuple:
        """Return sky coordinates ra, dec for a pixel position. x is the fastest varying axis."""
        ra_dec = self.wcs.celestial.wcs_pix2world(np.array([[x, y]]), 0)
        return ra_dec[0, 0], ra_dec[0, 1]

    def get_pixel_beam(self) -> float:
        """Get the size of the synthesised beam in pixels.

        Assumes square pixels and a roughly circular synthesised beam.
        """
        return abs(
            pi * 0.25 * self.header["BMAJ"] * self.header["BMIN"] / (self.header["CDELT1"] * self.header["CDELT2"])
        )

    def get_scale(self) -> np.ndarray:
        """Get the beam scale factor stored in the stack."""
        if "beam" in self.group and "SCALE" in self.group["beam"].attrs:
            return self.group["beam"].attrs["SCALE"]
        return np.array([1.0])

    def scale_beam(self, beam: np.ndarray) -> np.ndarray:
        """Apply the beam scale factor, which may be scalar or one value per polarisation."""
        scale = np.atleast_1d(np.squeeze(self.get_scale())).astype(float)
        if scale.size == 1:
            return beam * scale[0]
        if beam.ndim and beam.shape[0] == scale.size:
            return beam * scale.reshape((-1,) + (1,) * (beam.ndim - 1))
        raise ValueError(f"Cannot apply {scale.size} scale values to a beam of shape {beam.shape}")

    def pix2beam(self, x: float, y: float, avg_pol: bool = True, scale: bool = True):
        """Get the primary beam corresponding to pixel coordinates x, y."""
        beam = self.group["beam"][:, y, x, self.channel, 0]
        if scale:
            beam = self.scale_beam(beam)
        if avg_pol:
            if not np.any(beam):
                return 0.0
            return sault_beam(beam, self.sigma)
        return beam

    def world2beam(self, ra: float, dec: float, avg_pol: bool = True, scale: bool = True):
        """Get the primary beam corresponding to sky coordinates ra, dec."""
        x, y = self.world2pix(ra, dec)
        return self.pix2beam(x, y, avg_pol, scale=scale)

    def pix2ts(self, x: float, y: float, avg_pol: bool = True, correct: bool = True):
        """Produce a timeseries for a given pixel.

        :param avg_pol: Average polarisations
        :param correct: Correct for the primary beam. Only applies when avg_pol is True
        """
        ts = self.data[:, y, x, self.channel, self.steps[0] : self.steps[1]].astype(np.float64)
        if avg_pol:
            if self.n_pol == 1:
                if self.beam and correct:
                    return ts[0] / np.asarray(self.pix2beam(x, y, avg_pol=False))[0]
                return ts[0]
            if not self.beam:
                logger.warning("No beam stored in the stack. Returning the unweighted polarisation mean")
                return np.nanmean(ts, axis=0)
            beam = self.pix2beam(x, y, avg_pol=False)
            return sault_weight(ts, beam, self.sigma, correct)
        if correct:
            if not self.beam:
                logger.warning("No beam stored in the stack. Returning uncorrected data")
                return ts[0] if self.n_pol == 1 else ts
            beam = np.asarray(self.pix2beam(x, y, avg_pol=False))
            if self.n_pol == 1:
                return ts[0] / beam[0]
            return ts / beam[:, np.newaxis]
        return ts if self.n_pol > 1 else ts[0]

    def world2ts(self, ra: float, dec: float, avg_pol: bool = True, correct: bool = True):
        """Produce a timeseries for a given sky position."""
        x, y = self.world2pix(ra, dec)
        return self.pix2ts(x, y, avg_pol, correct)

    def slice2cube(
        self,
        x_slice: slice,
        y_slice: slice,
        avg_pol: bool = True,
        correct: bool = True,
    ) -> np.ndarray:
        """Produce an image cube for a pixel slice.

        :param avg_pol: Average polarisations
        :param correct: Correct for the primary beam. Only applies when avg_pol is True
        """
        cube = self.data[:, y_slice, x_slice, self.channel, self.steps[0] : self.steps[1]].astype(np.float64)
        if avg_pol:
            if self.n_pol == 1:
                if self.beam and correct:
                    beam = np.asarray(self.pix2beam(x_slice, y_slice, avg_pol=False))[0]
                    return cube[0] / beam[..., np.newaxis]
                return cube[0]
            if not self.beam:
                logger.warning("No beam stored in the stack. Returning the unweighted polarisation mean")
                return np.nanmean(cube, axis=0)
            return sault_weight(cube, self.pix2beam(x_slice, y_slice, avg_pol=False), self.sigma, correct)
        if correct:
            if not self.beam:
                logger.warning("No beam stored in the stack. Returning uncorrected data")
                return cube[0] if self.n_pol == 1 else cube
            beam = np.asarray(self.pix2beam(x_slice, y_slice, avg_pol=False))
            if self.n_pol == 1:
                return cube[0] / beam[0][..., np.newaxis]
            return cube / beam[..., np.newaxis]
        return cube

    def pix2cube(self, x: float, y: float, n: int, m: Optional[int] = None, avg_pol: bool = True, correct: bool = True):
        """Produce an n x m cube centred on a pixel. m defaults to n."""
        if m is None:
            m = n
        x_slice = pix2stamp(x, y, n)[0]
        y_slice = pix2stamp(x, y, m)[1]
        return self.slice2cube(x_slice, y_slice, avg_pol, correct)

    def pix2rms(self, x: float, y: float, avg_pol: bool = True, correct: bool = True) -> np.ndarray:
        """Calculate the local rms at each timestep for a stamp centred on x, y."""
        n = int(round(10 * max(1, sqrt(self.get_pixel_beam()))))
        cube = self.pix2cube(x, y, n, avg_pol=avg_pol, correct=correct)
        if avg_pol:
            return semihex(cube.reshape(-1, cube.shape[-1]), axis=0)
        return semihex(cube.reshape(cube.shape[0], -1, cube.shape[-1]), axis=1)

    def get_continuum(self, avg_pol: bool = True, correct: bool = True, sigma_weight: bool = True, scale: bool = True):
        """Generate a continuum image from the stack."""
        cont = self.group["continuum"][:, :, :, self.channel, 0]
        beam = self.group["beam"][:, :, :, self.channel, 0]
        if scale:
            beam = self.scale_beam(beam)
        sigma = self.sigma if sigma_weight else np.ones_like(self.sigma)
        if avg_pol:
            return sault_weight(cont, beam, sigma, correct)
        if correct:
            return cont / beam
        return cont

    def get_beam(self, sigma: bool = True) -> np.ndarray:
        """Get the properly weighted primary beam."""
        beam = self.group["beam"][:, :, :, self.channel, 0]
        weight = self.sigma if sigma else np.ones_like(self.sigma)
        return sault_beam(beam, weight)

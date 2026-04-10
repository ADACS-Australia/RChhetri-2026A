#!/usr/bin/env python3
"""
Read an Aegean source catalogue JSON and a reference FITS image, then write
a FITS mask file suitable for WSClean (-fits-mask).

Each source gets a rectangular box, sized as `padding * major_axis`, written
as non-zero pixels into an otherwise zero mask. WSClean will only clean inside
these boxes.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path

from astropy.io import fits
from astropy.wcs import WCS
import numpy as np
from pydantic import field_validator

from needle.config.mask import CreateMaskOutput, CreateMaskConfig
from needle.lib.aegean import AegeanSourceList
from needle.lib.logging import setup_logging
from needle.lib.validate import validate_path_fits
from needle.modules.needle_context import NeedleContext

logger = logging.getLogger(__name__)


class CreateMaskContext(NeedleContext):

    cfg: CreateMaskConfig
    "Static configuration for mask creation"

    image: Path
    "Path to the fits image. The intended mask target - used for size reference"

    sources: Path | AegeanSourceList
    "The source list. Also accepts a .json of the sources"

    @field_validator("sources")
    @classmethod
    def _valid_sources(cls, s) -> AegeanSourceList:
        """Convert .json to AegeanSourceList if necessary"""
        if isinstance(s, AegeanSourceList):
            return s
        return AegeanSourceList.from_json(s)

    @field_validator("image")
    @classmethod
    def _valid_image(cls, im) -> Path:
        validate_path_fits(im)
        return im

    def execute(self) -> CreateMaskOutput:
        """Creates the mask and writes it to file"""
        output = CreateMaskOutput(prefix=self.image.with_suffix(""))
        mask, header = generate_mask_array(
            source_list=self.sources, reference_fits=self.image, padding=self.cfg.padding
        )
        n_masked = int(mask.sum())
        total = mask.size
        if mask.size == 0:
            logger.warning("Mask is of size zero!")
        else:
            logger.info(f"Masked pixels : {n_masked:,} / {total:,} " f"({100 * n_masked / total:.2f}%)")

        fits.writeto(str(output.mask), mask, header, overwrite=True)
        return output


def generate_mask_array(
    source_list: AegeanSourceList | Path, reference_fits: Path, padding: float
) -> tuple[np.ndarray, fits.Header]:
    """
    Build a 2D boolean mask array with boxes around each source.

    :param source_list: The Aegean source list.
        Either the object itself or the path to the .json created by the source_find module
    :param reference_fits: A FITS image with the WCS and shape to match.
    :param padding: Box half-width = padding * semi-major axis (arcsec).
    :return: Tuple of the mask and the fits header
    """

    if not isinstance(source_list, AegeanSourceList):
        source_list = AegeanSourceList.from_json(source_list)

    with fits.open(reference_fits) as hdul:
        header = hdul[0].header.copy()
        data_shape = hdul[0].data.shape

    # Squeeze shape to 2D (handles 4D Stokes/freq cubes)
    ny, nx = data_shape[-2], data_shape[-1]
    wcs = WCS(header, naxis=2)
    mask = np.zeros((ny, nx), dtype=np.uint8)

    # Estimate pixel scale from WCS (arcsec/pixel)
    try:
        pix_scale_deg = abs(header.get("CDELT1") or float(wcs.pixel_scale_matrix[0, 0]))
    except Exception:
        pix_scale_deg = 1.0 / 3600.0
    pix_scale_arcsec = pix_scale_deg * 3600.0

    skipped = 0
    for src in source_list.sources:
        if src.ra is None or src.dec is None or src.a is None:
            skipped += 1
            continue

        # sky -> pixel
        x, y = wcs.all_world2pix([[src.ra, src.dec]], 0)[0]
        x, y = float(x), float(y)

        # half-width of box in pixels
        half = padding * float(src.a) / pix_scale_arcsec

        # bounding box clipped to image
        x0 = max(0, int(np.floor(x - half)))
        x1 = min(nx, int(np.ceil(x + half)))
        y0 = max(0, int(np.floor(y - half)))
        y1 = min(ny, int(np.ceil(y + half)))

        mask[y0:y1, x0:x1] = 1

    if skipped:
        logger.info(f"Skipped {skipped} sources with missing ra/dec/a")

    # Build a clean 2D header
    out_header = fits.Header()
    for key in (
        "SIMPLE",
        "BITPIX",
        "NAXIS",
        "NAXIS1",
        "NAXIS2",
        "CRPIX1",
        "CRPIX2",
        "CRVAL1",
        "CRVAL2",
        "CDELT1",
        "CDELT2",
        "CTYPE1",
        "CTYPE2",
        "CUNIT1",
        "CUNIT2",
        "EQUINOX",
        "RADESYS",
    ):
        if key in header:
            out_header[key] = header[key]

    # Copy CD matrix if present (alternative to CDELT)
    for key in ("CD1_1", "CD1_2", "CD2_1", "CD2_2"):
        if key in header:
            out_header[key] = header[key]

    out_header["NAXIS"] = 2
    out_header["NAXIS1"] = nx
    out_header["NAXIS2"] = ny
    out_header["BITPIX"] = 8
    out_header["BUNIT"] = "mask"
    out_header["COMMENT"] = "WSClean FITS mask - 1=clean, 0=skip"

    return mask, out_header


def create_mask(ctx: CreateMaskContext) -> CreateMaskOutput:
    """Creates a mask using a list of AegeanSources

    :param ctx: The CreateMaskContext object
    :return: The CreateMaskOutput object
    """
    logger.info(f"Building mask from reference image {ctx.image} ...")
    output = ctx.execute()
    logger.info(f"Mask written: {output.mask}")
    return output


def main():
    parser = CreateMaskConfig.add_to_parser(
        ArgumentParser("Calibrate a target measurement set using a calibrator measurement set.")
    )
    parser.add_argument("--image", type=Path, required=True, help="The path to the .fits image. Used as a refernce")
    parser.add_argument(
        "--sources",
        type=Path,
        required=True,
        help="The path to the .json file containing a list of source locations in the image",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    create_mask(CreateMaskContext(cfg=CreateMaskConfig.from_namespace(args), image=args.image, sources=args.sources))


if __name__ == "__main__":
    main()

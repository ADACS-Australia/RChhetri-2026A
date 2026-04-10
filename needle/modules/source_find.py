"""
find_sources.py
---------------
Run BANE and Aegean on a FITS image to detect radio sources.
Outputs the sources to a .json file
"""

import argparse
from astropy.io import fits
import numpy as np
import logging
from pathlib import Path
from typing import Tuple

from AegeanTools.BANE import filter_image
from AegeanTools.source_finder import SourceFinder
from pydantic import field_validator, computed_field

from needle.config.base import NeedleModel
from needle.config.source_find import SourceFindConfig
from needle.lib.aegean import AegeanSourceList
from needle.lib.logging import setup_logging
from needle.lib.validate import validate_path_fits
from needle.modules.needle_context import SubprocessExecContext


logger = logging.getLogger(__name__)


class SourceFindOutput(NeedleModel):
    """Class to encompass the outputs of BANE and Aegean"""

    prefix: Path
    "The prefix path. Will be used for naming output files."

    @property
    def bkg_path(self) -> Path:
        return Path(str(self.prefix) + "_bkg").with_suffix(".fits")

    @property
    def rms_path(self) -> Path:
        return Path(str(self.prefix) + "_rms").with_suffix(".fits")

    @property
    def sources_txt(self) -> Path:
        return Path(str(self.prefix) + "-sources").with_suffix(".txt")


class SourceFindContext(SubprocessExecContext):
    """Context object for the Source Finding module"""

    cfg: SourceFindConfig
    "Static configuration for the source finding module"

    image: Path
    "Fits image to source-find"

    @property
    def output(self) -> SourceFindOutput:
        return SourceFindOutput(prefix=self.image.with_suffix(""))

    @field_validator("image")
    @classmethod
    def valid_image(cls, im: Path) -> Path:
        validate_path_fits(im)
        return im

    @computed_field
    def squeezed_image(self) -> Path:
        return squeeze_fits(self.image)

    @property
    def cmd(self) -> list[list[str]]:
        bane_cmd = [
            "BANE",
            str(self.squeezed_image),
            "--out",
            str(self.output.prefix),
            "--cores",
            str(self.cfg.cores),
            "--stripes",
            str(self.cfg.cores - 1),
        ]
        aegean_cmd = [
            "aegean",
            str(self.squeezed_image),
            "--background",
            str(self.output.bkg_path),
            "--noise",
            str(self.output.rms_path),
            "--seedclip",
            str(self.cfg.innerclip),
            "--floodclip",
            str(self.cfg.outerclip),
            "--cores",
            str(self.cfg.cores),
            "--out",
            str(self.output.sources_txt),
        ]
        if self.cfg.max_summits:
            aegean_cmd += ["--maxsummits", str(self.cfg.max_summits)]
        return [bane_cmd, aegean_cmd]


def squeeze_fits(fits_path: Path) -> Path:
    """If the FITS file has >2 dimensions, squeeze to 2D and save a new file.

    :param fits_path: The path to the fits file to squeeze
    :return: The squeezed file. Will output the input file if no squeezing was necessary
    """

    squeezed_path = fits_path.with_name(fits_path.stem + "-2d.fits")
    with fits.open(fits_path) as hdul:
        data = hdul[0].data
        header = hdul[0].header
        if data.ndim > 2:
            logger.info("Squeezing fits to two dimensions")
            data = np.squeeze(data)
            # update header to match
            header["NAXIS"] = 2
            header["NAXIS1"] = data.shape[1]
            header["NAXIS2"] = data.shape[0]
            for key in [
                "NAXIS3",
                "NAXIS4",
                "CRPIX3",
                "CRVAL3",
                "CDELT3",
                "CRPIX4",
                "CRVAL4",
                "CDELT4",
                "CTYPE3",
                "CTYPE4",
                "CUNIT3",
                "CUNIT4",
            ]:
                header.remove(key, ignore_missing=True)
            fits.writeto(squeezed_path, data, header, overwrite=True)
            return squeezed_path
        else:
            logger.debug(f"No transformation performed on {fits} as it's already 2D")
    return fits_path  # already 2D, return original


def run_bane(fits_path: Path, cores: int = 1) -> Tuple[Path, Path]:
    """Generate background and RMS noise maps using BANE.

    :param fits_path: The path to the fits image file to operate on
    :param cores: The number of cores to use for processing
    :return: The path to the background image and rms noise image respectively
    """

    # BANE deadlocks if nslices >= cores
    slices = cores - 1
    logger.info(f"Running BANE on {fits_path} with {cores} cores and {slices} slices ...")
    filter_image(str(fits_path), out_base=fits_path.with_suffix(""), cores=cores, nslice=slices)

    output = SourceFindOutput(prefix=fits_path.with_suffix(""))
    if not output.bkg_path.exists() or not output.rms_path.exists():
        raise FileNotFoundError(f"One or more expected files not found: {output.bkg_path} | {output.rms_path}")

    logger.info(f"Background map : {output.bkg_path}")
    logger.info(f"RMS noise map  : {output.rms_path}")
    return output.bkg_path, output.rms_path


def run_aegean(fits_path: Path, bkg_path: Path, rms_path: Path, cfg: SourceFindConfig) -> SourceFindOutput:
    """Run Aegean source finder and write catalogue files.

    :param fits_path: The path to the fits image file to operate on
    :param bkg_path: The path to the background image
    :param rms_path: The path to the rms noise image
    :param cfg: The SourceFindConfig options for the source finder algorithm
    """
    logger.info(f"Running Aegean on {fits_path} ...")

    output = SourceFindOutput(prefix=fits_path.with_suffix(""))
    sf = SourceFinder()
    with open(output.sources_txt, "w") as f:
        sources = sf.find_sources_in_image(
            str(fits_path), outfile=f, bkgin=str(bkg_path), rmsin=str(rms_path), **cfg.to_kwargs()
        )
    logger.debug(f"Sources found : {len(sources)}")

    sources = AegeanSourceList.from_component_list(sources)
    sources.to_json(output.sources_json)
    logger.info(f"Sources written to {output.sources_json}")
    return output


def source_find(ctx: SourceFindContext) -> SourceFindOutput:
    """Finds sources in a fits image using BANE and Aegean.

    :param ctx: The SourceFindContext object
    :return: The source finder output packaged into an object
    """
    logger.info(f"Running BANE + Aegean on {ctx.image}")
    ctx.execute()

    assert ctx.output.rms_path.exists(), f"RMS path does not exist: {ctx.output.rms_path}"
    assert ctx.output.bkg_path.exists(), f"Background path does not exist: {ctx.output.bkg_path}"

    logger.info(f"Source finding complete. Sources written to {ctx.output.sources_txt}")
    return ctx.output


def main():
    parser = SourceFindConfig.add_to_parser(
        argparse.ArgumentParser(description="Run BANE + Aegean to find sources in a FITS image.")
    )

    required_group = parser.add_argument_group("Required Arguments")
    required_group.add_argument(
        "--image", type=Path, required=True, help="The path to the fits image to use for source finding"
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
    source_find(SourceFindContext(cfg=SourceFindConfig.from_namespace(args), image=args.image))


if __name__ == "__main__":
    main()

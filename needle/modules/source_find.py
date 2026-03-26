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

from needle.models.source_find import SourceFindContext, SourceFindOutput, SourceFindConfig
from needle.lib.aegean import AegeanSourceList
from needle.lib.logging import setup_logging


logger = logging.getLogger(__name__)


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
            logger.info(f"D -> 2D: {squeezed_path}")
            return squeezed_path
        else:
            logger.debug(f"No transformation performed on {fits} as it's already 2D")
    return fits_path  # already 2D, return original


# def run_bane(fits_path: Path, cores: int = 1) -> Tuple[Path, Path]:
#     """Generate background and RMS noise maps using BANE.
#     Runs via the BANE CLI to avoid deadlocks when called from within a process pool.
#
#     :param fits_path: The path to the fits image file to operate on
#     :param cores: The number of cores to use for processing
#     :return: The path to the background image and rms noise image respectively
#     """
#     logger.info(f"Running BANE on {fits_path} with {cores} cores ...")
#     result = subprocess.run(
#         ["BANE", "--cores", str(cores), str(fits_path)],
#         capture_output=True,
#         text=True,
#     )
#     if result.stdout:
#         logger.debug(f"BANE stdout:\n{result.stdout}")
#     if result.stderr:
#         logger.debug(f"BANE stderr:\n{result.stderr}")
#     if result.returncode != 0:
#         raise RuntimeError(f"BANE failed with return code {result.returncode}:\n{result.stderr}")
#
#     output = SourceFindOutput(prefix=fits_path.with_suffix(""))
#     if not output.bkg_path.exists() or not output.rms_path.exists():
#         raise FileNotFoundError(f"One or more expected files not found: {output.bkg_path} | {output.rms_path}")
#     logger.info(f"Background map : {output.bkg_path}")
#     logger.info(f"RMS noise map  : {output.rms_path}")
#     return output.bkg_path, output.rms_path


# def run_aegean(fits_path: Path, bkg_path: Path, rms_path: Path, cfg: SourceFindConfig) -> SourceFindOutput:
#     """Run Aegean source finder and write catalogue files.
#     Runs via the Aegean CLI to avoid deadlocks when called from within a process pool.
#
#     :param fits_path: The path to the fits image file to operate on
#     :param bkg_path: The path to the background image
#     :param rms_path: The path to the rms noise image
#     :param cfg: The SourceFindConfig options for the source finder algorithm
#     """
#     logger.info(f"Running Aegean on {fits_path} ...")
#     output = SourceFindOutput(prefix=fits_path.with_suffix(""))
#     result = subprocess.run(
#         [
#             "aegean",
#             "--background",
#             str(bkg_path),
#             "--noise",
#             str(rms_path),
#             "--table",
#             str(output.sources_json),
#             *cfg.to_cli_args(),
#             str(fits_path),
#         ],
#         capture_output=True,
#         text=True,
#     )
#     if result.stdout:
#         logger.debug(f"Aegean stdout:\n{result.stdout}")
#     if result.stderr:
#         logger.debug(f"Aegean stderr:\n{result.stderr}")
#     if result.returncode != 0:
#         raise RuntimeError(f"Aegean failed with return code {result.returncode}:\n{result.stderr}")
#
#     logger.info(f"Sources written to {output.sources_json}")
#     return output
#


def source_find(ctx: SourceFindContext) -> SourceFindOutput:
    """Finds sources in a fits image using BANE and Aegean Source Finder

    :param ctx: The SourceFindContext object
    :return: The source finder ouput packaged into an object
    """
    # Squeeze the fits file if we have to. Does nothing if not needed.
    fits_path = squeeze_fits(ctx.image)
    # BANE - noise removal
    bkg_path, rms_path = run_bane(fits_path, cores=ctx.cfg.cores)
    # Aegean - source finding
    output = run_aegean(fits_path, bkg_path, rms_path, cfg=ctx.cfg)
    assert output.rms_path.exists(), "rms path does not exist"
    assert output.bkg_path.exists(), "bkg path does not exist"
    assert output.sources_json.exists(), "sources json does not exist"
    return output


# def run_bane(fits_path: Path, cores: int = 1) -> Tuple[Path, Path]:
#     logger.info(f"Running BANE on {fits_path} with {cores} cores ...")
#
#     script = f"""
# import multiprocessing
# multiprocessing.set_start_method("spawn")
# from AegeanTools.BANE import filter_image
# filter_image("{fits_path}", out_base="{fits_path.with_suffix('')}", cores={cores}, nslice=0)
# """
#     with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
#         f.write(script)
#         script_path = f.name
#
#     result = subprocess.run(
#         [sys.executable, script_path],
#         capture_output=True,
#         text=True,
#     )
#     Path(script_path).unlink(missing_ok=True)
#
#     if result.stdout:
#         logger.debug(f"BANE stdout:\n{result.stdout}")
#     if result.stderr:
#         logger.debug(f"BANE stderr:\n{result.stderr}")
#     if result.returncode != 0:
#         raise RuntimeError(f"BANE failed with return code {result.returncode}:\n{result.stderr}")
#
#     output = SourceFindOutput(prefix=fits_path.with_suffix(""))
#     if not output.bkg_path.exists() or not output.rms_path.exists():
#         raise FileNotFoundError(f"One or more expected files not found: {output.bkg_path} | {output.rms_path}")
#     logger.info(f"Background map : {output.bkg_path}")
#     logger.info(f"RMS noise map  : {output.rms_path}")
#     return output.bkg_path, output.rms_path
#


def run_bane(fits_path: Path, cores: int = 1) -> Tuple[Path, Path]:
    """Generate background and RMS noise maps using BANE.

    :param fits_path: The path to the fits image file to operate on
    :param cores: The number of cores to use for processing
    :return: The path to the background image and rms noise image respectively
    """

    # BANE deadlocks if nslices >= cores
    # TODO: Revert
    slices = 0
    logger.info(f"Running BANE on {fits_path} with {cores} cores and {slices} slices ...")
    # with patch.object(multiprocessing, "get_context", return_value=multiprocessing.get_context("spawn")):
    #     filter_image(str(fits_path), out_base=str(fits_path.with_suffix("")), cores=cores, nslice=0)
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


def main():
    parser = SourceFindConfig.add_to_parser(
        argparse.ArgumentParser(description="Run BANE + Aegean to find sources in a FITS image.")
    )
    parser.add_argument(
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

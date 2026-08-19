"""
Runs WSClean on a measurement set.
Supports different cleaning modes:
    - Shallow clean (no mask)
    - Deep clean (with mask)
    - Model subtraction
"""

from glob import glob
import logging
from pathlib import Path
from typing import Optional

import click
from pydantic import field_validator

from needle.lib.validate import validate_path_ms, validate_path_fits
from needle.config.base import NeedleModel, needle_module_args
from needle.config.clean import WSCleanConfig, ShallowCleanConfig, DeepCleanConfig, ModelSubtractCleanConfig
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class WSCleanOutput(NeedleModel):
    """Class to encompass the outputs of WSClean. Uses glob to find expected files using the name prefix"""

    prefix: Path
    "The prefix path for wsclean outputs. Should be the -name of the context object"

    @property
    def image(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-image.fits")]

    @property
    def psf(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-psf.fits")]

    @property
    def dirty(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-dirty.fits")]

    @property
    def model(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-model.fits")]

    @property
    def residual(self) -> list[Path]:
        return [Path(i) for i in glob(f"{self.prefix}*-residual.fits")]

    def remap_interval_images(self, interval_start: int) -> list[Path]:
        """Rename interval images from chunk-relative to absolute timestep indices.

        WSClean names interval images with a chunk-relative index (e.g. 't0031'), which
        resets to zero for each task. This method renames them to absolute timestep indices
        by offsetting with the interval start (e.g. t0031 with interval_start=87 -> t0118).

        :param interval_start: The absolute timestep at which this interval chunk begins.
        :raises ValueError: Raised if an image with an unrecognised interval token format is found.
        :returns: List of renamed image paths with absolute timestep indices.
        """
        clean_prefix = str(self.prefix).rsplit("_", 2)[0]
        renamed = []
        for path in self.image:
            suffix = path.name[len(Path(self.prefix).name) :]
            t_str, product = suffix[1:].split("-", 1)
            if not (t_str.startswith("t") and t_str[1:].isdigit()):
                raise ValueError(f"Expected WSClean interval token (e.g. 't0031') but got '{t_str}' in '{path.name}'")
            absolute_idx = interval_start + int(t_str[1:])
            new_path = path.parent / f"{Path(clean_prefix).name}-t{absolute_idx:04d}-{product}"
            path.rename(new_path)
            renamed.append(new_path)
        return renamed


class WSCleanContext(SubprocessExecContext):
    """The full runtime context required for running WSClean"""

    cfg: WSCleanConfig
    "Static configuration for WSClean module"

    ms: Path
    "Path to the measurement set to clean"

    fits_mask: Path | None = None
    "Path to a FITS mask"

    interval: tuple[int, int] | None = None
    "A specific time interval range (start, end) to image. Maps to -interval flag. If none, images normally"

    predict: bool = False
    "Predict visibilities - this will create a MODEL_DATA column in the ms"

    output_dir: Path | None = None
    "A directory to output the resulting files to. Default (None) is ms directory."

    @field_validator("ms")
    @classmethod
    def _valid_ms(cls, ms) -> Path:
        validate_path_ms(ms)
        return ms

    @field_validator("fits_mask")
    @classmethod
    def _valid_fits_mask(cls, msk) -> Path | None:
        if msk:
            validate_path_fits(msk)
        return msk

    @property
    def name(self) -> str:
        name = self.ms.with_suffix("")
        if self.output_dir:
            name = str(self.output_dir / Path(self.ms.name).with_suffix(""))
        if self.cfg.tag:
            name = f"{name}_{self.cfg.tag}"
        if self.interval is not None:
            name = f"{name}_{self.interval[0]}_{self.interval[1]}"
        return name

    @property
    def cmd(self) -> list[list[str]]:
        """Constructs the full WSClean command as a list of strings suitable for passing to subprocess.
        Optional parameters are only included if set on the cfg.
        """

        cmd = [
            "wsclean",
            "-name",
            self.name,
            "-size",
            str(self.cfg.size),
            str(self.cfg.size),
            "-scale",
            self.cfg.scale,
            "-niter",
            str(self.cfg.niter),
            "-pol",
            self.cfg.pol,
            "-data-column",
            self.cfg.data_column,
        ]

        if self.cfg.weight == "briggs":  # Add robustness if using briggs weighting
            cmd += ["-weight", "briggs", str(self.cfg.robust)]
        else:
            cmd += ["-weight", self.cfg.weight]
        if self.interval is not None:
            start, end = self.interval
            cmd += ["-intervals-out", str(end - start), "-interval", str(start), str(end)]
        if self.cfg.auto_threshold is not None:
            cmd += ["-auto-threshold", str(self.cfg.auto_threshold)]
        if self.cfg.auto_mask is not None:
            cmd += ["-auto-mask", str(self.cfg.auto_mask)]
        if self.fits_mask is not None:
            cmd += ["-fits-mask", str(self.fits_mask)]
        if self.cfg.minuv_l is not None:
            cmd += ["-minuv-l", str(self.cfg.minuv_l)]
        if self.cfg.subtract_model:
            cmd += ["-subtract-model"]

        # Override the command if predict flag is used. No other cleaning is relevant when using this flag.
        if self.predict:
            cmd = ["wsclean", "-name", self.name, "-predict"]

        cmd.append(str(self.ms))
        return [cmd]  # execute() expects a list of lists

    @property
    def output(self) -> WSCleanOutput:
        """The WSCleanOutput object - the expected outputs from running the cmd"""
        return WSCleanOutput(prefix=self.name)


def run_clean(ctx: WSCleanContext) -> WSCleanOutput:
    """Run WSClean on a measurement set.

    Builds and executes the WSClean command for the given config. Both
    shallow and deep cleans are handled by passing the appropriate
    WSCleanConfig subclass. Output images are written with the given
    name prefix.

    :param ctx: WSClean run context object
    :returns: The wsclean output object
    """
    logger.info(f"Running WSClean on {ctx.ms}")
    ctx.log_cmd()
    procs = ctx.execute()
    for p in procs:
        logger.info(p.stdout)
        if p.stderr:
            logger.warning(p.stderr)
        p.check_returncode()

    logger.info(f"WSClean complete, output image: {ctx.output.image}")
    return ctx.output


# Shared business logic + shared options for every preset.
@click.option(
    "--ms",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="The path to the measurement set to clean",
)
@click.option(
    "--mask",
    "-m",
    type=click.Path(exists=True, path_type=Path),
    required=False,
    help="The path to the fits mask to use for masked clean",
)
def _clean(cfg: WSCleanConfig, ms: Path, mask: Optional[Path]):
    ctx = WSCleanContext(cfg=cfg, ms=ms, fits_mask=mask)
    run_clean(ctx)


_run_cmd = needle_module_args(WSCleanConfig, name="run", help="Run WSClean with generic configuration")(_clean)
_shallow_cmd = needle_module_args(ShallowCleanConfig, name="shallow", help="Run WSClean with shallow-clean presets")(
    _clean
)
_deep_cmd = needle_module_args(DeepCleanConfig, name="deep", help="Run WSClean with deep-clean presets")(_clean)
_subtract_cmd = needle_module_args(
    ModelSubtractCleanConfig, name="subtract", help="Run WSClean with model-subtract presets"
)(_clean)

entrypoint = click.Group(
    name="clean",
    help="""Run WSClean on a measurement set.

    Runs with one of several configuration presets:

    run :: Use the generic configuration
    shallow :: Use configuration for shallow cleaning
    deep :: Use configuration for deep cleaning
    subtract :: Use configuration for subtracting the model from the data
    """,
)
for _cmd in (_run_cmd, _shallow_cmd, _deep_cmd, _subtract_cmd):
    entrypoint.add_command(_cmd)


if __name__ == "__main__":
    entrypoint()

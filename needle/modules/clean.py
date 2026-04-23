"""
Runs WSClean on a calibrated measurement set.
Supports both shallow and deep cleaning via WSCleanConfig subclasses.
"""

from argparse import ArgumentParser, Namespace
from glob import glob
import logging
from pathlib import Path

from pydantic import field_validator

from needle.lib.logging import setup_logging
from needle.lib.validate import validate_path_ms, validate_path_fits
from needle.config.base import ContainerConfig, NeedleModel
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
            name = f"{name}_interval_{self.interval[0]}_{self.interval[1]}"
        return name

    # @property
    # def name(self) -> str:
    #     """The 'name' input to wsclean.
    #     Can use the output variable to write to a different directory
    #     """
    #     # ms path is /path/to/m_set.ms
    #     # /path/to/m_set
    #     name = self.ms.with_suffix("")
    #     if self.output_dir:
    #         # /path/to/out_dir/m_set
    #         name = str(self.output_dir / Path(self.ms.name).with_suffix(""))
    #     if self.cfg.tag:
    #         # /path/to/m_set_tag
    #         name = f"{name}_{self.cfg.tag}"
    #     return name
    #
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
    :raises RuntimeError: If WSClean exits with a non-zero return code
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


def _build_runtime(args: Namespace) -> ContainerConfig | None:
    """Constructs an container config from parsed args, or None if no image was provided"""
    if not args.image:
        return None
    env = dict(item.split("=", 1) for item in args.env) if args.env else None
    return ContainerConfig(image=args.image, binds=args.binds, env=env)


def _parse(parser: ArgumentParser) -> Namespace:
    required_group = parser.add_argument_group("Required Arguments")
    required_group.add_argument("--ms", type=Path, required=True, help="The path to the measurement set to clean")
    required_group.add_argument(
        "--mask", type=Path, required=False, help="The path to the fits mask to use for masked clean"
    )

    container_group = parser.add_argument_group("Container Arguments")
    ContainerConfig.add_to_parser(container_group)

    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        required=False,
        help="The minimum threshold logging level",
    )
    return parser.parse_args()


def model_subtract():
    """Create the MODEL_DATA column"""
    parser = ModelSubtractCleanConfig.add_to_parser(
        ArgumentParser("Run WSClean on a measurement set with model-subtract presets")
    )
    args = _parse(parser)
    setup_logging(args.log_level)
    ctx = WSCleanContext(
        cfg=ModelSubtractCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask, runtime=_build_runtime(args)
    )
    run_clean(ctx)


def shallow():
    """Shallow clean preset configuration"""
    parser = ShallowCleanConfig.add_to_parser(
        ArgumentParser("Run WSClean on a measurement set with shallow-clean presets")
    )
    args = _parse(parser)
    setup_logging(args.log_level)
    ctx = WSCleanContext(
        cfg=ShallowCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask, runtime=_build_runtime(args)
    )
    run_clean(ctx)


def deep():
    """Deep clean preset configuration"""
    parser = DeepCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set with deep-clean presets"))
    args = _parse(parser)
    setup_logging(args.log_level)
    ctx = WSCleanContext(
        cfg=DeepCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask, runtime=_build_runtime(args)
    )
    run_clean(ctx)


def main():
    """Generic WSClean configuration"""
    parser = WSCleanConfig.add_to_parser(ArgumentParser("Run WSClean on a measurement set"))
    args = _parse(parser)
    setup_logging(args.log_level)
    ctx = WSCleanContext(
        cfg=WSCleanConfig.from_namespace(args), ms=args.ms, fits_mask=args.mask, runtime=_build_runtime(args)
    )
    run_clean(ctx)


if __name__ == "__main__":
    main()

"""
Converts a file to a .ms file
"""

from abc import ABC, abstractmethod
import argparse
import logging
from pathlib import Path
import shutil
from typing import Optional
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore", SyntaxWarning)
    from casatasks import importuvfits, importmiriad


logger = logging.getLogger(__name__)


class ToMsStrategy(ABC):
    @abstractmethod
    def convert(self, input_path: Path, output_ms: Optional[Path] = None) -> Path:
        pass


class UvFitsToMs(ToMsStrategy):
    def convert(self, input_path: Path, output_ms: Optional[Path] = None) -> Path:
        if not output_ms:
            output_ms = input_path.with_suffix(".ms")
        importuvfits(fitsfile=str(input_path), vis=str(output_ms))
        logger.info(f"Written: {output_ms}")
        return output_ms


class MiriadToMs(ToMsStrategy):
    def convert(self, input_path: Path, output_ms: Optional[Path] = None) -> Path:
        if not output_ms:
            output_ms = input_path.with_suffix(".ms")
        importmiriad(mirfile=str(input_path), vis=str(output_ms))
        logger.info(f"Written: {output_ms}")
        return output_ms


def convert_to_ms(input_path: Path, output_dir: Optional[Path] = None) -> Path:
    """Converts a valid observation file to a measurement set

    :param input_path: The observation file to convert
    :param output_dir: The directory to write the output to
    :return: The written measurement set path
    """
    match input_path.suffix:
        case ".uvfits":
            strategy = UvFitsToMs()
        case ".mir":
            strategy = MiriadToMs()
        case ".ms":
            if output_dir:  # Copy to the provided output directory
                output_ms = output_dir / input_path.name
                if not output_ms.exists():
                    logger.info(f"Copying {input_path} -> {output_ms}")
                    shutil.copytree(input_path, output_ms)
                else:
                    logger.info(f"MS already exists at {output_ms}, skipping copy")
                return output_ms
            logger.info("File is already a measurement set!")
            return input_path
        case _:
            raise Exception(f"Unsupported file type: {input_path.suffix}")
    output = None
    if output_dir:
        output = output_dir / input_path.with_suffix(".ms").name
    return strategy.convert(input_path, output)


def main():
    desc = """Converts a file to a .ms file. Accepts .uvfits and .mir file types.
    Will do nothing if already a measurement set."""
    parser = argparse.ArgumentParser(desc)
    parser.add_argument("input_file", type=Path, help="The path to the file to convert")
    parser.add_argument(
        "--output-dir",
        "--output_dir",
        dest="output_dir",
        type=Path,
        default=None,
        help="Directory to write the output MS to",
    )

    args = parser.parse_args()
    convert_to_ms(args.input_file, output_dir=args.output_dir)


if __name__ == "__main__":
    main()

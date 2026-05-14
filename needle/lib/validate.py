from pathlib import Path


def validate_path_ms(ms: Path) -> None:
    """Validate that Path is a measurement set

    :param ms: Path to the measurement set
    :raises TypeError: Raised if the supplied file is not a measurement set
    """
    if ms.suffix != ".ms":
        raise TypeError(f"Expected a measurement set, got {ms}")


def validate_path_fits(fits: Path) -> None:
    """Validate that Path is a fits file

    :param fits: Path to the measurement set
    :raises TypeError: Raised if the supplied file is not a fits file
    """
    if fits.suffix != ".fits":
        raise TypeError(f"Expected a fits file, got {fits}")

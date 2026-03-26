from pathlib import Path


def validate_path_ms(ms: Path) -> None:
    """Validate that Path is a measurement set"""
    if ms.suffix != ".ms":
        raise TypeError(f"Expected a measurement set, got {ms}")


def validate_path_fits(ms: Path) -> None:
    """Validate that Path is a fits file"""
    if ms.suffix != ".fits":
        raise TypeError(f"Expected a fits file, got {ms}")

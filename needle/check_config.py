"""
Validate a needle pipeline YAML config file.
Usage: python validate_config.py [config.yaml]
"""

import argparse
import sys
from pathlib import Path

from needle.config.pipeline import NeedleConfig


FIELDS = (
    "flow",
    "data",
    "watcher",
    "flag",
    "calibrate",
    "shallow_clean",
    "source_find",
    "create_mask",
    "deep_clean",
    "model_subtract",
    "interval_clean",
)


def _is_pydantic(val) -> bool:
    return hasattr(type(val), "model_fields")


def _print_section(val, indent: int = 2):
    pad = " " * indent
    if _is_pydantic(val):
        for field_name in type(val).model_fields:
            field_val = getattr(val, field_name)
            if _is_pydantic(field_val):
                print(f"{pad}[{field_name}: {type(field_val).__name__}]")
                _print_section(field_val, indent + 2)
            else:
                print(f"{pad}{field_name}: {field_val}")
    else:
        print(f"{pad}value: {val}")


def check_config(path: Path, verbose: bool = False):
    print(f"Validating config: {path}\n")
    cfg = NeedleConfig.from_yaml(path)
    print("Config loaded successfully!\n")
    for f in FIELDS:
        val = getattr(cfg, f)
        print(f"  {f}: {type(val).__name__}")
    print()
    print("All sections validated OK.")

    if verbose:
        print()
        for f in FIELDS:
            val = getattr(cfg, f)
            print(f"[{f}: {type(val).__name__}]")
            _print_section(val)
            print()


def main():
    parser = argparse.ArgumentParser(description="A simple script to validate a needle pipeline YAML config file.")
    cfg_default = Path.home() / Path(".needle.yaml")
    parser.add_argument(
        "--cfg",
        dest="cfg",
        default=cfg_default,
        help=f"Path to the config YAML file (default: {cfg_default})",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Whether to print the config verbosely",
    )
    args = parser.parse_args()
    path = Path(args.cfg)

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(1)
    try:
        check_config(path=path, verbose=args.verbose)
    except ValueError as e:
        print(f"CONFIG ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"VALIDATION ERROR: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

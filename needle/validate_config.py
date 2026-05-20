"""
Validate a needle pipeline YAML config file.
"""

import argparse
import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from needle.config.pipeline import NeedleConfig


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


def check_config(path: Path, pretty_print: bool = False):
    print(f"Validating config: {path}\n")

    print("\n--- Fields ---\n")

    # Load raw YAML so we can attempt per-section validation even if some fail
    raw = yaml.safe_load(path.read_text())

    errors = {}
    validated = {}

    config_fields = tuple(NeedleConfig.model_fields.keys())
    for f in config_fields:
        section_type = NeedleConfig.model_fields[f].annotation
        section_data = raw.get(f)
        try:
            validated[f] = section_type.model_validate(section_data or {})
        except ValidationError as e:
            errors[f] = e

    # Summary
    for f in config_fields:
        if f in validated:
            print(f"  ✓ {f}: {type(validated[f]).__name__}")
        elif f in errors:
            print(f"  ✗ {f}: FAILED")

    if errors:
        print(f"\n{len(errors)} section(s) failed validation:\n")
        for f, exc in errors.items():
            print(f"[{f}]")
            for err in exc.errors():
                loc = " -> ".join(str(i) for i in err["loc"])
                print(f"  {loc}: {err['msg']}")
            print()
    else:
        print("\nAll sections validated OK.")
        try:
            NeedleConfig.load(path)
            print("  ✓ Full config loaded successfully")
        except Exception as e:
            print(f"  ✗ Full config FAILED: {e}")

    if pretty_print and validated:
        print("\n--- Config ---\n")
        for f, val in validated.items():
            print(f"[{f}: {type(val).__name__}]")
            _print_section(val)
            print()


def main():
    parser = argparse.ArgumentParser(description="Validates a needle pipeline YAML config file.")
    cfg_default = Path.home() / Path(".needle.yaml")
    parser.add_argument(
        "--cfg",
        dest="cfg",
        default=cfg_default,
        help=f"Path to the config YAML file (default: {cfg_default})",
    )
    parser.add_argument(
        "-p",
        "--pretty_print",
        action="store_true",
        help="Whether to pretty print the config",
    )
    args = parser.parse_args()
    path = Path(args.cfg)

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(1)
    try:
        check_config(path=path, pretty_print=args.pretty_print)
    except ValueError as e:
        print(f"CONFIG ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"VALIDATION ERROR: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

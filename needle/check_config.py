#!/usr/bin/env python3
"""
Validate a needle pipeline YAML config file.
Usage: python validate_config.py [config.yaml]
"""

import argparse
import sys
from pathlib import Path

from needle.config.pipeline import PipelineConfig


def check_config(path):
    print(f"Validating config: {path}\n")

    cfg = PipelineConfig.from_yaml(path)
    print("Config loaded successfully!\n")
    print("--- Summary ---")
    print(f"  flow.log_level:    {cfg.flow.log_level}")
    print(f"  flow.max_workers:  {cfg.flow.max_workers}")
    print(f"  flow.local_data_dir: {cfg.flow.local_data_dir}")
    print(f"  flow.tgt_pattern:  {cfg.flow.tgt_pattern}")
    print(f"  flow.cal_pattern:  {cfg.flow.cal_pattern}")
    print(f"  flow.overwrite:    {cfg.flow.overwrite}")
    print()
    print("--- Sub-configs ---")
    for field in [
        "flag",
        "calibrate",
        "shallow_clean",
        "source_find",
        "create_mask",
        "deep_clean",
        "interval_clean",
    ]:
        val = getattr(cfg, field)
        print(f"  {field}: {type(val).__name__}")
    print()
    print("All sections validated OK.")


def main():
    parser = argparse.ArgumentParser(description="A simple script to validate a needle pipeline YAML config file.")
    parser.add_argument(
        "--cfg",
        dest="cfg",
        default="needle.yaml",
        help="Path to the config YAML file (default: needle.yaml)",
    )
    args = parser.parse_args()
    path = Path(args.cfg)

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(1)

    try:
        check_config(path)
    except ValueError as e:
        print(f"CONFIG ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"VALIDATION ERROR: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

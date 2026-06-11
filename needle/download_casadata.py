#!/usr/bin/env python3
"""
Pre-populates CASA measures data before running needle.
Uses the needle cluster config to determine if a container is in use,
and runs casaconfig.data_update inside the container if so.
Run this once before needle-serve or needle-run to ensure measures data is ready.
"""
import subprocess
import sys
from pathlib import Path


def main():
    from needle.config.cluster import ClusterConfig
    from needle.config.pipeline import NeedleConfig

    needle_cfg = NeedleConfig.get_config()
    casa_data_path = needle_cfg.data.staging_dir / "casadata"
    readme_path = casa_data_path / "readme.txt"

    if readme_path.exists():
        print(f"CASA measures data already present at {casa_data_path}, nothing to do.")
        return

    print(f"CASA measures data not found at {casa_data_path}, downloading...")

    cluster_cfg_path = Path.home() / ".needle_cluster.yaml"
    use_container = False
    container_args = []

    if cluster_cfg_path.exists():
        cluster_cfg = ClusterConfig.get_config()
        if cluster_cfg.container:
            use_container = True
            container_args = cluster_cfg.container.to_args()
            print(f"Container detected: {cluster_cfg.container.image}")
        else:
            print("No container configured, running directly.")
    else:
        print("No cluster config found, running directly.")

    python_cmd = ["python", "-c", f"import casaconfig; casaconfig.data_update(path='{casa_data_path}')"]

    if use_container:
        cmd = container_args + python_cmd
    else:
        cmd = python_cmd

    print(f"Running: {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd)

    if result.returncode != 0:
        print("ERROR: CASA data update failed.")
        sys.exit(result.returncode)

    if readme_path.exists():
        print("CASA measures data successfully populated.")
    else:
        print("WARNING: data_update completed but readme.txt still not found.")
        sys.exit(1)


if __name__ == "__main__":
    main()

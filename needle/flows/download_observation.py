"""
Download flow for newly deposited CASDA observations, and the helper that registers its Prefect deployment.

The flow is triggered by the `casda-query` flow via
``run_deployment("download-observation/download-observation", parameters={"target_sbid": ..., "calibrator_sbid": ...})``,
so the flow name, default deployment name and parameter names below must stay in sync with that script.

Credentials come from two Prefect Secret blocks, `casda-username` and `casda-password`. They are copied into the
environment of the run, which is where `needle.lib.casda_login` reads them from.

Configuration (environment variables, read at run time on the worker):
    CASDA_DOWNLOAD_DIR   Root directory for downloads (default: ./casda_downloads)
    CASDA_PRODUCT_TYPE   obscore dataproduct_type to fetch (default: visibility)
"""

import asyncio
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from astroquery.casda import Casda  # noqa: F401  (type of casda_login())
from astroquery.utils.tap.core import TapPlus
from distributed import Client
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from prefect import flow, get_run_logger, task

from needle.config.pipeline import NeedleConfig
from needle.lib.casda import casda_login, CASDA_TAP_URL
from needle.lib.dask import build_dask_client

DEFAULT_DEPLOYMENT_NAME = "download-observation-ozstar"
PARTIAL_DIR_NAME = ".partial"

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _download_root() -> Path:
    return Path(os.environ.get("CASDA_DOWNLOAD_DIR", "casda_downloads")).expanduser().resolve()


def _product_query(obs_id: str) -> str:
    if not _SAFE_ID.match(obs_id):
        raise ValueError(f"Refusing to query with unexpected obs_id {obs_id!r}")
    product_type = os.environ.get("CASDA_PRODUCT_TYPE", "visibility")
    if not _SAFE_ID.match(product_type):
        raise ValueError(f"Unexpected CASDA_PRODUCT_TYPE {product_type!r}")
    return f"""
        SELECT
            obs_publisher_did,
            dataproduct_subtype,
            filename,
            access_url,
            access_estsize
        FROM ivoa.obscore
        WHERE obs_id = '{obs_id}'
            AND dataproduct_type = '{product_type}'
        ORDER BY filename
    """


def _casda_client() -> Casda:
    """Load the credentials from Prefect blocks and log in using the existing needle helper."""
    os.environ["CASDA_USERNAME"] = Secret.load("casda-username").get()
    os.environ["CASDA_PASSWORD"] = Secret.load("casda-password").get()
    return casda_login()


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=300, timeout_seconds=6 * 3600)
def download_products(client: Client, obs_id: str, sbid: int | str | None = None) -> str:
    """Stage and download an observation's data products. Safe to re-run: completed downloads are skipped.

    Files land in a scratch directory first and are moved into place only once everything has arrived, with a marker
    file written last, so an interrupted run never leaves something that looks complete.
    """
    logger = get_run_logger()
    dest = _download_root() / obs_id
    marker = dest / ".download_complete.json"

    if marker.exists():
        logger.info("%s already downloaded to %s; skipping.", obs_id, dest)
        return str(dest)

    table = TapPlus(url=CASDA_TAP_URL).launch_job(_product_query(obs_id)).get_results()
    if len(table) == 0:
        raise ValueError(f"No downloadable products found in CASDA for {obs_id}.")
    logger.info("Found %d product(s) for %s; staging.", len(table), obs_id)

    casda = _casda_client()
    urls = casda.stage_data(table)  # blocks until CASDA has staged the files
    logger.info("Staged %d file(s) for %s; downloading.", len(urls), obs_id)

    # A unique scratch directory per run, because two targets can share a calibrator and download it concurrently.
    partial = dest / f"{PARTIAL_DIR_NAME}-{uuid.uuid4().hex[:8]}"
    partial.mkdir(parents=True, exist_ok=True)
    try:
        downloaded = [Path(p) for p in casda.download_files(urls, savedir=str(partial))]
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise

    if marker.exists():  # another run finished this observation while we were downloading
        shutil.rmtree(partial, ignore_errors=True)
        return str(dest)

    for path in downloaded:
        target = dest / path.name
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
        shutil.move(str(path), str(target))
    shutil.rmtree(partial, ignore_errors=True)

    marker.write_text(
        json.dumps(
            {
                "obs_id": obs_id,
                "sbid": sbid,
                "files": sorted(p.name for p in downloaded),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )
    logger.info("Downloaded %d file(s) for %s to %s.", len(downloaded), obs_id, dest)
    return str(dest)


def _obs_id(sbid: int) -> str:
    """CASDA observation IDs are of the form ASKAP-<SBID>."""
    return f"ASKAP-{int(sbid)}"


@flow(name="download-observation", log_prints=True)
def download_observation(target_sbid: int, calibrator_sbid: int, cfg_path: Path) -> dict[str, str]:
    """Download the visibilities of a target observation and its calibrator from CASDA."""
    logger = get_run_logger()
    logger.info("Downloading target SBID %s with calibrator SBID %s.", target_sbid, calibrator_sbid)

    cfg = NeedleConfig.from_yaml(cfg_path)
    with build_dask_client(type="data_mover") as (client, _):
        target = download_products.submit(
            client=client, obs_id=_obs_id(target_sbid), sb_id=target_sbid, download_dir=cfg.download_dir
        )
        calibrator = download_products.submit(
            client=client, obs_id=_obs_id(calibrator_sbid), sbid=calibrator_sbid, download_dir=cfg.download_dir
        )

    # Rename and move the downloaded files to the cfg.work_dir/tgt_sbid directory

    return {"target_dir": target.result(), "calibrator_dir": calibrator.result()}


################
## Deployment ##
################


async def _ensure_work_pool(name: str, pool_type: str) -> None:
    """Create the work pool if it does not exist yet."""
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound

    async with get_client() as client:
        try:
            await client.read_work_pool(name)
            return
        except ObjectNotFound:
            pass
        try:
            await client.create_work_pool(WorkPoolCreate(name=name, type=pool_type))
        except ObjectAlreadyExists:  # lost a race with another caller; that's fine
            pass


def deploy_download_observation(
    work_pool_name: str,
    deployment_name: str = DEFAULT_DEPLOYMENT_NAME,
    pool_type: str = "process",
    job_variables: dict[str, Any] | None = None,
    concurrency_limit: int | None = None,
    tags: list[str] | None = None,
):
    """Create or update the download deployment on a work pool.

    Idempotent: the work pool is only created when missing, and Prefect upserts deployments by
    (flow name, deployment name), so running this repeatedly updates the same deployment in place.
    """
    asyncio.run(_ensure_work_pool(work_pool_name, pool_type))

    # The worker imports the flow from source, so point it at the project root containing the `needle` package.
    this_file = Path(__file__).resolve()
    root = this_file.parents[2]
    entrypoint = f"{this_file.relative_to(root).as_posix()}:download_observation"

    remote_flow = download_observation.from_source(source=str(root), entrypoint=entrypoint)
    return remote_flow.deploy(
        name=deployment_name,
        work_pool_name=work_pool_name,
        job_variables=job_variables or {},
        concurrency_limit=concurrency_limit,
        tags=tags or ["casda"],
        build=False,
        push=False,
        print_next_steps=False,
    )

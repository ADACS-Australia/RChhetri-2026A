"""
Download flow for newly deposited CASDA observations, and the helper that registers its Prefect deployment.

The flow is triggered by the `casda-query` flow via
``run_deployment("download-observation/download-observation-ozstar", parameters={"target_sbid": ..., "calibrator_sbid": ...})``,
so the flow name and parameter names below must stay in sync with that script. The deployment itself is defined in
the repository's `prefect.yaml`.

Credentials come from two Prefect Secret blocks, `casda-username` and `casda-password`. They are copied into the
environment of the run, which is where `needle.lib.casda_login` reads them from.

Downloads are staged on the login node, then downloaded concurrently by local worker threads, one per visibility.
Target observations only have their `.uvfits.tar` products downloaded; calibrators use their `.ms.tar` products. Each
visibility is extracted in its own scratch directory and installed into ``cfg.data.work_dir/SB<target_sbid>`` as
either ``SB<target_sbid>_beamXX.uvfits`` or ``cal_SB<calibrator_sbid>_beamXX.ms``. Once the downloads validate, the
flow triggers the ``needle-pipeline/needle-pipeline-ozstar`` deployment on the ``process-observation-ozstar`` work
pool.
"""

import os
import re
import shutil
import tarfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import requests
from astroquery.casda import Casda  # noqa: F401  (type of casda_login())
from astroquery.utils.tap.core import TapPlus
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from prefect.deployments import run_deployment
from prefect.runtime import flow_run

from needle.config.cluster import ClusterConfig
from needle.config.pipeline import NeedleConfig
from needle.lib.casda import casda_login, CASDA_TAP_URL

EXPECTED_VISIBILITIES = 36
MAX_CONCURRENT_DOWNLOADS = 8  # Number of visibility downloads to run concurrently on the local node.
PIPELINE_DEPLOYMENT = os.environ.get(
    "NEEDLE_PIPELINE_DEPLOYMENT",
    "needle-pipeline/needle-pipeline-ozstar",
)
TARGET_PRODUCT_SUFFIX = ".uvfits.tar"
CALIBRATOR_PRODUCT_SUFFIX = ".ms.tar"

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.\-]+$")
_BEAM = re.compile(r"beam(?P<beam>\d{2,3})", re.IGNORECASE)


def _obs_id(sbid: int) -> str:
    """CASDA observation IDs are of the form ASKAP-<SBID>."""
    return f"ASKAP-{int(sbid)}"


def _product_query(obs_id: str, kind: str) -> str:
    if not _SAFE_ID.match(obs_id):
        raise ValueError(f"Refusing to query with unexpected obs_id {obs_id!r}")

    if kind == "target":
        suffix = TARGET_PRODUCT_SUFFIX
    elif kind == "cal":
        suffix = CALIBRATOR_PRODUCT_SUFFIX
    else:
        raise ValueError(f"Unknown visibility kind {kind!r}")

    return f"""
        SELECT
            obs_publisher_did,
            filename,
            access_url,
            access_estsize
        FROM ivoa.obscore
        WHERE obs_id = '{obs_id}'
            AND dataproduct_type = 'visibility'
            AND filename LIKE '%{suffix}'
        ORDER BY filename
    """


def _casda_client() -> Casda:
    """Load the credentials from Prefect blocks and log in using the existing needle helper."""
    os.environ["CASDA_USERNAME"] = Secret.load("casda-username").get()
    os.environ["CASDA_PASSWORD"] = Secret.load("casda-password").get()
    return casda_login()


def _beam_from_filename(filename: str) -> int:
    match = _BEAM.search(filename)
    if match is None:
        raise ValueError(f"Could not parse a beam number from {filename!r}")
    return int(match.group("beam"))


def _validate_products(table, kind: str) -> None:
    """Require exactly one visibility per beam, 0..35."""
    if len(table) != EXPECTED_VISIBILITIES:
        raise ValueError(f"Expected {EXPECTED_VISIBILITIES} {kind} visibilities, found {len(table)}")

    beams = [_beam_from_filename(str(row["filename"])) for row in table]
    if sorted(beams) != list(range(EXPECTED_VISIBILITIES)):
        raise ValueError(f"Expected {kind} beams 00-{EXPECTED_VISIBILITIES - 1:02d}, found {sorted(beams)}")


def _is_checksum_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/").lower()
    return path.endswith("checksum") or path.endswith(".checksum")


def _staged_urls(casda: Casda, table) -> list[str]:
    """Stage a table's products and return one data URL per row, in row order."""
    urls = [url for url in casda.stage_data(table) if not _is_checksum_url(url)]
    if len(urls) != len(table):
        raise ValueError(f"Expected {len(table)} staged URLs, got {len(urls)} after removing checksums")

    # CASDA normally keeps the original filename at the end of the staged URL. Use
    # it when possible so a differently ordered result set cannot mis-pair files.
    by_name: dict[str, str] = {}
    for url in urls:
        name = unquote(urlparse(url).path).rsplit("/", 1)[-1]
        by_name.setdefault(name, url)

    ordered = [by_name.get(str(row["filename"])) for row in table]
    if all(ordered):
        return ordered  # type: ignore[return-value]

    # Fall back to the order returned by the staging service.
    return urls


def _final_name(kind: str, sbid: int, beam: int) -> str:
    if kind == "target":
        return f"SB{int(sbid)}_beam{beam:02d}.uvfits"
    return f"cal_SB{int(sbid)}_beam{beam:02d}.ms"


def _installed_beams(paths: list[Path]) -> list[int]:
    return sorted(_beam_from_filename(path.name) for path in paths)


def _append_log(log_path: Path, message: str) -> None:
    """Append a timestamped line to the shared download log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp} {message}\n")


def _is_complete(path: Path, kind: str) -> bool:
    if kind == "target":
        return path.is_file() and path.stat().st_size > 0
    # A CASA MeasurementSet always has a table.dat; use it as the completeness marker.
    return path.is_dir() and (path / "table.dat").is_file()


def _download(url: str, destination: Path) -> None:
    """Download one staged URL to `destination`, retrying transient failures."""
    if destination.is_file() and destination.stat().st_size > 0:
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(f"{destination.name}.part")

    # CASDA's staged URLs are commonly signed S3 URLs. Keep the signature intact.
    encoded_url = url.replace("+", "%2B").replace(" ", "%20").replace('"', "%22")

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with requests.get(
                encoded_url,
                stream=True,
                timeout=(120, 60 * 60),
            ) as response:
                response.raise_for_status()
                with part.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            part.replace(destination)
            return
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            part.unlink(missing_ok=True)
            if attempt < 3:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Failed to download {url!r}") from last_error


def _install(extracted: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        if final.is_dir():
            shutil.rmtree(final)
        else:
            final.unlink()
    shutil.move(str(extracted), str(final))


def _extract_target(tar_path: Path, scratch: Path) -> Path:
    with tarfile.open(tar_path, mode="r:*") as tar:
        members = [member for member in tar.getmembers() if member.isfile() and member.name.lower().endswith(".uvfits")]
        if len(members) != 1:
            raise ValueError(f"Expected exactly one .uvfits member in {tar_path.name}, found {len(members)}")
        member = members[0]
        tar.extract(member, scratch, filter="data")
        return scratch / member.name


def _extract_calibrator(tar_path: Path, scratch: Path) -> Path:
    with tarfile.open(tar_path, mode="r:*") as tar:
        members = tar.getmembers()

        ms_roots: set[Path] = set()
        for member in members:
            parts = Path(member.name).parts
            for index, part in enumerate(parts):
                if part.lower().endswith(".ms"):
                    ms_roots.add(Path(*parts[: index + 1]))
                    break

        if len(ms_roots) != 1:
            raise ValueError(f"Expected exactly one .ms directory in {tar_path.name}, found {len(ms_roots)}")

        ms_root = next(iter(ms_roots))
        to_extract = [
            member for member in members if Path(member.name) == ms_root or ms_root in Path(member.name).parents
        ]
        tar.extractall(scratch, members=to_extract, filter="data")
        return scratch / ms_root


def download_and_install(
    url: str,
    final_path: str,
    scratch_root: str,
    kind: str,
    sbid: int,
    beam: int,
    filename: str,
) -> str:
    """Download one visibility, extract it, and install it in the target directory."""
    final = Path(final_path)
    log_path = final.parent / "download.log"
    label = f"{kind} SB{sbid} beam{beam:02d}"

    if _is_complete(final, kind):
        _append_log(log_path, f"{label}: already complete at {final}; skipping")
        return str(final)

    scratch = Path(scratch_root) / f"{kind}-SB{sbid}-beam{beam:02d}-{uuid.uuid4().hex[:8]}"
    scratch.mkdir(parents=True, exist_ok=False)
    _append_log(log_path, f"{label}: starting download of {filename}")

    try:
        tar_path = scratch / Path(filename).name
        _download(url, tar_path)
        _append_log(log_path, f"{label}: downloaded {tar_path.stat().st_size} bytes")

        if kind == "target":
            extracted = _extract_target(tar_path, scratch)
        elif kind == "cal":
            extracted = _extract_calibrator(tar_path, scratch)
        else:
            raise ValueError(f"Unknown visibility kind {kind!r}")

        _append_log(log_path, f"{label}: extracted {extracted.name}")
        _install(extracted, final)
        _append_log(log_path, f"{label}: installed {final}")
        return str(final)
    except Exception as exc:
        _append_log(log_path, f"{label}: FAILED: {type(exc).__name__}: {exc}")
        raise
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=300, timeout_seconds=6 * 3600)
def stage_downloads(
    target_sbid: int,
    calibrator_sbid: int,
    target_dir: Path,
    scratch_root: Path,
) -> list[dict[str, Any]]:
    """Query and stage both observations, returning one download item per visibility."""
    logger = get_run_logger()
    log_path = target_dir / "download.log"
    casda = _casda_client()
    items: list[dict[str, Any]] = []

    try:
        for sbid, kind in ((target_sbid, "target"), (calibrator_sbid, "cal")):
            obs_id = _obs_id(sbid)
            table = TapPlus(url=CASDA_TAP_URL).launch_job(_product_query(obs_id, kind)).get_results()
            _validate_products(table, kind)
            logger.info(f"Found {len(table)} {kind} visibilities for {obs_id}; staging.")
            _append_log(log_path, f"staging {kind} SBID {sbid}: found {len(table)} visibilities")

            urls = _staged_urls(casda, table)
            _append_log(log_path, f"staging {kind} SBID {sbid}: staged {len(urls)} files")

            for row, url in zip(table, urls):
                beam = _beam_from_filename(str(row["filename"]))
                items.append(
                    {
                        "url": url,
                        "filename": str(row["filename"]),
                        "kind": kind,
                        "sbid": int(sbid),
                        "beam": beam,
                        "final_path": str(target_dir / _final_name(kind, int(sbid), beam)),
                        "scratch_root": str(scratch_root),
                    }
                )
    except Exception as exc:
        _append_log(log_path, f"staging FAILED: {type(exc).__name__}: {exc}")
        raise

    expected = 2 * EXPECTED_VISIBILITIES
    if len(items) != expected:
        message = f"expected {expected} download items, built {len(items)}"
        _append_log(log_path, f"staging FAILED: {message}")
        raise ValueError(message)
    logger.info(f"Staged {len(items)} files; starting local downloads.")
    _append_log(log_path, f"staged {len(items)} files; starting local downloads")
    return items


@task(retries=2, retry_delay_seconds=60)
def trigger_pipeline(
    target_sbid: int,
    target_dir: Path,
    cfg: NeedleConfig,
    cluster_cfg: ClusterConfig,
    log_path: Path,
) -> str:
    """Trigger the pipeline deployment for a completed download."""
    logger = get_run_logger()
    _append_log(log_path, f"triggering pipeline {PIPELINE_DEPLOYMENT} for {target_dir}")

    try:
        flow_run = run_deployment(
            PIPELINE_DEPLOYMENT,
            parameters={
                "cfg": cfg.model_dump(mode="json"),
                "cluster_cfg": cluster_cfg.model_dump(mode="json"),
                "work_dir": str(target_dir),
            },
            flow_run_name=f"pipeline-SB{target_sbid}",
            idempotency_key=f"pipeline-SB{target_sbid}",
            timeout=0,
            as_subflow=False,
        )
    except Exception as exc:
        _append_log(log_path, f"pipeline trigger FAILED: {type(exc).__name__}: {exc}")
        raise

    logger.info(f"Triggered {PIPELINE_DEPLOYMENT} (flow run {flow_run.id}).")
    _append_log(log_path, f"triggered pipeline {PIPELINE_DEPLOYMENT} (flow run {flow_run.id})")
    return str(flow_run.id)


@flow(
    name="download-observation",
    flow_run_name=lambda: f"download-SB{flow_run.get_parameters()['target_sbid']}",
    log_prints=True,
)
def download_observation(
    target_sbid: int,
    calibrator_sbid: int,
    cfg_path: Path | None = None,
) -> dict[str, Any]:
    """Download the visibilities of a target observation and its calibrator from CASDA."""
    logger = get_run_logger()
    cfg = NeedleConfig.from_yaml(cfg_path) if cfg_path else NeedleConfig.get_config()

    work_root = Path(cfg.data.work_dir).expanduser().resolve()
    target_dir = work_root / f"SB{int(target_sbid)}"
    scratch_root = Path(cfg.data.download_dir).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    scratch_root.mkdir(parents=True, exist_ok=True)
    log_path = target_dir / "download.log"
    _append_log(log_path, f"flow started: target SBID {target_sbid}, calibrator SBID {calibrator_sbid}")
    _append_log(log_path, f"work directory: {target_dir}; scratch directory: {scratch_root}")

    logger.info(f"Downloading target SBID {target_sbid} with calibrator SBID {calibrator_sbid}.")
    try:
        items = stage_downloads(target_sbid, calibrator_sbid, target_dir, scratch_root)
    except Exception as exc:
        _append_log(log_path, f"staging FAILED: {type(exc).__name__}: {exc}")
        raise

    max_workers = min(MAX_CONCURRENT_DOWNLOADS, len(items))
    _append_log(log_path, f"downloading {len(items)} visibilities with up to {max_workers} local workers")
    try:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="download") as executor:
            futures = [executor.submit(download_and_install, **item) for item in items]
            for future in as_completed(futures):
                future.result()
    except Exception as exc:
        _append_log(log_path, f"download FAILED: {type(exc).__name__}: {exc}")
        raise

    _append_log(log_path, "all download jobs finished")

    target_files = sorted(target_dir.glob(f"SB{target_sbid}_beam*.uvfits"))
    calibrator_dirs = sorted(path for path in target_dir.glob(f"cal_SB{calibrator_sbid}_beam*.ms") if path.is_dir())

    expected_beams = list(range(EXPECTED_VISIBILITIES))
    if len(target_files) != EXPECTED_VISIBILITIES:
        message = f"expected {EXPECTED_VISIBILITIES} target visibilities, found {len(target_files)}"
        _append_log(log_path, f"validation warning: {message}")
        logger.warning(f"{message} in {target_dir}")
    if len(calibrator_dirs) != EXPECTED_VISIBILITIES:
        message = f"expected {EXPECTED_VISIBILITIES} calibrator visibilities, found {len(calibrator_dirs)}"
        _append_log(log_path, f"validation warning: {message}")
        logger.warning(f"{message} in {target_dir}")
    if _installed_beams(target_files) != expected_beams:
        message = f"target beams are not exactly 00-{EXPECTED_VISIBILITIES - 1:02d}"
        _append_log(log_path, f"validation FAILED: {message}")
        raise RuntimeError(message)
    if _installed_beams(calibrator_dirs) != expected_beams:
        message = f"calibrator beams are not exactly 00-{EXPECTED_VISIBILITIES - 1:02d}"
        _append_log(log_path, f"validation FAILED: {message}")
        raise RuntimeError(message)
    if any(path.stat().st_size == 0 for path in target_files):
        message = "one or more target .uvfits files are empty"
        _append_log(log_path, f"validation FAILED: {message}")
        raise RuntimeError(message)
    if any(not (path / "table.dat").is_file() for path in calibrator_dirs):
        message = "one or more calibrator .ms directories are incomplete"
        _append_log(log_path, f"validation FAILED: {message}")
        raise RuntimeError(message)

    _append_log(
        log_path,
        f"validation OK: {len(target_files)} target visibilities, {len(calibrator_dirs)} calibrator visibilities",
    )
    try:
        cluster_cfg = ClusterConfig.get_cluster_config()
        trigger_pipeline(target_sbid, target_dir, cfg, cluster_cfg, log_path)
    except Exception as exc:
        _append_log(log_path, f"pipeline trigger FAILED: {type(exc).__name__}: {exc}")
        raise

    _append_log(log_path, "flow completed successfully")
    logger.info(
        f"Installed {len(target_files)} target visibilities and {len(calibrator_dirs)} calibrator visibilities in "
        f"{target_dir}."
    )
    return {
        "target_dir": str(target_dir),
        "target_sbid": int(target_sbid),
        "calibrator_sbid": int(calibrator_sbid),
        "target_count": len(target_files),
        "calibrator_count": len(calibrator_dirs),
    }

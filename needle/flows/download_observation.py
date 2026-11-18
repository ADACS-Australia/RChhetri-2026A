# flows/download.py
from pathlib import Path

import pyvo
from astroquery.casda import Casda
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from needle.lib.casda import casda_login

TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
BATCH = 5


def sbid_of(obs_id: str) -> int:
    return int(obs_id.split("-")[1])  # "ASKAP-10108" -> 10108


@task
def find_calibrator(obs_id: str, window_days: float = 1.0) -> str | None:
    """Closest-in-time 1934-638 observation to this science observation."""
    tap = pyvo.dal.TAPService(TAP_URL)
    sbid = sbid_of(obs_id)

    obs = tap.search(f"""
        SELECT obs_start_mjd, obs_end_mjd FROM casda.observation WHERE sbid = {sbid}
    """).to_table()
    if len(obs) == 0:
        return None
    t0, t1 = float(obs["obs_start_mjd"][0]), float(obs["obs_end_mjd"][0])

    cands = tap.search(f"""
        SELECT sbid FROM casda.observation
        WHERE obs_start_mjd BETWEEN {t0 - window_days} AND {t1 + window_days}
          AND sbid <> {sbid}
    """).to_table()
    if len(cands) == 0:
        return None

    ids = ", ".join(f"'ASKAP-{int(s)}'" for s in cands["sbid"])
    cal = tap.search(f"""
        SELECT obs_id, MIN(t_min) AS t_start
        FROM ivoa.obscore
        WHERE obs_id IN ({ids}) AND target_name LIKE '%1934%'
        GROUP BY obs_id
    """).to_table()
    if len(cal) == 0:
        return None

    best = min(range(len(cal)), key=lambda i: abs(float(cal["t_start"][i]) - t0))
    return str(cal["obs_id"][best])


@task
def list_products(obs_id: str, subtypes: list[str] | None = None):
    tap = pyvo.dal.TAPService(TAP_URL)
    where = f"obs_id = '{obs_id}' AND access_url IS NOT NULL"
    if subtypes:
        quoted = ", ".join(f"'{s}'" for s in subtypes)
        where += f" AND dataproduct_subtype IN ({quoted})"
    return tap.search(f"""
        SELECT obs_id, dataproduct_type, dataproduct_subtype, filename,
               access_estsize, access_url
        FROM ivoa.obscore WHERE {where}
    """).to_table()


@task(retries=3, retry_delay_seconds=120)
def download_products(products, outdir: str):
    logger = get_run_logger()
    out = Path(outdir)
    marker = out / ".done"
    if marker.exists():
        logger.info(f"{out} already complete, skipping")
        return
    out.mkdir(parents=True, exist_ok=True)

    casda = casda_login(username=Secret.load("casda-username").get(), password=Secret.load("casda-password").get())

    for i in range(0, len(products), BATCH):
        urls = casda.stage_data(products[i : i + BATCH])
        casda.download_files(urls, savedir=str(out))
        logger.info(f"{min(i + BATCH, len(products))}/{len(products)} -> {out}")
    marker.touch()


@flow(log_prints=True)
def download_observation(
    obs_id: str,  # e.g. "ASKAP-10108"
    savedir: str = "/Downloads",
    science_subtypes: list[str] | None = None,  # None = all products
    require_calibrator: bool = True,
    dry_run: bool = False,
):
    logger = get_run_logger()
    base = Path(savedir)

    cal_id = find_calibrator(obs_id)
    if cal_id is None:
        msg = f"no calibrator found for {obs_id}"
        if require_calibrator:
            raise RuntimeError(msg)
        logger.warning(msg)
    else:
        logger.info(f"calibrator for {obs_id}: {cal_id}")

    jobs = {obs_id: base / obs_id / "science"}
    if cal_id:
        jobs[cal_id] = base / "calibrators" / cal_id  # shared across science obs

    for oid, outdir in jobs.items():
        subtypes = science_subtypes if oid == obs_id else None
        products = list_products(oid, subtypes)
        gb = float(sum(products["access_estsize"])) / 1e6  # estsize is in kB
        logger.info(f"{oid}: {len(products)} products, ~{gb:.1f} GB")
        if not dry_run:
            download_products(products, str(outdir))

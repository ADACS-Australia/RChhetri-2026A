#!/usr/bin/env python3
"""
ms_inspect.py
-------------
Inspect a Measurement Set and return structured metadata.
"""

import sys
import logging
from pathlib import Path
import argparse
import numpy as np
from dataclasses import dataclass, asdict
from typing import Optional
import json

try:
    from casatools import table, quanta
except ImportError:
    sys.exit("Error: casatools not found. Install with: pip install casatools")

logger = logging.getLogger(__name__)


@dataclass
class TimeInfo:
    start_utc: str
    end_utc: str
    start_mjd_s: float
    end_mjd_s: float
    integration_time_s: float
    n_integrations: int
    total_duration_s: float
    total_duration_min: float


@dataclass
class FrequencyInfo:
    n_spw: int
    spw_centre_hz: list[float]
    spw_width_hz: list[float]
    spw_n_channels: list[int]
    spw_freq_min_hz: list[float]
    spw_freq_max_hz: list[float]
    centre_wavelength_m: Optional[float]


@dataclass
class BaselineInfo:
    n_antennas: int
    antenna_names: list[str]
    antenna_positions_m: list[list[float]]
    n_baselines: int
    uv_min_lambda: Optional[float]
    uv_max_lambda: Optional[float]
    uv_min_m: Optional[float]
    uv_max_m: Optional[float]


@dataclass
class PolarisationInfo:
    polarisations: list[str]
    n_pols: int


@dataclass
class FieldInfo:
    n_fields: int
    field_names: list[str]
    phase_centres_deg: list[dict]


@dataclass
class MSInspectResult:
    ms: Path
    time: TimeInfo
    frequency: FrequencyInfo
    baselines: BaselineInfo
    polarisation: PolarisationInfo
    fields: FieldInfo
    data_columns: dict[str, tuple]

    def to_json(self, output_dir: Optional[Path] = None) -> Path:
        """Write the inspection result to a JSON file.

        The output filename is inferred from the MS path e.g.
        /data/my_obs.ms -> /data/my_obs_inspect.json

        :param output_dir: Optional directory to write to. Defaults to the MS directory.
        :returns: Path to the written JSON file
        """
        out_dir = output_dir or self.ms.parent
        output_path = out_dir / f"{self.ms.stem}_inspect.json"
        logger.info(f"Writing inspection result to {output_path}")
        with open(output_path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)
        return output_path

    @classmethod
    def from_json(cls, path: Path) -> "MSInspectResult":
        """Load an inspection result from a JSON file.

        :param path: Path to the JSON file
        :raises FileNotFoundError: If the JSON file does not exist
        :returns: MSInspectResult instance
        """
        if not path.exists():
            raise FileNotFoundError(f"Inspection JSON not found: {path}")
        logger.info(f"Loading inspection result from {path}")
        with open(path) as f:
            data = json.load(f)

        return cls(
            ms=Path(data["ms"]),
            time=TimeInfo(**data["time"]),
            frequency=FrequencyInfo(**data["frequency"]),
            baselines=BaselineInfo(**data["baselines"]),
            polarisation=PolarisationInfo(**data["polarisation"]),
            fields=FieldInfo(**data["fields"]),
        )


STOKES_MAP = {
    1: "I",
    2: "Q",
    3: "U",
    4: "V",
    5: "RR",
    6: "RL",
    7: "LR",
    8: "LL",
    9: "XX",
    10: "XY",
    11: "YX",
    12: "YY",
}


def mjd_s_to_utc(mjd_s: float) -> str:
    qa = quanta()
    return qa.time(qa.quantity(mjd_s, "s"), form="ymd")[0]


def rad_to_deg(rad: float) -> float:
    return float(np.degrees(rad))


def get_time_info(ms: Path) -> TimeInfo:
    logger.debug(f"Reading time info from {ms}")
    tb = table()
    tb.open(str(ms))
    times = np.unique(tb.getcol("TIME"))
    tb.close()

    if len(times) < 2:
        raise ValueError(f"MS {ms} has fewer than 2 unique timestamps — cannot determine integration time")

    int_time = float(times[1] - times[0])
    duration = float(times[-1] - times[0]) + int_time

    result = TimeInfo(
        start_utc=mjd_s_to_utc(times[0]),
        end_utc=mjd_s_to_utc(times[-1]),
        start_mjd_s=float(times[0]),
        end_mjd_s=float(times[-1]),
        integration_time_s=int_time,
        n_integrations=len(times),
        total_duration_s=duration,
        total_duration_min=round(duration / 60, 4),
    )
    logger.debug(f"Time info: {result}")
    return result


def get_frequency_info(ms: Path) -> FrequencyInfo:
    logger.debug(f"Reading frequency info from {ms}")
    tb = table()
    tb.open(str(ms / "SPECTRAL_WINDOW"))
    n_spw = tb.nrows()
    chan_freqs = [tb.getcell("CHAN_FREQ", i) for i in range(n_spw)]
    chan_width = [tb.getcell("CHAN_WIDTH", i) for i in range(n_spw)]
    tb.close()

    centres, widths, nchan, fmin, fmax = [], [], [], [], []
    for i, freqs in enumerate(chan_freqs):
        centres.append(float(np.mean(freqs)))
        widths.append(float(np.mean(np.abs(chan_width[i]))))
        nchan.append(len(freqs))
        fmin.append(float(freqs.min()))
        fmax.append(float(freqs.max()))

    c = 2.998e8
    lam = c / centres[0] if centres else None

    result = FrequencyInfo(
        n_spw=n_spw,
        spw_centre_hz=centres,
        spw_width_hz=widths,
        spw_n_channels=nchan,
        spw_freq_min_hz=fmin,
        spw_freq_max_hz=fmax,
        centre_wavelength_m=round(lam, 6) if lam else None,
    )
    logger.debug(f"Frequency info: {result}")
    return result


def get_baseline_info(ms: Path, freq_info: FrequencyInfo) -> BaselineInfo:
    logger.debug(f"Reading baseline info from {ms}")
    tb = table()

    tb.open(str(ms / "ANTENNA"))
    names = list(tb.getcol("NAME"))
    positions = tb.getcol("POSITION").T.tolist()  # shape (n_ant, 3)
    tb.close()

    n_ant = len(names)
    n_baselines = n_ant * (n_ant - 1) // 2

    tb.open(str(ms))
    uvw = tb.getcol("UVW")  # shape (3, n_rows)
    tb.close()

    uv_dist_m = np.sqrt(uvw[0] ** 2 + uvw[1] ** 2)
    uv_dist_m = uv_dist_m[uv_dist_m > 0]  # Exclude zero-length autocorrelations

    lam = freq_info.centre_wavelength_m
    if lam and len(uv_dist_m):
        uv_min_lam = round(float(uv_dist_m.min()) / lam / 1e3, 4)  # kλ
        uv_max_lam = round(float(uv_dist_m.max()) / lam / 1e3, 4)
        uv_min_m = round(float(uv_dist_m.min()), 2)
        uv_max_m = round(float(uv_dist_m.max()), 2)
    else:
        logger.warning("Could not compute UV range — missing wavelength or UV data")
        uv_min_lam = uv_max_lam = uv_min_m = uv_max_m = None

    result = BaselineInfo(
        n_antennas=n_ant,
        antenna_names=names,
        antenna_positions_m=positions,
        n_baselines=n_baselines,
        uv_min_lambda=uv_min_lam,
        uv_max_lambda=uv_max_lam,
        uv_min_m=uv_min_m,
        uv_max_m=uv_max_m,
    )
    logger.debug(f"Baseline info: {result}")
    return result


def get_polarisation_info(ms: Path) -> PolarisationInfo:
    logger.debug(f"Reading polarisation info from {ms}")
    tb = table()
    tb.open(str(ms / "POLARIZATION"))
    corr_types = tb.getcell("CORR_TYPE", 0)
    tb.close()

    pols = [STOKES_MAP.get(int(c), f"Unknown({c})") for c in corr_types]
    result = PolarisationInfo(polarisations=pols, n_pols=len(pols))
    logger.debug(f"Polarisation info: {result}")
    return result


def get_field_info(ms: Path) -> FieldInfo:
    logger.debug(f"Reading field info from {ms}")
    tb = table()
    tb.open(str(ms / "FIELD"))
    names = list(tb.getcol("NAME"))
    phase_dir = tb.getcol("PHASE_DIR")  # shape (2, 1, n_fields)
    tb.close()

    centres = []
    for i in range(len(names)):
        ra_rad = phase_dir[0, 0, i]
        dec_rad = phase_dir[1, 0, i]
        centres.append(
            {
                "ra_deg": round(rad_to_deg(ra_rad) % 360, 6),
                "dec_deg": round(rad_to_deg(dec_rad), 6),
            }
        )

    result = FieldInfo(
        n_fields=len(names),
        field_names=names,
        phase_centres_deg=centres,
    )
    logger.debug(f"Field info: {result}")
    return result


def inspect_ms(ms: Path) -> MSInspectResult:
    """Inspect a Measurement Set and return structured metadata.

    :param ms: Path to the measurement set
    :raises FileNotFoundError: If the MS does not exist
    :raises ValueError: If the MS has insufficient time samples
    :returns: MSInspectResult containing all metadata
    """
    if not ms.exists():
        raise FileNotFoundError(f"Measurement set not found: {ms}")
    logger.info(f"Inspecting: {ms}")

    time_info = get_time_info(ms)
    freq_info = get_frequency_info(ms)
    base_info = get_baseline_info(ms, freq_info)
    pol_info = get_polarisation_info(ms)
    field_info = get_field_info(ms)

    # Data columns
    tb = table()
    tb.open(str(ms))
    all_columns = tb.colnames()
    data_columns = {}
    for col in all_columns:
        if col == "DATA" or col.endswith("_DATA"):
            try:
                val = tb.getcell(col, 0)
                data_columns[col] = getattr(val, "shape", ())
            except Exception:
                pass
    tb.close()

    result = MSInspectResult(
        ms=ms,
        time=time_info,
        frequency=freq_info,
        baselines=base_info,
        polarisation=pol_info,
        fields=field_info,
        data_columns=data_columns,
    )
    return result


def pretty_print(result: MSInspectResult):
    t = result.time
    f = result.frequency
    b = result.baselines
    p = result.polarisation
    fi = result.fields

    print(f"\n{'='*55}")
    print(f"  MS: {result.ms}")
    print(f"{'='*55}")

    print("\n── Data Columns ──────────────────────────────────────")
    for name, shape in result.data_columns.items():
        print(f"  {name}: {shape}")

    print("\n── Time ──────────────────────────────────────────────")
    print(f"  Start:            {t.start_utc}")
    print(f"  End:              {t.end_utc}")
    print(f"  Integration time: {t.integration_time_s}s")
    print(f"  N integrations:   {t.n_integrations}")
    print(f"  Total duration:   {t.total_duration_min} min")

    print("\n── Frequency ─────────────────────────────────────────")
    print(f"  N SPWs:           {f.n_spw}")
    for i in range(f.n_spw):
        print(
            f"  SPW {i}: {f.spw_centre_hz[i]/1e6:.3f} MHz centre, "
            f"{f.spw_n_channels[i]} channels, "
            f"{f.spw_width_hz[i]/1e3:.1f} kHz/chan"
        )
    if f.centre_wavelength_m:
        print(f"  Centre λ (SPW 0): {f.centre_wavelength_m} m")

    print("\n── Baselines ─────────────────────────────────────────")
    print(f"  N antennas:       {b.n_antennas}  ({', '.join(b.antenna_names)})")
    print(f"  N baselines:      {b.n_baselines}")
    print(f"  UV min:           {b.uv_min_m} m  ({b.uv_min_lambda} kλ)")
    print(f"  UV max:           {b.uv_max_m} m  ({b.uv_max_lambda} kλ)")

    print("\n── Polarisation ──────────────────────────────────────")
    print(f"  Correlations:     {', '.join(p.polarisations)}")

    print("\n── Fields ────────────────────────────────────────────")
    for i, name in enumerate(fi.field_names):
        c = fi.phase_centres_deg[i]
        print(f"  [{i}] {name:20s}  RA={c['ra_deg']}°  Dec={c['dec_deg']}°")

    print(f"\n{'='*55}\n")


def main():
    from needle.lib.logging import setup_logging

    parser = argparse.ArgumentParser(description="Inspect a Measurement Set.")
    parser.add_argument("ms", type=Path, help="Path to the .ms file")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument(
        "--log-level",
        "--log_level",
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    result = inspect_ms(args.ms)

    if args.json:
        print(json.dumps(asdict(result), indent=2, default=str))
    else:
        pretty_print(result)


if __name__ == "__main__":
    main()

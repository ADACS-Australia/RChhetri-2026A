import argparse
import json
import logging
from dataclasses import dataclass, asdict
from functools import cached_property
from pathlib import Path
from typing import Optional

import numpy as np

from needle.lib.units import mjd_s_to_utc, rad_to_deg


def _get_table():
    """Imports casatools and returns a table object. This exists to avoid top-level CASA importing - thereby
    allowing this module to be loaded without having CASA installed. This is useful for orchestration."""
    try:
        from casatools import table

        return table()
    except ImportError:
        raise RuntimeError(
            "casatools is required to read an MS directly. Install it or load from JSON via MSInfo.from_json()."
        )


logger = logging.getLogger(__name__)

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


class MSInfo:
    """Lazy-loading inspector for a CASA Measurement Set.

    All sub-info sections are computed on first access and cached.
    Construct directly from an MS path, or deserialise from JSON.

    Example usage::

        ms = MSInfo("/data/my_obs.ms")
        print(ms.time.total_duration_min)
        print(ms.frequency.n_spw)
        ms.pretty_print()
        ms.to_json()
    """

    def __init__(self, ms: Path | str):
        self.ms = Path(ms)
        if not self.ms.exists():
            raise FileNotFoundError(f"Measurement set not found: {self.ms}")

        # Populated only when loading from JSON, bypassing lazy computation.
        self._preloaded: dict = {}

    @cached_property
    def time(self) -> TimeInfo:
        if "time" in self._preloaded:
            return TimeInfo(**self._preloaded["time"])
        return self._read_time()

    @cached_property
    def frequency(self) -> FrequencyInfo:
        if "frequency" in self._preloaded:
            return FrequencyInfo(**self._preloaded["frequency"])
        return self._read_frequency()

    @cached_property
    def baselines(self) -> BaselineInfo:
        if "baselines" in self._preloaded:
            return BaselineInfo(**self._preloaded["baselines"])
        return self._read_baselines()

    @cached_property
    def polarisation(self) -> PolarisationInfo:
        if "polarisation" in self._preloaded:
            return PolarisationInfo(**self._preloaded["polarisation"])
        return self._read_polarisation()

    @cached_property
    def fields(self) -> FieldInfo:
        if "fields" in self._preloaded:
            return FieldInfo(**self._preloaded["fields"])
        return self._read_fields()

    @cached_property
    def data_columns(self) -> dict[str, tuple]:
        if "data_columns" in self._preloaded:
            return self._preloaded["data_columns"]
        return self._read_data_columns()

    def to_json(self, output_dir: Optional[Path] = None) -> Path:
        """Serialise all metadata to a JSON file next to the MS.

        Accessing this property triggers loading of all sections.

        :param output_dir: Optional directory override; defaults to the MS parent.
        :returns: Path to the written JSON file.
        """
        out_dir = output_dir or self.ms.parent
        output_path = out_dir / f"{self.ms.stem}_inspect.json"
        payload = {
            "ms": str(self.ms),
            "time": asdict(self.time),
            "frequency": asdict(self.frequency),
            "baselines": asdict(self.baselines),
            "polarisation": asdict(self.polarisation),
            "fields": asdict(self.fields),
            "data_columns": self.data_columns,
        }
        logger.info(f"Writing inspection result to {output_path}")
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        return output_path

    @classmethod
    def from_json(cls, path: Path | str) -> "MSInfo":
        """Deserialise an MSInfo from a previously written JSON file.

        Sub-info sections are restored from the JSON rather than re-read
        from the MS, so the MS itself does not need to be accessible.

        :param path: Path to the JSON file.
        :raises FileNotFoundError: If the JSON file does not exist.
        :returns: MSInfo instance with all sections pre-populated.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Inspection JSON not found: {path}")
        logger.info(f"Loading inspection result from {path}")
        with open(path) as f:
            data = json.load(f)

        # Construct without existence check on the MS path.
        instance = object.__new__(cls)
        instance.ms = Path(data["ms"])
        instance._preloaded = data
        return instance

    def pretty_print(self) -> None:
        """Print a human-readable summary of all metadata sections."""
        t = self.time
        f = self.frequency
        b = self.baselines
        p = self.polarisation
        fi = self.fields

        print(f"\n{'=' * 55}")
        print(f"  MS: {self.ms}")
        print(f"{'=' * 55}")

        print("\n── Data Columns ──────────────────────────────────────")
        for name, shape in self.data_columns.items():
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
                f"  SPW {i}: {f.spw_centre_hz[i] / 1e6:.3f} MHz centre, "
                f"{f.spw_n_channels[i]} channels, "
                f"{f.spw_width_hz[i] / 1e3:.1f} kHz/chan"
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

        print(f"\n{'=' * 55}\n")

    def _read_time(self) -> TimeInfo:
        logger.debug(f"Reading time info from {self.ms}")
        tb = _get_table()
        tb.open(str(self.ms))
        times = np.unique(tb.getcol("TIME"))
        tb.close()

        if len(times) < 2:
            raise ValueError(f"MS {self.ms} has fewer than 2 unique timestamps — " "cannot determine integration time")

        int_time = float(times[1] - times[0])
        duration = float(times[-1] - times[0]) + int_time

        return TimeInfo(
            start_utc=mjd_s_to_utc(times[0]),
            end_utc=mjd_s_to_utc(times[-1]),
            start_mjd_s=float(times[0]),
            end_mjd_s=float(times[-1]),
            integration_time_s=int_time,
            n_integrations=len(times),
            total_duration_s=duration,
            total_duration_min=round(duration / 60, 4),
        )

    def _read_frequency(self) -> FrequencyInfo:
        logger.debug(f"Reading frequency info from {self.ms}")
        tb = _get_table()
        tb.open(str(self.ms / "SPECTRAL_WINDOW"))
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

        lam = (2.998e8 / centres[0]) if centres else None

        return FrequencyInfo(
            n_spw=n_spw,
            spw_centre_hz=centres,
            spw_width_hz=widths,
            spw_n_channels=nchan,
            spw_freq_min_hz=fmin,
            spw_freq_max_hz=fmax,
            centre_wavelength_m=round(lam, 6) if lam else None,
        )

    def _read_baselines(self) -> BaselineInfo:
        logger.debug(f"Reading baseline info from {self.ms}")
        tb = _get_table()

        tb.open(str(self.ms / "ANTENNA"))
        names = list(tb.getcol("NAME"))
        positions = tb.getcol("POSITION").T.tolist()
        tb.close()

        n_ant = len(names)

        tb.open(str(self.ms))
        uvw = tb.getcol("UVW")  # shape (3, n_rows)
        tb.close()

        uv_dist_m = np.sqrt(uvw[0] ** 2 + uvw[1] ** 2)
        uv_dist_m = uv_dist_m[uv_dist_m > 0]

        lam = self.frequency.centre_wavelength_m
        if lam and len(uv_dist_m):
            uv_min_lam = round(float(uv_dist_m.min()) / lam / 1e3, 4)
            uv_max_lam = round(float(uv_dist_m.max()) / lam / 1e3, 4)
            uv_min_m = round(float(uv_dist_m.min()), 2)
            uv_max_m = round(float(uv_dist_m.max()), 2)
        else:
            logger.warning("Could not compute UV range — missing wavelength or UV data")
            uv_min_lam = uv_max_lam = uv_min_m = uv_max_m = None

        return BaselineInfo(
            n_antennas=n_ant,
            antenna_names=names,
            antenna_positions_m=positions,
            n_baselines=n_ant * (n_ant - 1) // 2,
            uv_min_lambda=uv_min_lam,
            uv_max_lambda=uv_max_lam,
            uv_min_m=uv_min_m,
            uv_max_m=uv_max_m,
        )

    def _read_polarisation(self) -> PolarisationInfo:
        logger.debug(f"Reading polarisation info from {self.ms}")
        tb = _get_table()
        tb.open(str(self.ms / "POLARIZATION"))
        corr_types = tb.getcell("CORR_TYPE", 0)
        tb.close()

        pols = [STOKES_MAP.get(int(c), f"Unknown({c})") for c in corr_types]
        return PolarisationInfo(polarisations=pols, n_pols=len(pols))

    def _read_fields(self) -> FieldInfo:
        logger.debug(f"Reading field info from {self.ms}")
        tb = _get_table()
        tb.open(str(self.ms / "FIELD"))
        names = list(tb.getcol("NAME"))
        phase_dir = tb.getcol("PHASE_DIR")  # shape (2, 1, n_fields)
        tb.close()

        centres = [
            {
                "ra_deg": round(rad_to_deg(phase_dir[0, 0, i]) % 360, 6),
                "dec_deg": round(rad_to_deg(phase_dir[1, 0, i]), 6),
            }
            for i in range(len(names))
        ]
        return FieldInfo(n_fields=len(names), field_names=names, phase_centres_deg=centres)

    def _read_data_columns(self) -> dict[str, tuple]:
        logger.debug(f"Reading data columns from {self.ms}")
        tb = _get_table()
        tb.open(str(self.ms))
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
        return data_columns


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

    msinfo = MSInfo(args.ms)

    if args.json:
        msinfo.to_json()
    else:
        msinfo.pretty_print()


if __name__ == "__main__":
    main()

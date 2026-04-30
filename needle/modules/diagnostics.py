import argparse
from functools import cached_property
import json
import logging
from pathlib import Path
from typing import Literal

import numpy as np
import matplotlib

matplotlib.use("Agg")  # non-interactive backend, safe for scripting
import matplotlib.pyplot as plt
from pydantic import BaseModel, field_validator

from needle.config.base import ContainerConfig
from needle.lib.validate import validate_path_ms
from needle.lib.casa import open_table, open_msmetadata
from needle.modules.needle_context import SubprocessExecContext

logger = logging.getLogger(__name__)


class DiagnosticsContext(SubprocessExecContext):
    """Wraps execution of MS Diagnostics"""

    ms: Path
    "Path to the measurement set to perform diagnostics for"
    gcal: Path | None = None
    "Path to the gain cal solution table"
    bpcal: Path | None = None
    "Path to the gain bpcal solution table"
    output_dir: Path | None = None
    "Location to output the diagnostics to"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    "Log level. Only relevant if runtime is set"

    @field_validator("ms")
    @classmethod
    def _valid_ms(cls, ms: Path) -> Path:
        validate_path_ms(ms)
        return ms

    @property
    def cmd(self) -> list[list[str]]:
        cmds = ["needle-diagnostics", str(self.ms), "--log-level", self.log_level]
        if self.output_dir:
            cmds += ["--output_dir", str(self.output_dir)]
        if self.gcal:
            cmds += ["--gcal", str(self.gcal)]
        if self.bpcal:
            cmds += ["--bpcal", str(self.gcal)]
        return [cmds]


class DiagnosticsOutput(BaseModel):
    antenna_amp_stats_plot: Path | None = None
    antenna_amp_stats_data: Path | None = None
    amp_phase_vs_time_plot: Path | None = None
    amp_phase_vs_channel_plot: Path | None = None
    antenna_positions_plot: Path | None = None
    flag_summary_plot: Path | None = None
    flag_summary_data: Path | None = None
    gain_caltable_plot: Path | None = None
    bandpass_caltable_plot: Path | None = None

    @property
    def all_files(self) -> list[Path]:
        return [p for p in self.model_fields_set if p is not None]


class MSDiagnostics(BaseModel):
    """Class to take care of MS Diagnostics computation"""

    ms: Path
    "Path to the measurement set to perform diagnostics for"
    gcal: Path | None
    "Path to the gain cal solution table"
    bpcal: Path | None
    "Path to the gain bpcal solution table"
    output_dir: Path | None
    "Location to output the diagnostics to"
    spw: int = 0
    "Spectral window index to run diagnostics on"
    _output: DiagnosticsOutput
    "The DiagnosticsOutput object to store output paths"

    @field_validator("ms")
    @classmethod
    def _valid_ms(cls, ms: Path) -> Path:
        validate_path_ms(ms)
        if not ms.exists():
            raise ValueError(f"Measurement set does not exist: {ms}")
        return ms

    @field_validator("gcal")
    @classmethod
    def _valid_gcal(cls, gcal: Path) -> Path:
        if not gcal.exists():
            raise ValueError(f"Gaincal table does not exist: {gcal}")
        return gcal

    @field_validator("bpcal")
    @classmethod
    def _valid_bpcal(cls, bpcal: Path) -> Path:
        if not bpcal.exists():
            raise ValueError(f"Bandpass table does not exist: {bpcal}")
        return bpcal

    def model_post_init(self, __context):
        if self.output_dir is None:
            self.output_dir: Path = self.ms.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)
        object.__setattr__(self, "_output", DiagnosticsOutput())

    @property
    def tb_query(self):
        "Query to use for subtable creation"
        return f"DATA_DESC_ID=={self.spw}"

    @property
    def amp_phase_vs_time_plot(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_amp_phase_vs_time.png"

    @property
    def amp_phase_vs_channel_plot(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_amp_phase_vs_channel.png"

    @property
    def gain_caltable_plot(self) -> Path:
        return self.output_dir / f"{self.gcal.stem}_gain_caltable.png"

    @property
    def bandpass_caltable_plot(self) -> Path:
        return self.output_dir / f"{self.bpcal.stem}_bandpass_caltable.png"

    @property
    def antenna_positions_plot(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_antenna_positions.png"

    @property
    def flag_summary_plot(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_flag_summary.png"

    @property
    def flag_summary_data(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_flag_summary.json"

    @property
    def antenna_amp_stats_plot(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_antenna_amp_stats.png"

    @property
    def antenna_amp_stats_data(self) -> Path:
        return self.output_dir / f"{self.ms.stem}_antenna_amp_stats.json"

    @cached_property
    def _active_antenna_indices(self) -> list[int]:
        """Antenna indices that actually appear in the data."""
        with open_table(self.ms) as tb:
            ant1 = tb.getcol("ANTENNA1")
            ant2 = tb.getcol("ANTENNA2")
        all_indices = np.union1d(ant1, ant2)
        return list(all_indices)

    @cached_property
    def _inactive_antenna_names(self) -> list[str]:
        with open_msmetadata(self.ms) as md:
            all_names = md.antennanames()
        return [all_names[i] for i in range(len(all_names)) if i not in self._active_antenna_indices]

    @cached_property
    def _antenna_names(self):
        """Return list of antenna names from an MS."""
        with open_msmetadata(self.ms) as md:
            all_names = md.antennanames()
        return [all_names[i] for i in self._active_antenna_indices]

    @cached_property
    def _antenna_colors(self):
        """Return n distinct colors cycling through a colormap."""
        cmap = plt.get_cmap("plasma")
        n = len(self._antenna_names)
        return [cmap(i / (n - 1)) for i in range(n)]

    def _antenna_legend_handles(self, style: Literal["line", "scatter"] = "line") -> list:
        if style == "scatter":
            return [
                plt.Line2D(
                    [0], [0], marker="o", color="w", markerfacecolor=self._antenna_colors[i], markersize=5, label=n
                )
                for i, n in enumerate(self._antenna_names)
            ]
        return [
            plt.Line2D([0], [0], color=self._antenna_colors[i], lw=2, label=n)
            for i, n in enumerate(self._antenna_names)
        ]

    def _amp_phase_fig(self, xlabel: str, title_amp: str, title_phase: str) -> tuple[plt.Figure, np.ndarray]:
        fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
        axes[0].set_ylabel("Amplitude")
        axes[0].set_title(title_amp)
        axes[0].grid(True, alpha=0.3)
        axes[1].set_ylabel("Phase (deg)")
        axes[1].set_xlabel(xlabel)
        axes[1].set_title(title_phase)
        axes[1].set_ylim(-185, 185)
        axes[1].grid(True, alpha=0.3)
        return fig, axes

    def _save(self, fig: plt.Figure, path: Path):
        plt.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        logger.info(f"Saved plot: {path}")
        plt.close(fig)

    def to_output(self) -> DiagnosticsOutput:
        return self._output

    def amp_phase_vs_time(self, corr=0, avg_channels=True):
        """Plot amplitude and phase vs. time, coloured per antenna.

        :param corr: Correlation index (0 = RR or XX)
        :param avg_channels: Average across channels before plotting
        """
        logger.debug("Diagnostics: amp_phase_vs_time")
        with open_table(self.ms) as tb:
            subtb = tb.query(self.tb_query)
            data = subtb.getcol("DATA")  # (ncorr, nchan, nrow)
            flags = subtb.getcol("FLAG")  # (ncorr, nchan, nrow)
            times = subtb.getcol("TIME")  # (nrow,)
            ant1 = subtb.getcol("ANTENNA1")  # (nrow,)
            subtb.close()

        # Apply flags
        d = data[corr]  # (nchan, nrow)
        f = flags[corr]  # (nchan, nrow)
        d = np.where(f, np.nan + 0j, d)

        if avg_channels:
            d = np.nanmean(d, axis=0)  # (nrow,)
        else:
            d = d[d.shape[0] // 2]  # centre channel

        amp = np.abs(d)
        phase = np.angle(d, deg=True)

        # Normalise time to minutes from start
        t0 = np.nanmin(times)
        t_minutes = (times - t0) / 60.0

        fig, axes = self._amp_phase_fig(
            xlabel="Time (minutes from start)",
            title_amp=f"Amplitude vs Time  (spw={self.spw}, corr={corr})",
            title_phase=f"Phase vs Time  (spw={self.spw}, corr={corr})",
        )
        axes[1].set_xlim(t_minutes.min(), t_minutes.max())

        for i, name in enumerate(self._antenna_names):
            mask = ant1 == i
            if not np.any(mask):
                continue
            axes[0].scatter(t_minutes[mask], amp[mask], s=2, color=self._antenna_colors[i], label=name, alpha=0.7)
            axes[1].scatter(t_minutes[mask], phase[mask], s=2, color=self._antenna_colors[i], alpha=0.7)

        # Shared legend outside plot
        fig.legend(
            handles=self._antenna_legend_handles("scatter"), loc="center right", fontsize=7, bbox_to_anchor=(1.08, 0.5)
        )

        self._save(fig, self.amp_phase_vs_time_plot)
        self._output.amp_phase_vs_time_plot = self.amp_phase_vs_time_plot

    def amp_phase_vs_channel(self, corr=0, avg_time=True):
        """Plot amplitude and phase vs. channel, coloured per antenna."""
        logger.debug("Diagnostics: amp_phase_vs_channel")
        with open_table(self.ms) as tb:
            subtb = tb.query(self.tb_query)
            data = subtb.getcol("DATA")  # (ncorr, nchan, nrow)
            flags = subtb.getcol("FLAG")
            ant1 = subtb.getcol("ANTENNA1")
            subtb.close()

        d = data[corr].astype(complex)  # (nchan, nrow)
        f = flags[corr]
        d[f] = np.nan
        nchan = d.shape[0]

        channels = np.arange(nchan)
        fig, axes = self._amp_phase_fig(
            xlabel="Time (minutes from start)",
            title_amp=f"Amplitude vs Channel  (spw={self.spw}, corr={corr})",
            title_phase=f"Phase vs Channel  (spw={self.spw}, corr={corr})",
        )
        axes[1].set_xlim(channels.min(), channels.max())

        for i, name in enumerate(self._antenna_names):
            mask = ant1 == i
            if not np.any(mask):
                continue
            d_ant = d[:, mask]  # (nchan, nrows_for_ant)
            if avg_time:
                d_ant = np.nanmean(d_ant, axis=1)  # (nchan,)
            else:
                d_ant = d_ant[:, 0]

            axes[0].plot(channels, np.abs(d_ant), color=self._antenna_colors[i], lw=0.8, label=name, alpha=0.8)
            axes[1].plot(channels, np.angle(d_ant, deg=True), color=self._antenna_colors[i], lw=0.8, alpha=0.8)

        fig.legend(
            handles=self._antenna_legend_handles("line"), loc="center right", fontsize=7, bbox_to_anchor=(1.08, 0.5)
        )

        self._save(fig, self.amp_phase_vs_channel_plot)
        self._output.amp_phase_vs_channel_plot = self.amp_phase_vs_channel_plot

    def gain_caltable(self):
        """Plot gain caltable amplitude and phase per antenna.
        Works for tables produced by gaincal (TYPE='G Jones')."""
        logger.debug("Diagnostics: gain_caltable")
        with open_table(self.gcal) as tb:
            gains = tb.getcol("CPARAM")  # (npol, 1, nrow)
            flags = tb.getcol("FLAG")
            ant_col = tb.getcol("ANTENNA1")

        names, amps, phases = [], [], []
        for idx, name in zip(self._active_antenna_indices, self._antenna_names):
            mask = (ant_col == idx) & ~flags[0, 0, :]
            if not np.any(mask):
                continue
            g = gains[0, 0, mask]
            amps.append(float(np.abs(g).mean()))
            phases.append(float(np.angle(g, deg=True).mean()))
            names.append(name)

        x = np.arange(len(names))
        colors = [self._antenna_colors[self._antenna_names.index(n)] for n in names]
        amp_array = np.array(amps)
        phase_array = np.array(phases)
        amp_pad = (amp_array.max() - amp_array.min()) * 0.5 or 0.01
        phase_pad = (phase_array.max() - phase_array.min()) * 0.5 or 1.0

        fig, axes = self._amp_phase_fig(
            xlabel="Antenna",
            title_amp="Gain Amplitude — pol 0",
            title_phase="Gain Phase — pol 0",
        )
        axes[0].bar(x, amps, color=colors, edgecolor="navy", lw=0.5)
        axes[0].set_ylim(amp_array.min() - amp_pad, amp_array.max() + amp_pad)

        axes[1].bar(x, phases, color=colors, edgecolor="navy", lw=0.5)
        axes[1].set_ylim(phase_array.min() - phase_pad, phase_array.max() + phase_pad)
        axes[1].set_xticks(x)
        axes[1].set_xticklabels(names, rotation=45, ha="right", fontsize=8)

        self._save(fig, self.gain_caltable_plot)
        self._output.gain_caltable_plot = self.gain_caltable_plot

    def bandpass_caltable(self):
        """Plot bandpass caltable amplitude and phase vs. channel, per antenna.
        Works for tables produced by bandpass (TYPE='B Jones')."""
        logger.debug("Diagnostics: bandpass_caltable")
        with open_table(self.bpcal) as tb:
            gains = tb.getcol("CPARAM")  # (npol, nchan, nrow)
            flags = tb.getcol("FLAG")
            ant_col = tb.getcol("ANTENNA1")

        nchan = gains.shape[1]
        channels = np.arange(nchan)

        fig, axes = self._amp_phase_fig(
            xlabel="Channel",
            title_amp="Bandpass Amplitude — pol 0",
            title_phase="Bandpass Phase — pol 0",
        )
        axes[1].set_xlim(channels.min(), channels.max())

        for i, _ in enumerate(self._antenna_names):
            mask = ant_col == i
            if not np.any(mask):
                continue
            g = gains[0, :, mask].T
            fl = flags[0, :, mask].T
            g = np.where(fl, np.nan + 0j, g)
            g_mean = np.nanmean(g, axis=1)

            axes[0].plot(channels, np.abs(g_mean), color=self._antenna_colors[i], lw=0.8, alpha=0.8)
            axes[1].plot(channels, np.angle(g_mean, deg=True), color=self._antenna_colors[i], lw=0.8, alpha=0.8)

        fig.legend(
            handles=self._antenna_legend_handles("line"), loc="center right", fontsize=7, bbox_to_anchor=(1.08, 0.5)
        )
        self._save(fig, self.bandpass_caltable_plot)
        self._output.bandpass_caltable_plot = self.bandpass_caltable_plot

    def antenna_positions(self):
        """Plot antenna positions (ENU) with names labelled."""
        logger.debug("Diagnostics: antenna_positions")
        with open_table(self.ms / "ANTENNA") as tb:
            positions = tb.getcol("POSITION")  # (3, nant) ITRF XYZ
            names = tb.getcol("NAME")

        # Convert to approximate ENU relative to array centre
        xyz = positions.T  # (nant, 3)
        mean = xyz.mean(axis=0)
        dxyz = xyz - mean

        # Simple rotation to East-North (ignores curvature, fine for diagnostic)
        lat = np.arctan2(mean[2], np.sqrt(mean[0] ** 2 + mean[1] ** 2))
        lon = np.arctan2(mean[1], mean[0])
        slat, clat = np.sin(lat), np.cos(lat)
        slon, clon = np.sin(lon), np.cos(lon)

        east = -slon * dxyz[:, 0] + clon * dxyz[:, 1]
        north = -slat * clon * dxyz[:, 0] - slat * slon * dxyz[:, 1] + clat * dxyz[:, 2]

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.scatter(east, north, s=60, zorder=3, color="steelblue", edgecolors="navy", lw=0.5)

        for i, name in enumerate(names):
            ax.annotate(name, (east[i], north[i]), textcoords="offset points", xytext=(5, 5), fontsize=7, color="black")

        ax.set_xlabel("East (m)")
        ax.set_ylabel("North (m)")
        ax.set_title("Antenna Positions (ENU)")
        ax.set_aspect("equal")
        ax.grid(True, alpha=0.3)

        self._save(fig, self.antenna_positions_plot)
        self._output.antenna_positions_plot = self.antenna_positions_plot

    def flag_summary(self):
        """
        Compute and plot flagging fraction per antenna. Writes summary JSON.
        """
        logger.debug("Diagnostics: flag_summary")
        with open_table(self.ms) as tb:
            flags = tb.getcol("FLAG")
            ant1 = tb.getcol("ANTENNA1")
            ant2 = tb.getcol("ANTENNA2")

        nant = len(self._antenna_names)
        ant_flagged = np.zeros(nant)
        ant_total = np.zeros(nant)
        for i, (idx, _) in enumerate(zip(self._active_antenna_indices, self._antenna_names)):
            mask = (ant1 == idx) | (ant2 == idx)
            ant_total[i] = flags[:, :, mask].size
            ant_flagged[i] = flags[:, :, mask].sum()

        ant_frac = np.zeros(nant)
        nonzero = ant_total > 0
        ant_frac[nonzero] = ant_flagged[nonzero] / ant_total[nonzero] * 100
        overall = flags.sum() / flags.size * 100
        summary = {
            "overall_flagged_pct": round(float(overall), 2),
            "antenna": {name: round(float(ant_frac[i]), 2) for i, name in enumerate(self._antenna_names)},
            "inactive_antennas": self._inactive_antenna_names,
        }

        # Plot
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.bar(range(nant), ant_frac, color="steelblue", edgecolor="navy", lw=0.5)
        ax.set_xticks(range(nant))
        ax.set_xticklabels(self._antenna_names, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Flagged (%)")
        ax.set_title(
            f"Flagging Fraction per Antenna " f"({nant}/{nant + len(self._inactive_antenna_names)} antennas active)"
        )
        ax.set_ylim(0, 105)
        ax.grid(True, axis="y", alpha=0.3)

        if self._inactive_antenna_names:
            fig.text(
                0.5,
                -0.02,
                f"Inactive antennas (no data): {', '.join(self._inactive_antenna_names)}",
                ha="center",
                fontsize=8,
                color="grey",
                style="italic",
            )

        self._save(fig, self.flag_summary_plot)
        self._output.flag_summary_plot = self.flag_summary_plot

        # To JSON
        with open(self.flag_summary_data, "w") as f:
            json.dump(summary, f, indent=2)
        self._output.flag_summary_data = self.flag_summary_data
        logger.info(f"Saved flag summary JSON: {self.flag_summary_data}")

        return summary

    def antenna_amp_stats(self, datacolumn="DATA"):
        """Plot per-antenna amplitude statistics as a bar chart with error bars."""
        logger.debug("Diagnostics: antenna_amp_stats")
        with open_table(self.ms) as tb:
            subtb = tb.query(self.tb_query)
            data = subtb.getcol(datacolumn)
            flags = subtb.getcol("FLAG")
            ant1 = subtb.getcol("ANTENNA1")
            subtb.close()

        data = np.where(flags, np.nan, np.abs(data))
        means, stds, names = [], [], []
        for i, name in enumerate(self._antenna_names):
            mask = ant1 == i
            if not np.any(mask):
                continue
            vals = data[:, :, mask].flatten()
            vals = vals[~np.isnan(vals)]
            if len(vals) == 0:
                continue
            means.append(vals.mean())
            stds.append(vals.std())
            names.append(name)

        x = np.arange(len(names))
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.bar(
            x,
            means,
            yerr=stds,
            color="steelblue",
            edgecolor="navy",
            lw=0.5,
            error_kw={"elinewidth": 1, "capsize": 3, "ecolor": "navy"},
        )
        ax.set_xticks(x)
        ax.set_xticklabels(names, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Amplitude")
        ax.set_title(f"Per-Antenna Amplitude Statistics ({datacolumn}, spw={self.spw})")
        ax.grid(True, axis="y", alpha=0.3)

        self._save(fig, self.antenna_amp_stats_plot)
        self._output.antenna_amp_stats_plot = self.antenna_amp_stats_plot

        # Write to JSON
        stats = {
            name: {
                "mean": round(float(means[i]), 6),
                "std": round(float(stds[i]), 6),
            }
            for i, name in enumerate(names)
        }

        with open(self.antenna_amp_stats_data, "w") as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Saved antenna amp stats JSON: {self.antenna_amp_stats_data}")
        self._output.antenna_amp_stats_data = self.antenna_amp_stats_data

    def run_all_diagnostics(self):
        """Run all available diagnostics using the supplied tables"""
        logger.info(f"Running diagnostics on: {self.ms}")
        logger.debug(f"Output directory: {self.output_dir}\n")

        self.antenna_positions()
        self.flag_summary()
        self.amp_phase_vs_time()
        self.amp_phase_vs_channel()
        self.antenna_amp_stats()

        if self.gcal:
            logger.debug(f"Gain table supplied. Will create diagnostics for: {self.gcal}")
            self.gain_caltable()
        if self.bpcal:
            logger.debug(f"Bandpass table supplied. Will create diagnostics for: {self.bpcal}")
            self.bandpass_caltable()


def diagnostics(ctx: DiagnosticsContext) -> DiagnosticsOutput:
    """If runtime is provided, rerun self inside the provided container.
    Otherwise, run in the current environment.

    This setup allows us to run CASA functions without needing CASA libraries installed locally.

    :param ctx: The DiagnosticsContext object
    :return: The DiagnosticsOutput object - contains paths of output files.
    """
    msd = MSDiagnostics(ms=ctx.ms, gcal=ctx.gcal, bpcal=ctx.bpcal, output_dir=ctx.output_dir)
    if ctx.runtime:
        logger.info(f"Loading container: {ctx.runtime.image}")
        ctx.log_cmd()
        procs = ctx.execute()
        for p in procs:
            if p.stderr:
                logger.warning(p.stderr)
            if p.stdout:
                print(p.stdout)
    else:
        msd.run_all_diagnostics()

    return msd.to_output()


def main():
    from needle.lib.logging import setup_logging

    parser = argparse.ArgumentParser(
        description="Run diagnostics on a measurement set. Optionally supply bandpass/gain calibrators."
    )
    parser.add_argument("ms", type=Path, help="Path to the .ms file")
    parser.add_argument(
        "--output_dir",
        type=Path,
        help="Directory to write outputs to. Will be created if it doesn't exist. If not supplied, uses MS directory.",
    )
    parser.add_argument("--gcal", type=Path, help="Path to the gain caltable")
    parser.add_argument("--bpcal", type=Path, help="Path to the bandpass caltable")
    parser.add_argument(
        "--log-level",
        "--log_level",
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="logger level",
    )

    container_group = parser.add_argument_group(title="Container Arguments")
    ContainerConfig.add_to_parser(container_group)

    args = parser.parse_args()
    setup_logging(args.log_level)

    runtime = None
    if args.image:
        runtime = ContainerConfig.from_namespace(args)

    ctx = DiagnosticsContext(
        runtime=runtime,
        ms=args.ms,
        gcal=args.gcal,
        bpcal=args.bpcal,
        output_dir=args.output_dir,
        log_level=args.log_level,
    )
    diagnostics(ctx)


if __name__ == "__main__":
    main()

import logging

import pandas as pd

logging = logging.getLogger(__name__)


class QC_Zone:

    def __init__(self, hr, zones, week):
        self.hr = hr
        self.zones = zones
        self.week = int(week)
        self.err = {}
        self.zone_metrics = None
        self._is_supervised = False

    def supervised(self):
        """
        Run the supervised zone QC
        """
        self._is_supervised = True
        # These define the weeks and their expected zones
        zone_info = {
            1: {
                "zones": [1, 2, 3],
                "warmup_min": 5,
                "bounded_min": 15,
                "unbounded_min": 15,
                "cooldown_min": 5,
            },
            2: {
                "zones": [1, 2, 3],
                "warmup_min": 5,
                "bounded_min": 20,
                "unbounded_min": 10,
                "cooldown_min": 5,
            },
            3: {
                "zones": [2, 3],
                "warmup_min": 5,
                "bounded_min": 25,
                "unbounded_min": 5,
                "cooldown_min": 5,
            },
            4: {
                "zones": [2, 3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            5: {
                "zones": [3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            6: {
                "zones": [3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
        }

        weekly_plan = zone_info.get(self.week)
        if weekly_plan is None:
            self.err["zone_summary"] = [f"no supervised plan for week {self.week}", None]
            return None

        self._cap_hr_to_minutes(45)
        return self._run_zone_qc(weekly_plan)

    def unsupervised(self):
        self._is_supervised = False

        training_plan = {
            7: {
                "zones": [3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            8: {
                "zones": [3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            9: {
                "zones": [3, 4],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            10: {
                "zones": [3, 4, 5],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            11: {
                "zones": [4, 5],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
            12: {
                "zones": [4, 5],
                "warmup_min": 5,
                "bounded_min": 30,
                "unbounded_min": 0,
                "cooldown_min": 5,
            },
        }

        weekly_plan = training_plan.get(self.week)
        if weekly_plan is None:
            self.err["zone_summary"] = [f"no unsupervised plan for week {self.week}", None]
            return None

        return self._run_zone_qc(weekly_plan)

    def _run_zone_qc(self, weekly_plan: dict):
        """
        Shared helper implementing the zone QC calculations.

        things to extract
        1. Time spent in zones
           - Use subject-level zone bounds from hr/util/zone/extract_zones.py
             (columns z1_start/z1_end...z5_start/z5_end after midpoint_snap)
             to map each hr sample to a zone bucket based on its bpm.
           - Ignore warmup/cooldown entirely; only tally time in the
             designated weekly_plan["zones"] plus time above the top zone or
             below the bottom zone.
        2. Time spent above/below zones
           - Above: hr > highest end of the highest zone in weekly_plan["zones"].
           - Below: hr < lowest start of the lowest zone in weekly_plan["zones"].
           - Calculate durations using the subject’s hr file (time, hr) that
             extract_hr() produces.
        3. Longest bounded bout and target flag
           - Find the longest continuous bout where hr stays within or above
             the allowed zones (never dropping below the lowest allowed start).
           - Boolean flag: True if a single continuous bounded bout meets or
             exceeds weekly_plan["bounded_min"] minutes without dipping below
             that lower bound (going above is acceptable); False otherwise.

        Returns a dict of summary metrics and populates self.err with messages.
        """

        self._weekly_plan = weekly_plan
        ctx = self._zone_context(weekly_plan)
        if ctx is None:
            self.err["zone_summary"] = ["hr data missing for zone QC", None]
            return None

        hr_df, hr_vals, deltas, zone_bounds, allowed_zones, lowest_allowed, highest_allowed = ctx
        category = pd.Series("below", index=hr_df.index)
        category.loc[hr_vals > highest_allowed] = "above"
        for z in allowed_zones:
            start, end = zone_bounds[z]
            in_zone = hr_vals.between(start, end, inclusive="both")
            category.loc[in_zone] = f"z{z}"

        # Aggregate durations
        durations = deltas
        time_in_allowed = durations[category.isin([f"z{z}" for z in allowed_zones])].sum()
        time_above = durations[category == "above"].sum()
        time_below = durations[category == "below"].sum()

        # Longest bounded bout without dropping below lowest_allowed
        good_mask = hr_vals >= lowest_allowed
        run_id = good_mask.ne(good_mask.shift()).cumsum()
        bout_lengths = (
            pd.DataFrame({"good": good_mask, "dur": durations, "run": run_id})
            .groupby("run")
            .agg(is_good=("good", "first"), duration_s=("dur", "sum"))
        )
        good_bouts = bout_lengths.loc[bout_lengths["is_good"], "duration_s"]
        longest_bout = good_bouts.max() if not good_bouts.empty else 0
        bounded_met = longest_bout >= weekly_plan["bounded_min"] * 60

        zone_compliance = self._calc_zone_compliance(
            time_in_allowed,
            time_above,
            time_below,
        )
        mazd = self._calc_mazd(weekly_plan, apply_cap=not self._is_supervised)
        self.zone_metrics = {
            "week": self.week,
            "time_in_allowed_s": float(time_in_allowed),
            "time_above_s": float(time_above),
            "time_below_s": float(time_below),
            "longest_bounded_bout_s": float(longest_bout),
            "bounded_met": bool(bounded_met),
            "zone_compliance": zone_compliance,
            "mazd": mazd,
        }
        summary_msg = (
            f"time_in_allowed_s={time_in_allowed:.1f}; "
            f"time_above_s={time_above:.1f}; "
            f"time_below_s={time_below:.1f}; "
            f"longest_bounded_bout_s={longest_bout:.1f}; "
            f"bounded_met={bounded_met}"
        )
        self.err["zone_summary"] = [summary_msg, None]
        if not bounded_met:
            self.err["bounded_short"] = [
                "bounded time target not met without dropping below zone floor",
                None,
            ]

        return self.zone_metrics

    def _zone_context(self, weekly_plan: dict):
        if self.hr is None or self.hr.empty:
            return None

        # Ensure time is datetime and ordered
        hr_df = self.hr.copy()
        hr_df["time"] = pd.to_datetime(hr_df["time"])
        hr_df = hr_df.sort_values("time").reset_index(drop=True)

        # Build zone bounds from subject-level zones
        zone_bounds = {}
        for i in range(1, 6):
            start_col = f"z{i}_start"
            end_col = f"z{i}_end"
            if start_col in self.zones.columns and end_col in self.zones.columns:
                zone_bounds[i] = (
                    int(self.zones[start_col].iat[0]),
                    int(self.zones[end_col].iat[0]),
                )
        if not zone_bounds:
            return None

        allowed_zones = weekly_plan.get("zones") if weekly_plan else None
        if not allowed_zones:
            return None

        lowest_allowed = min(zone_bounds[z][0] for z in allowed_zones)
        highest_allowed = max(zone_bounds[z][1] for z in allowed_zones)

        # Per-sample durations (seconds) using the next-sample delta; last sample uses median delta
        time_vals = hr_df["time"]
        deltas = (time_vals.shift(-1) - time_vals).dt.total_seconds()
        median_delta = deltas.dropna().median()
        if pd.isna(median_delta):
            median_delta = 0
        deltas = deltas.fillna(median_delta).clip(lower=0)

        hr_vals = hr_df["hr"]
        return (
            hr_df,
            hr_vals,
            deltas,
            zone_bounds,
            allowed_zones,
            lowest_allowed,
            highest_allowed,
        )

    def _cap_hr_to_minutes(self, max_minutes: int):
        if self.hr is None or self.hr.empty:
            return

        hr_df = self.hr.copy()
        hr_df["time"] = pd.to_datetime(hr_df["time"])
        hr_df = hr_df.sort_values("time").reset_index(drop=True)

        time_vals = hr_df["time"]
        deltas = (time_vals.shift(-1) - time_vals).dt.total_seconds()
        median_delta = deltas.dropna().median()
        if pd.isna(median_delta):
            median_delta = 0
        deltas = deltas.fillna(median_delta).clip(lower=0)

        max_seconds = max_minutes * 60
        cum_end = deltas.cumsum()
        start_offset = cum_end - deltas
        in_window = start_offset < max_seconds
        if not in_window.any():
            self.hr = hr_df.iloc[:0].copy()
            return

        capped = hr_df.loc[in_window].copy()
        last_idx = in_window[in_window].index[-1]
        if cum_end.at[last_idx] > max_seconds:
            remaining = max_seconds - start_offset.at[last_idx]
            if remaining > 0:
                cap_row = hr_df.loc[[last_idx]].copy()
                cap_row["time"] = time_vals.at[last_idx] + pd.to_timedelta(remaining, unit="s")
                capped = pd.concat([capped, cap_row], ignore_index=True)

        self.hr = capped.reset_index(drop=True)

    def _calc_mazd(self, weekly_plan: dict | None = None, apply_cap: bool = True):
        """
        Calculate the Mean Absolute Zone Deviation (MAZD):
        Formula:
            1/T * ∑ |z_i - z_target|

        Purpose:
            The MAZD quantifies how closely an individual's heart rate
            during exercise sessions aligns with the prescribed target zone.
        """
        ctx = self._zone_context(weekly_plan or getattr(self, "_weekly_plan", None))
        if ctx is None:
            return None

        hr_df, hr_vals, deltas, zone_bounds, allowed_zones, _, _ = ctx
        if apply_cap:
            max_seconds = 45 * 60
            cum_end = deltas.cumsum()
            in_window = (cum_end - deltas) < max_seconds
            window_deltas = deltas.where(in_window, 0)
            overflow = cum_end > max_seconds
            if overflow.any():
                last_idx = overflow.idxmax()
                remaining = max_seconds - (cum_end.at[last_idx] - deltas.at[last_idx])
                if remaining < 0:
                    remaining = 0
                window_deltas.at[last_idx] = min(window_deltas.at[last_idx], remaining)
        else:
            window_deltas = deltas
        zone_idx = pd.Series(pd.NA, index=hr_df.index, dtype="Float64")
        for z, (start, end) in zone_bounds.items():
            in_zone = hr_vals.between(start, end, inclusive="both")
            zone_idx.loc[in_zone] = float(z)

        min_start = min(start for start, _ in zone_bounds.values())
        max_end = max(end for _, end in zone_bounds.values())
        zone_idx.loc[hr_vals < min_start] = 0.0
        zone_idx.loc[hr_vals > max_end] = float(max(zone_bounds.keys()) + 1)

        valid = zone_idx.notna()
        total_time = window_deltas[valid].sum()
        if total_time <= 0:
            return None

        def nearest_allowed(zone_val: float) -> int:
            return min(allowed_zones, key=lambda target: abs(zone_val - target))

        nearest = zone_idx[valid].apply(nearest_allowed)
        deviation = (zone_idx[valid] - nearest).abs()
        mazd = (deviation * window_deltas[valid]).sum() / total_time
        return float(mazd)

    def _calc_zone_compliance(
        self,
        time_in_allowed_s: float,
        time_above_s: float,
        time_below_s: float,
    ):
        """
        Formula:
            1 - (out_of_zone_time / total_time)

        Purpose:
            This metric quantifies the proportion of time an individual's heart rate
            remains within the prescribed target zones during exercise sessions.
        """
        total_time = time_in_allowed_s + time_above_s + time_below_s
        if total_time <= 0:
            return None
        return float(time_in_allowed_s / total_time)

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

    def supervised(self):
        """
        Run the supervised zone QC
        """
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

        return self._run_zone_qc(weekly_plan)

    def unsupervised(self):

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

        if self.hr is None or self.hr.empty:
            self.err["zone_summary"] = ["hr data missing for zone QC", None]
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

        allowed_zones = weekly_plan["zones"]
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

        self.zone_metrics = {
            "week": self.week,
            "time_in_allowed_s": float(time_in_allowed),
            "time_above_s": float(time_above),
            "time_below_s": float(time_below),
            "longest_bounded_bout_s": float(longest_bout),
            "bounded_met": bool(bounded_met),
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

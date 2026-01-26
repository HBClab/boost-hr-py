import os
import re
from pathlib import Path
import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


def save_zones(zone_master: dict[str, list[list[Any]]], out_csv: str | os.PathLike) -> pd.DataFrame:
    """
    Flatten zone QC summaries into a tidy table and persist as CSV.

    Parameters
    ----------
    zone_master : dict
        Expected format: { subject: [ [file_path, metrics_dict], ... ], ... }
        where metrics_dict is what QC_Zone.supervised/unsupervised returns,
        e.g. {
            "week": 1,
            "time_in_allowed_s": 1800.0,
            "time_above_s": 120.0,
            "time_below_s": 45.0,
            "longest_bounded_bout_s": 1650.0,
            "bounded_met": True,
            "mazd": 0.25,
            "trimp": 75.2,
        }
    out_csv : str | PathLike
        Destination CSV path.

    Returns
    -------
    pd.DataFrame
        One row per file, columns: group, subject, week, session,
        time_in_allowed_s, time_above_s, time_below_s,
        longest_bounded_bout_s, bounded_met, mazd, trimp.
    """
    rows: list[dict[str, Any]] = []

    def _parse_path(file_path: str) -> dict[str, Any]:
        """Extract session metadata from the file path."""
        group = None
        if re.search(r"/Supervised/", file_path, re.IGNORECASE):
            group = "Supervised"
        elif re.search(r"/Unsupervised/", file_path, re.IGNORECASE):
            group = "Unsupervised"

        subject = None
        match_subject = re.search(r"/(sub\d+)/", file_path, re.IGNORECASE)
        if match_subject:
            subject = match_subject.group(1).lower()

        week = None
        session = None
        match_ws = re.search(r"_wk(\d+)_ses(\d+(?:\.\d+)?)", file_path, re.IGNORECASE)
        if match_ws:
            week = int(match_ws.group(1))
            session = match_ws.group(2)

        return {"group": group, "subject": subject, "week": week, "session": session}

    for subject, entries in (zone_master or {}).items():
        if not entries:
            continue
        for entry in entries:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            file_path, metrics = entry
            meta = _parse_path(str(file_path))
            metrics = metrics or {}

            row = {
                "group": meta["group"],
                "subject": meta["subject"] or subject,
                "week": metrics.get("week", meta["week"]),
                "session": meta["session"],
                "time_in_allowed_s": metrics.get("time_in_allowed_s"),
                "time_above_s": metrics.get("time_above_s"),
                "time_below_s": metrics.get("time_below_s"),
                "longest_bounded_bout_s": metrics.get("longest_bounded_bout_s"),
                "bounded_met": metrics.get("bounded_met"),
                "mazd": metrics.get("mazd"),
                "trimp": metrics.get("trimp"),
            }
            rows.append(row)

    df_out = pd.DataFrame(rows, columns=[
        "group",
        "subject",
        "week",
        "session",
        "time_in_allowed_s",
        "time_above_s",
        "time_below_s",
        "longest_bounded_bout_s",
        "bounded_met",
        "mazd",
        "trimp",
    ])

    if not df_out.empty:
        df_out["week"] = pd.array(df_out["week"], dtype="Int64")
        numeric_cols = [
            "time_in_allowed_s",
            "time_above_s",
            "time_below_s",
            "longest_bounded_bout_s",
            "mazd",
            "trimp",
        ]
        for col in numeric_cols:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
        if "bounded_met" in df_out.columns:
            df_out["bounded_met"] = df_out["bounded_met"].astype("boolean")

        df_out.sort_values(
            by=["group", "subject", "week", "session"],
            inplace=True,
            kind="mergesort",
        )

    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False)
    log.info("Zone QC summary written: %s (%d rows)", out_csv, len(df_out))
    return df_out

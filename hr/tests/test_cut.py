import pandas as pd

from util.hr.cut_time import cut_time_series


def test_cut_time_series_trims_over_40_minutes():
    # build 45 minutes of 1-minute samples from 00:00:00 to 00:44:00
    times = pd.date_range("2024-01-01 00:00:00", periods=45, freq="1min").time
    df = pd.DataFrame({"time": times, "hr": range(len(times))})

    trimmed, cut_seconds, meta = cut_time_series(df, "time", 40)

    assert len(trimmed) == 41  # keep samples up to and including 40:00
    assert int(cut_seconds) == 240  # 4 minutes trimmed
    assert meta is not None
    assert meta["cut_start"].time().isoformat(timespec="minutes") == "00:40"
    assert meta["original_end"].time().isoformat(timespec="minutes") == "00:44"


def test_cut_time_series_no_trim_when_short():
    times = pd.date_range("2024-01-01 00:00:00", periods=10, freq="1min").time
    df = pd.DataFrame({"time": times, "hr": range(len(times))})

    trimmed, cut_seconds, meta = cut_time_series(df, "time", 40)

    assert len(trimmed) == len(df)
    assert cut_seconds == 0.0
    assert meta["cut_seconds"] == 0.0


# Improvement ideas for a human:
# - Add day-rollover scenarios (e.g., 23:55 -> 00:10) to ensure cut logic handles date jumps.
# - Property-based test varying sample cadence (seconds vs minutes) and cutoff minutes.
# - Validate that non-monotonic input time order is sorted before trimming.


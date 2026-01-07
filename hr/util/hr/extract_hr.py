import logging
import pandas as pd

logger = logging.getLogger(__name__)


def extract_hr(file):
    if not file:
        raise ValueError("File must be a non-empty path or list of paths.")

    file_list = [file] if isinstance(file, (str, bytes)) else list(file)
    if not file_list:
        raise ValueError("Files must be a non-empty list of file paths.")

    for path in file_list:
        if str(path).lower().endswith(".csv"):
            week = path.split('/')[-1].split('_')[3]
            df = pd.read_csv(path, skiprows=2)
            df = df[["Time", "HR (bpm)"]].rename(columns={"Time": "time", "HR (bpm)": "hr"})
            # Normalize invalid >=24:MM:SS to HH%24:MM:SS before parsing, and log when it occurs
            time_str = df["time"].astype(str).str.strip()
            parts = time_str.str.split(":", n=2, expand=True)
            if parts.shape[1] >= 3:
                hours = pd.to_numeric(parts[0], errors="coerce")
                bad_mask = hours >= 24
                if bad_mask.any():
                    sample = time_str[bad_mask].iloc[0]
                    logger.warning(
                        "Found %d time values with hour >= 24 in %s (sample %s); normalizing to HH%%24",
                        int(bad_mask.sum()),
                        path,
                        sample,
                    )
                    hours = hours.where(~bad_mask, hours % 24)
                    parts[0] = hours.fillna(0).astype(int).astype(str).str.zfill(2)
                    time_str = parts[0] + ":" + parts[1].str.zfill(2) + ":" + parts[2].str.zfill(2)
            df["time"] = pd.to_datetime(time_str, format="%H:%M:%S")
            return df, week


def recording_window(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timedelta] | None:
    if df is None or df.empty or "time" not in df.columns:
        return None
    times = df["time"].reset_index(drop=True)
    if times.empty:
        return None

    # Handle day rollovers by incrementing a day when time decreases.
    deltas = times.diff()
    day_offsets = (deltas < pd.Timedelta(0)).cumsum()
    adjusted = times + pd.to_timedelta(day_offsets, unit="D")

    start_time = adjusted.iloc[0]
    end_time = adjusted.iloc[-1]
    duration = end_time - start_time
    return start_time, end_time, duration

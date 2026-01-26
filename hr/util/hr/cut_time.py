import pandas as pd


def cut_time_series(
    data: pd.DataFrame, time_column: str, mins: int
    ) -> tuple[pd.DataFrame, float, dict | None]:
    """
    Trim a time series to the first `mins` minutes.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing a time column and any other columns (e.g., hr).
    time_column : str
        Column name holding time values; convertible to datetime.
    mins : int
        Maximum number of minutes to keep from the start of the series.

    Returns
    -------
    tuple[pd.DataFrame, float, dict | None]
        - The (possibly trimmed) DataFrame.
        - Seconds of data removed (0 if no trim performed).
        - Details dict with cut window metadata when trimming occurred.
    """
    if data is None or data.empty or time_column not in data.columns:
        return data, 0.0, None

    df = data.copy()
    df[time_column] = pd.to_datetime(df[time_column])
    df = df.sort_values(time_column).reset_index(drop=True)

    times = df[time_column]
    deltas = times.diff()
    # Handle potential day rollovers (e.g., 23:59 -> 00:01) by carrying forward days
    day_offsets = (deltas < pd.Timedelta(0)).cumsum()
    adjusted_times = times + pd.to_timedelta(day_offsets, unit="D")

    start_time = adjusted_times.iloc[0]
    end_time = adjusted_times.iloc[-1]
    duration = end_time - start_time
    allowed_duration = pd.Timedelta(minutes=mins)

    if pd.isna(duration) or duration <= allowed_duration:
        return df, 0.0, {
            "cut_start": None,
            "original_end": end_time,
            "cut_seconds": 0.0,
        }

    cutoff_time = start_time + allowed_duration
    keep_mask = adjusted_times <= cutoff_time
    trimmed_df = df.loc[keep_mask].copy()
    trimmed_df.reset_index(drop=True, inplace=True)

    cut_seconds = float((end_time - cutoff_time).total_seconds())
    details = {
        "cut_start": cutoff_time,
        "original_end": end_time,
        "cut_seconds": cut_seconds,
    }
    return trimmed_df, cut_seconds, details

import pandas as pd
def midpoint_snap(zones: pd.DataFrame, snap_to: int = 5) -> pd.DataFrame:
    """
    zones: 1-row DataFrame with columns
      z1_start, z1_end, z2_start, z2_end, …, z5_start, z5_end
    snap_to: round each midpoint to nearest multiple of this.
    """
    n = zones.shape[1] // 2
    # extract lists of ints
    starts = [int(zones[f"z{i+1}_start"].iat[0]) for i in range(n)]
    ends   = [int(zones[f"z{i+1}_end"].iat[0])   for i in range(n)]

    # compute snapped midpoints between each zone boundary
    mids = []
    for i in range(1, n):
        raw_mid = (ends[i-1] + starts[i]) / 2
        snapped = int(round(raw_mid / snap_to) * snap_to)
        mids.append(snapped)

    # build new starts/ends
    new_starts = [starts[0]] + [m + 1 for m in mids]
    new_ends   = mids + [ends[-1]]

    # assemble result
    out = {}
    for i in range(n):
        out[f"z{i+1}_start"] = new_starts[i]
        out[f"z{i+1}_end"]   = new_ends[i]

    return pd.DataFrame([out])

import pandas as pd
from util.zone.midpoint import midpoint_snap
import logging
logger = logging.getLogger(__name__)

def extract_zones(path, subject, snap_to=5):
    if subject.startswith('sub'):
        subject = subject.removeprefix('sub')

    # 1) Read in only the 5 zones for that subject
    df = pd.read_excel(path, sheet_name='Sheet1')
    zone_cols = df.columns[5:15].tolist()   # this is ['Zone 1…', 'Unnamed: 6', … 'Unnamed: 14']
    all_cols  = ['BOOST ID'] + zone_cols     # length = 1 + 10 = 11

    # 2) Filter rows AND pick columns in one go:
    sub = df.loc[df['BOOST ID'] == int(subject), all_cols]

    if sub.empty:
        raise ValueError(f"No rows matching ID {subject}")

    # 3) Now sub is a DataFrame with exactly 11 cols:
    row = sub.iloc[0]  # a Series of length 11

    # 4) If you really need a new DataFrame with renamed cols:
    new_names = {
        'BOOST ID': 'boost_id',
        zone_cols[0]: 'z1_start',
        zone_cols[1]: 'z1_end',
        zone_cols[2]: 'z2_start',
        zone_cols[3]: 'z2_end',
        zone_cols[4]: 'z3_start',
        zone_cols[5]: 'z3_end',
        zone_cols[6]: 'z4_start',
        zone_cols[7]: 'z4_end',
        zone_cols[8]: 'z5_start',
        zone_cols[-1]: 'z5_end',
    }

    zones = sub.rename(columns=new_names).reset_index(drop=True)
    return midpoint_snap(zones, snap_to=snap_to)

def extract_rest_max(path, subject):
    """
    Exists as:
        Max Hr
        Rest Hr

    In the sheet as solo columns.
    """

    if subject.startswith('sub'):
        subject = subject.removeprefix('sub')

    df = pd.read_excel(path, sheet_name='Sheet1')
    cols = {c.lower(): c for c in df.columns}
    try:
        boost_col = cols['boost id']
        rest_col = cols['rest hr']
        max_col = cols['max hr']
    except KeyError as exc:
        missing = {"boost id", "rest hr", "max hr"} - set(cols)
        raise ValueError(f"Missing required columns in HR range sheet: {sorted(missing)}") from exc

    sub = df.loc[df[boost_col] == int(subject), [boost_col, rest_col, max_col]]
    if sub.empty:
        raise ValueError(f"No rows matching ID {subject}")

    rest_hr = pd.to_numeric(sub[rest_col].iloc[0], errors="coerce")
    max_hr = pd.to_numeric(sub[max_col].iloc[0], errors="coerce")
    if pd.isna(rest_hr) or pd.isna(max_hr):
        raise ValueError(f"Rest or max HR missing for ID {subject}")

    return {"rest_hr": int(rest_hr), "max_hr": int(max_hr)}

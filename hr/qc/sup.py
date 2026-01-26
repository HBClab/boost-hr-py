import pandas as pd
import logging

from qc.zone.zone_qc import QC_Zone
from util.hr.cut_time import cut_time_series

logger = logging.getLogger(__name__)

class QC_Sup:

    def __init__(self, hr, zones, week, session_type: str, rest_max=None, rest_max_err: str | None = None):
        self.hr = hr
        self.zones = zones
        self.week = week
        self.err = {}
        if rest_max_err:
            self.err["rest_max"] = [rest_max_err, None]
        self.rest_max = rest_max or {}
        self.session_type = session_type.lower()
        self.zone_metrics = None

    def main(self):
        self.qc_data()
        self.zone_metrics = self.qc_zones()

        return self.err, self.zone_metrics

    def qc_data(self):
        """
        QC the raw data itself
        """
        logger.debug("running duration trim check")
        trimmed, cut_seconds, cut_meta = cut_time_series(self.hr, "time", 40)
        self.hr = trimmed
        if cut_seconds > 0 and cut_meta is not None:
            self.err["duration_trimmed"] = [
                f"time series longer than 40 minutes; trimmed {int(cut_seconds)} seconds",
                pd.DataFrame(
                    {
                        "start_time": [cut_meta.get("cut_start")],
                        "end_time": [cut_meta.get("original_end")],
                        "duration": [pd.Timedelta(seconds=cut_seconds)],
                    }
                ),
            ]
        
        logger.debug("running missing check")
        missing_check, missing_periods = self._missing_periods()
        nan_runs = self._nan_check(self.hr.copy())
        if missing_check == 1:
            self.err['missing'] = ['missing significant time', missing_periods]
        elif not nan_runs.empty:
            self.err['nan'] = ['more than 30 NaNs in a row', nan_runs]
        else: 
            return None

    def qc_zones(self):
        """
        Run the qc_zone class
        This should return the errors found in zone qc for reporting
        """
        logger.debug("running phantom zone qc")

        qc_zone = QC_Zone(self.hr, self.zones, self.week, self.rest_max)
        if self.session_type.startswith("super"):
            qc_zone.supervised()
        else:
            qc_zone.unsupervised()

        # Merge any zone errors into the overall error dictionary
        self.err.update(qc_zone.err)
        return qc_zone.zone_metrics



    def _missing_periods(self):

        df = self.hr.copy()

        # assume df has columns “time” and “hr”
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
        df = df.sort_values('time')

        # drop any NaNs so we only look at real measurements
        valid = df.dropna(subset=['hr'])

        # compute time‐diff between successive valid samples
        delta = valid['time'].diff()

        # mask where that gap exceeds 30 s
        gaps = delta > pd.Timedelta(seconds=30)

        # build a table of missing‐data intervals
        prev_time = valid['time'].shift()
        missing_periods = pd.DataFrame({
            'gap_start': prev_time[gaps],    # end of last good sample
            'gap_end':   valid['time'][gaps] # start of next good sample
        })
        missing_periods['duration'] = missing_periods['gap_end'] - missing_periods['gap_start']
        if missing_periods.empty:
            return 0, missing_periods
        else:
            return 1, missing_periods


    def _nan_check(self, df: pd.DataFrame, min_run: int = 30) -> pd.DataFrame:
        """
        Detect runs of > min_run consecutive NaNs in df['hr'].
        Returns a DataFrame with columns: [start_time, end_time, length].
        """
        # 1) Ensure time is datetime and sorted
        df = df.copy()
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time').reset_index(drop=True)

        # 2) Boolean mask of where hr is NaN
        is_nan = df['hr'].isna()

        # 3) Build run‐IDs by marking where the mask changes
        run_id = is_nan.ne(is_nan.shift()).cumsum()

        # 4) Aggregate each run
        summary = (
            df
            .assign(is_nan=is_nan, run=run_id)
            .groupby('run')
            .agg(
                start_time=('time', 'first'),
                end_time  =('time', 'last'),
                length    =('is_nan', 'size'),
                all_nan   =('is_nan', 'all')
            )
        )

        # 5) Filter to runs that are all-NaN and longer than min_run
        long_runs = summary[(summary['all_nan']) & (summary['length'] > min_run)]

        # 6) Add duration and return start/end/length/duration
        long_runs = long_runs.copy()
        long_runs['duration'] = long_runs['end_time'] - long_runs['start_time']
        return long_runs[['start_time', 'end_time', 'duration', 'length']]

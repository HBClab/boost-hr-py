import re
import os
import logging
import pandas as pd
from typing import Dict, List

logger = logging.getLogger(__name__)

_SES_RE = re.compile(r"_ses(\d+)\.csv$", re.IGNORECASE)

def _max_session(dir_path: str) -> int:
    """
    Scan a directory and return the largest session number from files
    named like '*_wkXX_sesNN.CSV'. Returns 0 if none found.
    """
    try:
        return max(
            (int(m.group(1)) for fn in os.listdir(dir_path)
             if (m := _SES_RE.search(fn)) is not None),
            default=0,
        )
    except FileNotFoundError:
        return 0

class Get_Data:
    """
    Build a dataset for OLS/WLS:
      - sup_prop  = (# supervised CSVs) / 30
      - unsup_den = (# unsupervised CSVs actually observed; <= 30)
      - unsup_prop= (# unsupervised CSVs) / max(unsup_den, 1)
    Notes:
      - We treat each *.csv file as a completed session.
      - If you prefer unsupervised adherence out of 30 planned, add a column:
          unsup_prop_30 = unsup_n / 30.0
    """

    def __init__(self, sup_path: str, unsup_path: str, study: str = "InterventionStudy"):
        self.sup_path = sup_path
        self.unsup_path = unsup_path
        self.study = study
        self.master = pd.DataFrame()

    @staticmethod
    def _list_subjects(path: str) -> List[str]:
        return [
            d for d in os.listdir(path)
            if not d.startswith(".") and os.path.isdir(os.path.join(path, d))
        ]

    @staticmethod
    def _count_csvs(path: str) -> int:
        try:
            return sum(
                1 for f in os.listdir(path)
                if f.lower().endswith(".csv") and not f.startswith(".")
            )
        except FileNotFoundError:
            return 0

    def get_meta(self) -> Dict:
        """
        Count how many subjects have session 30 present (by filename containing '_ses30')
        and how many sessions are missing from 30 in each folder.
        """
        meta = {
            "sup": {"ses30_count": 0, "total_missing": 0, "subjects_complete": []},
            "unsup": {"ses30_count": 0, "total_missing": 0, "subjects_complete": []}
        }

        for study_path, label in [(self.sup_path, "sup"), (self.unsup_path, "unsup")]:
            for subject in self._list_subjects(study_path):
                subject_path = os.path.join(study_path, subject)
                files = [
                    f for f in os.listdir(subject_path)
                    if f.lower().endswith(".csv") and not f.startswith(".")
                ]

                # Session 30 present?
                if any("_ses30" in f.lower() for f in files):
                    meta[label]["ses30_count"] += 1
                    meta[label]["subjects_complete"].append(subject)

                # Missing (from 30 planned)
                meta[label]["total_missing"] += max(0, 30 - len(files))

        return meta

    def build_master_df(self) -> pd.DataFrame:
        """
        Create one row per subject with:
          subject, sup_n, sup_prop, sup_den, unsup_n, unsup_den, unsup_prop, unsup_prop_30
        - 'den' for each side is the largest session index seen in filenames.
        - Skip subjects with fewer than 6 unsupervised sessions.
        """
        sup_subjects = set(self._list_subjects(self.sup_path))
        unsup_subjects = set(self._list_subjects(self.unsup_path))
        subjects = sorted(sup_subjects | unsup_subjects)

        rows = []
        for subj in subjects:
            sup_dir = os.path.join(self.sup_path, subj)
            unsup_dir = os.path.join(self.unsup_path, subj)

            sup_n = self._count_csvs(sup_dir)
            unsup_n = self._count_csvs(unsup_dir)

            # Denominator = max session index observed in filenames
            sup_den = _max_session(sup_dir)
            unsup_den = _max_session(unsup_dir)

            logger.debug(
                f"Subject {subj}: sup_n={sup_n}, sup_den={sup_den}, "
                f"unsup_n={unsup_n}, unsup_den={unsup_den}"
            )

            # Skip if fewer than 6 unsupervised sessions
            if unsup_n < 6:
                continue

            # Guard against zero denominators: if no session index found,
            # fall back to n (prevents div-by-zero but keeps proportion meaningful)
            sup_den_eff = sup_den if sup_den > 0 else max(sup_n, 1)
            unsup_den_eff = unsup_den if unsup_den > 0 else max(unsup_n, 1)

            sup_prop = sup_n / float(sup_den_eff)
            unsup_prop = unsup_n / float(unsup_den_eff)
            unsup_prop_30 = unsup_n / 30.0  # keep for reference/comparison if you still want it

            rows.append({
                "subject": subj,
                "sup_n": sup_n,
                "sup_den": sup_den,          # raw parsed max session
                "sup_prop": sup_prop,
                "unsup_n": unsup_n,
                "unsup_den": unsup_den,      # raw parsed max session
                "unsup_prop": unsup_prop,    # used by Rust CLI as y
                "unsup_prop_30": unsup_prop_30,
            })

        self.master = pd.DataFrame(rows)
        return self.master

    def save_for_rust(self, out_csv: str = "data.csv") -> str:
        """
        Save the minimal schema the Rust CLI expects:
          sup_prop (x), unsup_prop (y), unsup_den (m)
        """
        if getattr(self, "master", None) is None or self.master.empty:
            self.build_master_df()
        df = self.master[["sup_prop", "unsup_prop", "unsup_den"]].copy()
        df.to_csv(out_csv, index=False)
        return out_csv
'''
from main import Main
from plot.get_data import Get_Data
import os
main = Main(system="Home")
path = os.path.join(main.base_path, "InterventionStudy", "3-Experiment", "data", "polarhrcsv")
gd = Get_Data(sup_path=os.path.join(path, "Supervised"), unsup_path=os.path.join(path, "Unsupervised"), study="InterventionStudy")
df_master = gd.build_master_df()
gd.save_for_rust("../rust-ols-adherence-cli/data.csv")


'''

import os
import sys
import logging
from pathlib import Path
import pandas as pd

class Main:

    def __init__(self, system):
        import os

        # Set the base path dependent on system
        if system is None:
            raise ValueError("System cannot be None")
        if system == "Argon":
            self.base_path = "/Shared/vosslabhpc/Projects/BOOST/"
        elif system == "Home":
            self.base_path = "/mnt/lss/Projects/BOOST/"
        elif system == "vosslnx":
            self.base_path = "/mnt/nfs/lss/vosslabhpc/Projects/BOOST/"
        else:
            raise ValueError(f"Unknown system: {system}")

        # Ensure base_path is an absolute path and exists
        self.base_path = os.path.abspath(self.base_path)
        if not os.path.isdir(self.base_path):
            raise FileNotFoundError(f"Base path does not exist: {self.base_path}")

        # add zone path to class 
        self.zone_path = os.path.join(
            self.base_path,
            "InterventionStudy/1-projectManagement/participants/ExerciseSessionMaterials/Intervention Materials/BOOST HR ranges.xlsx"
        )
        if not os.path.isfile(self.zone_path):
            raise FileNotFoundError(f"Zone path does not exist: {self.zone_path}")

        self.out_path = "../qc_out.csv"
        self.zone_out_path = "../zone_out.csv"


        # add logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("main.log", mode="w", encoding="utf-8"), # mode = w will allow for logging to NOT APPEND - then we don't have crazy logs
                logging.StreamHandler()
            ]
        )


    def main(self):
        """
        Main function to run the script.
        """
        err_master = {} # dict to hold all errors
        zone_master = {} # dict to hold all zone metrics
        from util.get_files import get_files
        from util.hr.extract_hr import extract_hr, recording_window
        from util.zone.extract_zones import extract_zones
        from qc.sup import QC_Sup
        project_path = os.path.join(self.base_path, "InterventionStudy", "3-experiment", "data", "polarhrcsv")
        if os.path.exists(project_path):
            for session in ["Supervised", "Unsupervised"]:
                session_path = os.path.join(project_path, session)
                logging.debug(f"Processing session: {session_path}")
                if os.path.exists(session_path):
                    # return the files dict that contains base_path and list of files for each base_path
                    files = get_files(session_path)
                    # extract hr from each file
                    for subject, subject_files in files.items():
                        for file in subject_files:
                            if file.lower().endswith('.csv'):
                                hr, week = extract_hr(file)
                                if hr is None or week is None:
                                    logging.warning("Skipping file with unparseable week: %s", file)
                                    err = {"week_parse": ["could not parse week from filename; file skipped", None]}
                                    if subject not in err_master:
                                        err_master[subject] = [[file, err]]
                                    else:
                                        err_master[subject].append([file, err])
                                    continue
                                window = recording_window(hr)
                                if window is not None:
                                    start_time, end_time, duration = window
                                    if duration > pd.Timedelta(hours=4):
                                        logging.warning(
                                            "Skipping file with long duration (%s): %s",
                                            duration,
                                            file,
                                        )
                                        err = {
                                            "duration": [
                                                "recording longer than 4 hours; file ignored",
                                                pd.DataFrame({
                                                    "start_time": [start_time],
                                                    "end_time": [end_time],
                                                    "duration": [duration],
                                                }),
                                            ]
                                        }
                                        if subject not in err_master:
                                            err_master[subject] = [[file, err]]
                                        else:
                                            err_master[subject].append([file, err])
                                        continue
                                zones = extract_zones(self.zone_path, subject)
                                err, zone_metrics = QC_Sup(hr, zones, week, session).main()

                                if subject not in err_master:
                                    # first time: create a list with this one error
                                    err_master[subject] = [[file,err]]
                                else:
                                    # append to the existing list
                                    err_master[subject].append([file,err])
                                if zone_metrics is not None:
                                    if subject not in zone_master:
                                        zone_master[subject] = [[file, zone_metrics]]
                                    else:
                                        zone_master[subject].append([file, zone_metrics])
        err_master = {
            subject: [e for e in errs if e]
            for subject, errs in err_master.items()
        }
        from qc.save_qc import save_qc
        save_qc(err_master, self.out_path)
        from qc.zone.save_zones import save_zones
        save_zones(zone_master, self.zone_out_path)
        from plot.get_data import Get_Data
        path = os.path.join(self.base_path, "InterventionStudy", "3-Experiment", "data", "polarhrcsv")
        gd = Get_Data(sup_path=os.path.join(path, "Supervised"), unsup_path=os.path.join(path, "Unsupervised"), study="InterventionStudy")
        meta = gd.get_meta()
        df_master = gd.build_master_df()
        #gd.save_for_rust("./rust-ols-adherence-cli/data.csv")



        return err_master


if __name__ == '__main__':
    if sys.argv[1]:
        if sys.argv[1] in ["Argon", "Home", "vosslnx"]:
            Main(system=sys.argv[1]).main()
        else:
            raise ValueError("""First Argument is not one of the desired systems: 
            The argument must be one of the following:
            vosslnx = the vosslab linux machine used for automation
            Argon = the Argon HPC
            Home = My (Zak) personal linux machine mount
            """)
    else:
        raise ValueError("""First Argument does not exist.
        The argument must be one of the following:
        vosslnx = the vosslab linux machine used for automation
        Argon = the Argon HPC
        Home = My (Zak) personal linux machine mount
        """)




        

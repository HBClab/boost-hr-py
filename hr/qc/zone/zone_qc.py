import logging

import pandas as pd

logging = logging.getLogger(__name__)


class QC_Zone:

    def __init__(self, hr, zones):
        self.hr = hr
        self.zones = zones
        self.err = {}

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


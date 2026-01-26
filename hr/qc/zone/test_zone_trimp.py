import math

import pandas as pd
import pytest

try:  # Allow running with repo root on PYTHONPATH or hr/ on PYTHONPATH
    from hr.qc.zone.zone_qc import QC_Zone
except ImportError:  # pragma: no cover
    from qc.zone.zone_qc import QC_Zone


def test_calc_trimp_basic():
    hr_vals = pd.Series([120, 150, 160])
    deltas = pd.Series([60.0, 60.0, 60.0])  # seconds
    qc = QC_Zone(hr=None, zones=None, week=1, rest_max={"rest_hr": 60, "max_hr": 180})

    trimp, err = qc._calc_trimp(hr_vals, deltas, qc.rest_hr, qc.max_hr)

    # Expected from Banister-Edwards formula with rest=60, max=180
    # intensity = (hr-rest)/(max-rest); weight = 0.64 * exp(1.92*intensity)
    # TRIMP = sum(duration_minutes * intensity * weight)
    expected = 5.503294156664471
    assert err is None
    assert trimp == pytest.approx(expected, rel=1e-6)


def test_calc_trimp_missing_rest_max():
    hr_vals = pd.Series([120, 130])
    deltas = pd.Series([60.0, 60.0])
    qc = QC_Zone(hr=None, zones=None, week=1, rest_max={"rest_hr": None, "max_hr": 180})

    trimp, err = qc._calc_trimp(hr_vals, deltas, qc.rest_hr, qc.max_hr)

    assert trimp is None
    assert "resting or max HR missing" in err


def test_calc_trimp_supervised_cap():
    hr_vals = pd.Series([150] * 60)  # 60 minutes of data
    deltas = pd.Series([60.0] * 60)
    qc = QC_Zone(hr=None, zones=None, week=1, rest_max={"rest_hr": 60, "max_hr": 180})

    # Simulate supervised cutoff by passing only the first 45 minutes
    trimp, err = qc._calc_trimp(hr_vals.iloc[:45], deltas.iloc[:45], qc.rest_hr, qc.max_hr)

    intensity = (150 - 60) / 120  # 0.75
    weight = 0.64 * math.exp(1.92 * intensity)
    expected_per_minute = intensity * weight
    expected_trimp = expected_per_minute * 45

    assert err is None
    assert trimp == pytest.approx(expected_trimp, rel=1e-6)

"""
One-off fix (2026-09-28):
  1. Drop synthetic incidents dated after TODAY (Divya's generator ran through Dec 2026).
  2. Fill river readings for incidents after 2026-07-12, where the original one-time USGS pull ended.
     Source: richmond_discharge_2021_2026_ext.csv = the original USGS daily values, extended with
     daily means of the 15-minute readings the Richmond cron committed to the data/ repo every day.
     Checked against the 12 days where both exist (Jul 1-12): within 0.6% of USGS's own daily value.
Deltas and flags are recomputed with the same function and thresholds as build_historical_incidents.py.
Originals are in _pre_datefix/.
"""
import sys
import pandas as pd

sys.path.insert(0, "../pipeline")
from join_features import build_environmental_features
from build_historical_incidents import (DISCHARGE_RAPID_RISE_CFS, DISCHARGE_CALM_LOCAL_MAX_CFS,
                                        DISCHARGE_CALM_DELTA_MIN_CFS)

TODAY = "2026-09-28"
env = build_environmental_features(pd.read_csv("richmond_discharge_2021_2026_ext.csv")[["date", "discharge_cfs"]],
                                   level_col="discharge_cfs",
                                   rapid_rise_threshold=DISCHARGE_RAPID_RISE_CFS,
                                   deceptive_calm_local_max=DISCHARGE_CALM_LOCAL_MAX_CFS,
                                   deceptive_calm_delta_min=DISCHARGE_CALM_DELTA_MIN_CFS)
env["date"] = env.date.dt.strftime("%Y-%m-%d")
env = env.set_index("date").rename(columns={
    "delta_24h": "delta_discharge_24h_cfs", "delta_72h": "delta_discharge_72h_cfs",
    "rapid_rise_flag": "rapid_rise_flag_discharge", "deceptive_calm_flag": "deceptive_calm_flag_discharge"})
cols = ["discharge_cfs", "delta_discharge_24h_cfs", "delta_discharge_72h_cfs",
        "rapid_rise_flag_discharge", "deceptive_calm_flag_discharge"]

keep = None
for f in ["historical_incidents_clean.csv", "incidents_with_narratives.csv", "incidents_with_narratives_llm.csv"]:
    df = pd.read_csv(f)
    k = (df.incident_date <= TODAY).values
    keep = k if keep is None else keep
    assert (k == keep).all(), f
    df = df[k].reset_index(drop=True)
    miss = df.discharge_cfs.isna()
    for c in cols:
        df.loc[miss, c] = df.loc[miss, "incident_date"].map(env[c])
    df.to_csv(f, index=False)
    print(f"{f}: dropped {(~k).sum()} future rows, filled {miss.sum()}, still missing {df.discharge_cfs.isna().sum()}")

# the LLM parser's results are row-aligned with incidents_with_narratives_llm.csv
llm = pd.read_csv("llm_mainllm_results.csv")
llm[keep].reset_index(drop=True).to_csv("llm_mainllm_results.csv", index=False)

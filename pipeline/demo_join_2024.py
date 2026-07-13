"""
Demo/proof of the join+feature pipeline using REAL 2024 USGS discharge
history (pulled live from NWIS on 2026-07-13, saved in
usgs_2024_discharge_raw.json) joined against Divya's synthetic 2024
incidents -- with the synthetic file's own environmental columns STRIPPED
first, so the join and features are computed independently and then
compared as a sanity check.

Why 2024: the synthetic dataset models 2024 as the "bad year" (5 fatalities,
93 incidents) per the project plan, so it's the most interesting single-year
test case.

Why discharge and not gage height: real historical gage height requires a
chunked iv backfill (15-min cadence, ~35k rows/year) that wasn't pulled in
full for this session -- only a few days were sampled to confirm the
approach works (see usgs_history.py docstring). Full daily discharge history
IS available in one call and was pulled for real. This demo is therefore a
proof of the JOIN MECHANICS, not a claim that discharge-derived flags equal
what a real gage-height-based risk index would say.
"""

import json
import pandas as pd

from join_features import build_environmental_features, join_incidents

# --- load real USGS 2024 discharge (pulled live via web fetch, saved raw) ---
with open("usgs_2024_discharge_raw.json") as f:
    raw = json.load(f)

records = raw["value"]["timeSeries"][0]["values"][0]["value"]
env_daily = pd.DataFrame(
    {
        "date": [r["dateTime"][:10] for r in records],
        "gage_height_ft": [float(r["value"]) for r in records],  # actually discharge_cfs; see docstring
    }
)
print(f"Real USGS 2024 daily discharge series: {len(env_daily)} days "
      f"({env_daily['date'].min()} to {env_daily['date'].max()})")

env_features = build_environmental_features(
    env_daily,
    rapid_rise_threshold=1500,       # cfs, not ft -- placeholder for demo purposes
    deceptive_calm_local_max=3000,   # cfs
    deceptive_calm_delta_min=1000,   # cfs
)

# --- load synthetic 2024 incidents, strip pre-baked environmental columns ---
synthetic = pd.read_csv("synthetic_rfd_james_river_incidents_unified_2021_2026.csv")
synthetic_2024 = synthetic[synthetic["year"] == 2024].copy()

env_cols_to_strip = [
    "gage_height_ft", "discharge_cfs", "water_temp_c", "air_temp_f",
    "precip_24h_in", "wind_mph", "upstream_gage_ft", "delta_gage_24h_ft",
    "delta_gage_72h_ft", "rapid_rise_flag", "deceptive_calm_flag",
    "risk_index_1_10", "public_risk_level",
]
bare_incidents = synthetic_2024.drop(columns=[c for c in env_cols_to_strip if c in synthetic_2024.columns])

joined = join_incidents(bare_incidents, env_features)

print(f"\nJoined {len(joined)} 2024 incidents to real discharge history.")
print(joined[["incident_id", "incident_date", "gage_height_ft", "delta_24h",
              "rapid_rise_flag", "response_time_min"]].head(10).to_string(index=False))

# --- sanity check: does OUR rapid_rise_flag (from real discharge) agree with
# Divya's synthetic rapid_rise_flag (from her synthetic gage height)? ---
comparison = synthetic_2024[["incident_id", "incident_class", "rapid_rise_flag"]].merge(
    joined[["incident_id", "rapid_rise_flag"]], on="incident_id", suffixes=("_synthetic", "_real_discharge")
)
agreement = (comparison["rapid_rise_flag_synthetic"] == comparison["rapid_rise_flag_real_discharge"]).mean()
print(f"\nAgreement between synthetic rapid_rise_flag and real-discharge-derived "
      f"rapid_rise_flag on the same 2024 incident dates: {agreement:.0%}")
print("(Not expected to be 1.0 or even close -- different variables (gage height vs "
      "discharge) and placeholder thresholds. This is a pipeline mechanics check, "
      "not a model validation.)")

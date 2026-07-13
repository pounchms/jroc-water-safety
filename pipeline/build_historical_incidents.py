"""
Builds the clean "Historical Incidents" deliverable: Divya's synthetic
incident STRUCTURE (real NFIRS/NERIS fields, contributed by her) rejoined
against REAL USGS discharge history 2021-2026, instead of her synthetic
environmental/risk columns (which are circular by construction -- see
risk_index.py docstring and jroc-synthetic-data-package memory).

Inputs:
  - synthetic_rfd_james_river_incidents_unified_2021_2026.csv (Divya's file --
    used ONLY for incident structure/fields; every environmental, risk, and
    synthetic-join-metadata column is stripped before use)
  - richmond_discharge_2021_2026.csv (real USGS daily discharge, station
    02037500, pulled live 2026-07-13 -- see richmond_discharge_2021_2026.csv
    header comment / this script for the exact NWIS query pattern)

Output:
  - historical_incidents_clean.csv -- ready to import into the "Historical
    Incidents" Google Sheet tab per setup_google_sheets.md

IMPORTANT CAVEATS (read before trusting this file):

1. DISCHARGE, NOT GAGE HEIGHT. Real historical gage-height (ft) data isn't
   available in bulk for station 02037500 via the daily-values (dv) service
   -- only discharge (cfs) has full multi-year daily history in one call.
   Gage height would require a slow, chunked instantaneous-values (iv)
   backfill (see usgs_history.backfill_daily_gage_height). So every derived
   column here (delta_discharge_24h_cfs, rapid_rise_flag_discharge, etc.) is
   discharge-based, with placeholder cfs thresholds (1500/3000/1000) that
   have NOT been validated against real incident outcomes -- they mirror the
   demo_join_2024.py thresholds, chosen to be roughly proportional to
   Richmond's typical flow range, nothing more.

2. risk_index.py WAS DELIBERATELY NOT RUN on this output. risk_index.score()
   expects gage_height_ft with breakpoints calibrated in feet (4/8/14). Feeding
   it discharge_cfs values (which run into the thousands) would silently
   misfire every threshold and mislabel nearly every day "Flood" -- a
   plausible-looking but meaningless result. Don't run risk_index against
   this file until real gage-height history is backfilled.

3. COVERAGE: 478 of 525 synthetic incidents get a real discharge match
   (2021-01-01 through 2026-07-12). The remaining 47 are synthetic incidents
   dated after 2026-07-12 (Divya's file extends speculative incidents through
   Dec 2026) -- those rows have discharge_cfs = NaN, which is correct: that
   real data doesn't exist yet.

4. This is real discharge joined to SYNTHETIC incidents -- the incident
   facts (date, location, injuries, response time) are still fabricated by
   Divya's generator, not RFD records. Only the environmental side is real.
   Swap in real RFD incidents through this same join_incidents() call the
   day the MOU clears -- no rework needed.
"""

import pandas as pd
from join_features import build_environmental_features, join_incidents

ENV_COLS_TO_STRIP = [
    "gage_height_ft", "discharge_cfs", "water_temp_c", "air_temp_f",
    "precip_24h_in", "wind_mph", "upstream_gage_ft", "delta_gage_24h_ft",
    "delta_gage_72h_ft", "rapid_rise_flag", "deceptive_calm_flag",
    "risk_index_1_10", "public_risk_level",
    "usgs_station", "nws_station", "data_join_status",
    "response_time_min", "incident_duration_min",  # recomputed fresh below
]

DISCHARGE_RAPID_RISE_CFS = 1500
DISCHARGE_CALM_LOCAL_MAX_CFS = 3000
DISCHARGE_CALM_DELTA_MIN_CFS = 1000


def build():
    synthetic = pd.read_csv("synthetic_rfd_james_river_incidents_unified_2021_2026.csv")
    bare = synthetic.drop(columns=[c for c in ENV_COLS_TO_STRIP if c in synthetic.columns])

    discharge = pd.read_csv("richmond_discharge_2021_2026.csv")
    env_features = build_environmental_features(
        discharge,
        level_col="discharge_cfs",
        rapid_rise_threshold=DISCHARGE_RAPID_RISE_CFS,
        deceptive_calm_local_max=DISCHARGE_CALM_LOCAL_MAX_CFS,
        deceptive_calm_delta_min=DISCHARGE_CALM_DELTA_MIN_CFS,
    ).rename(columns={
        "delta_24h": "delta_discharge_24h_cfs",
        "delta_72h": "delta_discharge_72h_cfs",
        "rapid_rise_flag": "rapid_rise_flag_discharge",
        "deceptive_calm_flag": "deceptive_calm_flag_discharge",
    })

    joined = join_incidents(bare, env_features).drop(columns=["date"])
    joined.to_csv("historical_incidents_clean.csv", index=False)

    coverage = joined["discharge_cfs"].notna().sum()
    print(f"Built historical_incidents_clean.csv: {len(joined)} rows, "
          f"{coverage} with real discharge match ({coverage/len(joined):.0%}).")
    return joined


if __name__ == "__main__":
    build()

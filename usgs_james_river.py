"""
USGS James River Gauge Data Fetcher
Station: 02037500 - James River Near Richmond, VA
Pulls gage height (ft) and discharge (cfs) for the past 30 days

Outputs:
  - usgs_james_river_conditions.csv (local backup)
  - Google Sheet tab "USGS Live Data" (Looker Studio source)

Google Sheets write requires GOOGLE_CREDENTIALS (service account JSON)
and GOOGLE_SHEET_ID env vars. See setup_google_sheets.md.

Risk scoring: this script writes TWO risk columns side by side.
  - risk_level: the original simple gage-height-only bucket (kept for
    backward compatibility with anything already reading it).
  - risk_index_1_10 / risk_index_level / risk_index_reasons: the fuller,
    rule-based score from pipeline/risk_index.py, using this script's own
    15-min series to detect rapid-rise / deceptive-calm (the "looks calm
    but is rising fast" danger pattern the project is built around).
    NOT YET INCLUDED: the weather and upstream-gauge bonuses. Those live
    in separate Sheet tabs ("Weather Live", "Upstream Live") written by
    nws_weather.py / usgs_upstream.py on their own schedules -- combining
    them here would mean reading those tabs back in, not just this
    script's own API pull. Tracked as a deliberate follow-up, not an
    oversight -- see JROC_Handoff_Guide.md.
"""

import json
import os
import sys

import pandas as pd
import requests
from datetime import datetime

# repo root (this file's directory) is already on sys.path when run as
# `python usgs_james_river.py` from the repo root, same as the workflow does --
# this insert() is just a safety net if it's ever run from elsewhere.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline.risk_index import RiskInputs, score as compute_risk_index, public_risk_level
from pipeline.join_features import (
    RAPID_RISE_THRESHOLD_FT,
    DECEPTIVE_CALM_LOCAL_MAX_FT,
    DECEPTIVE_CALM_DELTA_MIN_FT,
)

STATION = "02037500"
PERIOD = "P30D"  # Last 30 days — change to P7D for 7 days, P365D for a year
OUTPUT_FILE = "usgs_james_river_conditions.csv"  # relative -- lands next to whatever runs this (repo root in CI)
SHEET_TAB = "USGS Live Data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

API_URL = (
    f"https://waterservices.usgs.gov/nwis/iv/"
    f"?sites={STATION}"
    f"&parameterCd=00065,00060"
    f"&format=json"
    f"&period={PERIOD}"
)

def get_risk_level(gage_height):
    """Classify risk based on gage height thresholds derived from incident data."""
    if gage_height is None:
        return "Unknown"
    if gage_height >= 14:
        return "Flood"
    if gage_height >= 8:
        return "High"
    if gage_height >= 4:
        return "Moderate"
    return "Low"


def add_risk_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the fuller risk_index_1_10 score (pipeline/risk_index.py) alongside
    the existing simple risk_level, computed from THIS script's own ~15-min
    series (not day-indexed like join_features.build_environmental_features,
    since this data is finer-grained than daily).

    For each row, looks up the reading closest to 24h earlier (within a
    30-minute tolerance) and derives delta_24h, then rapid_rise_flag /
    deceptive_calm_flag using the same thresholds as join_features.py, so a
    single source of truth exists for what counts as "rapid" or "deceptive."
    Rows in the first 24h of the fetched window (no 24h-ago reading available)
    get both flags forced False rather than a wrong/missing delta.
    """
    d = df.copy()
    d["dt"] = pd.to_datetime(d["datetime"])
    d = d.sort_values("dt").reset_index(drop=True)

    # shift a copy of the series forward 24h so an asof-nearest match against
    # the current timestamp effectively finds "the reading from ~24h ago"
    lookup = d[["dt", "gage_height_ft"]].rename(columns={"gage_height_ft": "gage_24h_ago"})
    lookup["dt"] = lookup["dt"] + pd.Timedelta(hours=24)
    matched = pd.merge_asof(
        d[["dt"]], lookup.sort_values("dt"), on="dt",
        direction="nearest", tolerance=pd.Timedelta(minutes=30),
    )
    d["gage_24h_ago"] = matched["gage_24h_ago"]
    d["delta_24h"] = (d["gage_height_ft"] - d["gage_24h_ago"]).round(2)

    d["rapid_rise_flag"] = d["delta_24h"] >= RAPID_RISE_THRESHOLD_FT
    d["deceptive_calm_flag"] = (
        (d["gage_height_ft"] <= DECEPTIVE_CALM_LOCAL_MAX_FT)
        & (d["delta_24h"] >= DECEPTIVE_CALM_DELTA_MIN_FT)
    )
    no_match = d["gage_24h_ago"].isna()
    d.loc[no_match, "rapid_rise_flag"] = False
    d.loc[no_match, "deceptive_calm_flag"] = False

    scores, levels, reasons = [], [], []
    for _, row in d.iterrows():
        result = compute_risk_index(RiskInputs(
            gage_height_ft=row["gage_height_ft"],
            rapid_rise_flag=bool(row["rapid_rise_flag"]),
            deceptive_calm_flag=bool(row["deceptive_calm_flag"]),
        ))
        scores.append(result["risk_index_1_10"])
        levels.append(public_risk_level(result["risk_index_1_10"]))
        reasons.append("; ".join(result["reasons"]))

    d["risk_index_1_10"] = scores
    d["risk_index_level"] = levels
    d["risk_index_reasons"] = reasons

    return d.drop(columns=["dt", "gage_24h_ago"])

def fetch_usgs_data():
    print(f"Fetching USGS data for station {STATION}...")

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    series = data["value"]["timeSeries"]

    gage_series = next(
        (s for s in series if s["variable"]["variableCode"][0]["value"] == "00065"), None
    )
    discharge_series = next(
        (s for s in series if s["variable"]["variableCode"][0]["value"] == "00060"), None
    )

    if not gage_series:
        raise ValueError("Gage height data not found in API response")

    gage_records = gage_series["values"][0]["value"]
    discharge_lookup = {}
    if discharge_series:
        for rec in discharge_series["values"][0]["value"]:
            discharge_lookup[rec["dateTime"]] = (
                float(rec["value"]) if rec["value"] != "-999999" else None
            )

    rows = []
    for rec in gage_records:
        if rec["value"] == "-999999":
            continue

        dt = datetime.fromisoformat(rec["dateTime"].replace("Z", "+00:00"))
        gage_ht = float(rec["value"])
        discharge = discharge_lookup.get(rec["dateTime"])

        rows.append({
            "datetime": dt.strftime("%Y-%m-%d %H:%M"),
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "gage_height_ft": round(gage_ht, 2),
            "discharge_cfs": round(discharge, 0) if discharge else None,
            "risk_level": get_risk_level(gage_ht),
            "station": STATION,
            "station_name": "James River Near Richmond, VA",
        })

    df = pd.DataFrame(rows)
    df = add_risk_index_columns(df)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"CSV written: {OUTPUT_FILE} ({len(rows)} records)")

    write_to_google_sheets(df)

    latest = df.iloc[-1]
    print(f"\nLatest reading: {latest['datetime']}")
    print(f"  Gage height:     {latest['gage_height_ft']} ft")
    print(f"  Discharge:       {latest['discharge_cfs']} cfs")
    print(f"  Risk level:      {latest['risk_level']} (simple, gage-only)")
    print(f"  Risk index 1-10: {latest['risk_index_1_10']} ({latest['risk_index_level']})")
    print(f"  Why:             {latest['risk_index_reasons']}")


def write_to_google_sheets(df: pd.DataFrame):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")

    if not creds_json or not sheet_id:
        print("GOOGLE_CREDENTIALS or GOOGLE_SHEET_ID not set — skipping Sheets write.")
        return

    # imported here, not at module level, so the script still runs (and still
    # writes the local CSV) on a machine that hasn't pip-installed gspread/
    # google-auth -- only needed once you're actually ready to write to Sheets
    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(SHEET_TAB)

    sheet.clear()
    # discharge_cfs can be None when the API misses a reading, which becomes
    # NaN in the numeric column -- gspread's request JSON breaks on a raw NaN
    # even without this fix ever having been hit yet on this script, so this
    # is preventative, not a confirmed-live bug like the other two scripts
    import numpy as np
    safe_df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    sheet.update([safe_df.columns.tolist()] + safe_df.astype(str).values.tolist())

    print(f"Google Sheet updated: tab '{SHEET_TAB}' ({len(df)} rows)")


if __name__ == "__main__":
    fetch_usgs_data()

"""
Upstream gauge live ingestion -- the leading-indicator sibling of
usgs_james_river.py. Same rolling-window pattern, different station:
James River at Bent Creek, VA (02026000), ~60 river miles upstream of the
Richmond gauge (02037500).

Why this station, why it matters, why lag=1 day: see upstream_lag.py, which
documents the empirical cross-correlation analysis (real 2024 discharge data
for both stations) that justified this pick over guessing a number. Short
version: Bent Creek's daily discharge correlates best with Richmond's
discharge ONE DAY LATER (r=0.88 at lag=1, vs r=0.83 at lag=0) -- so a surge
at Bent Creek today is a real, measurable early warning for Richmond
tomorrow, not a guess about rainfall.

This is the reason the project's own risk table calls this an "upgrade
path" rather than a nice-to-have: it is a materially better leading
indicator than weather, because it measures the water that is already
moving downstream instead of trying to model where rain will end up.

Outputs:
  - usgs_bentcreek_conditions.csv (local backup)
  - Google Sheet tab "Upstream Live" (new tab, alongside USGS Live / Weather Live)
"""

import json
import os
import sys
from datetime import datetime

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from join_features import RAPID_RISE_THRESHOLD_FT

STATION = "02026000"  # James River at Bent Creek, VA
PERIOD = "P30D"
OUTPUT_FILE = "usgs_bentcreek_conditions.csv"  # relative -- lands next to whatever runs this (repo root in CI)
SHEET_TAB = "Upstream Live"

API_URL = (
    f"https://waterservices.usgs.gov/nwis/iv/"
    f"?sites={STATION}&parameterCd=00065,00060&format=json&period={PERIOD}"
)


def add_rise_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Adds delta_24h / upstream_rapid_rise_flag to the upstream series, same
    approach as usgs_james_river.py's add_risk_index_columns: for each row,
    look up the reading closest to 24h earlier (30-min tolerance) and flag a
    rapid rise. This is what compute_daily_risk_index.py reads (at a 1-day
    lag) for the upstream-surge bonus -- see upstream_lag.py for why lag=1
    day, not this script's own same-day reading.
    """
    d = df.copy()
    d["dt"] = pd.to_datetime(d["datetime"])
    d = d.sort_values("dt").reset_index(drop=True)

    lookup = d[["dt", "upstream_gage_ft"]].rename(columns={"upstream_gage_ft": "gage_24h_ago"})
    lookup["dt"] = lookup["dt"] + pd.Timedelta(hours=24)
    matched = pd.merge_asof(
        d[["dt"]], lookup.sort_values("dt"), on="dt",
        direction="nearest", tolerance=pd.Timedelta(minutes=30),
    )
    d["gage_24h_ago"] = matched["gage_24h_ago"]
    d["delta_24h"] = (d["upstream_gage_ft"] - d["gage_24h_ago"]).round(2)
    d["upstream_rapid_rise_flag"] = d["delta_24h"] >= RAPID_RISE_THRESHOLD_FT
    d.loc[d["gage_24h_ago"].isna(), "upstream_rapid_rise_flag"] = False

    return d.drop(columns=["dt", "gage_24h_ago"])


def fetch_upstream_data() -> pd.DataFrame:
    print(f"Fetching upstream USGS data for station {STATION} (Bent Creek)...")
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    series = data["value"]["timeSeries"]
    gage_series = next((s for s in series if s["variable"]["variableCode"][0]["value"] == "00065"), None)
    discharge_series = next((s for s in series if s["variable"]["variableCode"][0]["value"] == "00060"), None)

    if not gage_series:
        raise ValueError("Gage height data not found in Bent Creek API response")

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
        rows.append({
            "datetime": dt.strftime("%Y-%m-%d %H:%M"),
            "date": dt.strftime("%Y-%m-%d"),
            "upstream_gage_ft": round(float(rec["value"]), 2),
            "upstream_discharge_cfs": (
                round(discharge_lookup.get(rec["dateTime"]), 0)
                if discharge_lookup.get(rec["dateTime"]) else None
            ),
            "station": STATION,
            "station_name": "James River at Bent Creek, VA",
        })

    df = pd.DataFrame(rows)
    df = add_rise_flag(df)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"CSV written: {OUTPUT_FILE} ({len(rows)} records)")
    write_to_google_sheets(df)
    return df


def write_to_google_sheets(df: pd.DataFrame):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not creds_json or not sheet_id:
        print("GOOGLE_CREDENTIALS or GOOGLE_SHEET_ID not set -- skipping Sheets write.")
        return
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(SHEET_TAB)
    sheet.clear()
    # same NaN-leak fix as nws_weather.py -- upstream_discharge_cfs is None
    # whenever the API misses a reading, which becomes NaN in the numeric
    # column and breaks gspread's request JSON even after astype(str)
    import numpy as np
    safe_df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    sheet.update([safe_df.columns.tolist()] + safe_df.astype(str).values.tolist())
    print(f"Google Sheet updated: tab '{SHEET_TAB}' ({len(df)} rows)")


if __name__ == "__main__":
    result = fetch_upstream_data()
    print(f"\nFetched {len(result)} upstream readings from Bent Creek")

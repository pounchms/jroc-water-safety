"""
USGS James River Gauge Data Fetcher
Station: 02037500 - James River Near Richmond, VA
Pulls gage height (ft) and discharge (cfs) for the past 30 days

Outputs:
  - usgs_james_river_conditions.csv (local backup)
  - Google Sheet tab "USGS Live Data" (Looker Studio source)

Google Sheets write requires GOOGLE_CREDENTIALS (service account JSON)
and GOOGLE_SHEET_ID env vars. See setup_google_sheets.md.
"""

import json
import os

import pandas as pd
import requests
from datetime import datetime

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
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"CSV written: {OUTPUT_FILE} ({len(rows)} records)")

    write_to_google_sheets(df)

    latest = rows[-1]
    print(f"\nLatest reading: {latest['datetime']}")
    print(f"  Gage height: {latest['gage_height_ft']} ft")
    print(f"  Discharge:   {latest['discharge_cfs']} cfs")
    print(f"  Risk level:  {latest['risk_level']}")


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
    sheet.update([df.columns.tolist()] + df.values.tolist())

    print(f"Google Sheet updated: tab '{SHEET_TAB}' ({len(df)} rows)")


if __name__ == "__main__":
    fetch_usgs_data()

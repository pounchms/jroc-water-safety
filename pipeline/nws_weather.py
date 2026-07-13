"""
NWS weather ingestion -- the sibling of usgs_james_river.py for the third
environmental variable set (air temp, precipitation, wind) called for in the
project architecture doc, which already reserves a "Weather Live" Google
Sheet tab for exactly this.

*** NOT LIVE-TESTED THIS SESSION -- READ BEFORE RELYING ON THIS ***
Every attempt to reach api.weather.gov from this sandbox (points, stations,
observations/latest -- three different endpoints) came back empty, while
the plain www.weather.gov website fetched fine and waterservices.usgs.gov
(used for usgs_history.py, same session) worked without issue. That's not a
"domain is blocked" message -- it's a silent empty response, so the honest
status is "untested here," not "confirmed broken." The schema below matches
the documented, stable api.weather.gov contract (used broadly and unchanged
for years), but treat it as unverified until someone runs it somewhere that
can actually reach the API -- a real machine, or as a GitHub Actions run,
the same way usgs_james_river.py's own scheduled runs are the real proof it
works (this chat sandbox could never run that one live either, for the same
reason -- see usgs_history.py docstring).

Station: KRIC (Richmond International Airport) -- nearest ASOS station with
a stable NWS observation feed to the James River gauge at 37.5632, -77.5469.

Design mirrors usgs_james_river.py on purpose: same output shape (local CSV
backup + Google Sheet tab), same "pull recent window, overwrite" pattern,
so the two scripts read the same to whoever maintains this after handoff.

Outputs:
  - nws_weather_conditions.csv (local backup)
  - Google Sheet tab "Weather Live" (already named in the architecture doc)
"""

import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests

STATION = "KRIC"  # Richmond International Airport
API_URL = f"https://api.weather.gov/stations/{STATION}/observations"
SHEET_TAB = "Weather Live"
OUTPUT_FILE = "nws_weather_conditions.csv"  # relative -- lands next to whatever runs this (repo root in CI)


def _c_to_f(celsius):
    return None if celsius is None else round(celsius * 9 / 5 + 32, 1)


def _kmh_to_mph(kmh):
    return None if kmh is None else round(kmh * 0.621371, 1)


def _mm_to_in(mm):
    return None if mm is None else round(mm / 25.4, 2)


def _parse_observation(feature: dict) -> dict:
    """One NWS observation record -> our flat schema. NWS reports in SI
    units (degC, km/h, mm) -- converted here so downstream code never has
    to think about units."""
    props = feature["properties"]
    precip = props.get("precipitationLastHour", {}).get("value")
    return {
        "datetime": props.get("timestamp"),
        "air_temp_f": _c_to_f((props.get("temperature") or {}).get("value")),
        "wind_mph": _kmh_to_mph((props.get("windSpeed") or {}).get("value")),
        "precip_1h_in": _mm_to_in(precip),
        "station": STATION,
    }


def fetch_recent_weather(hours: int = 72) -> pd.DataFrame:
    """Live pull -- mirrors usgs_james_river.py's rolling-window approach.
    Run daily; each run overwrites the local CSV and Sheet tab with the
    latest rolling window (same pattern as the USGS script, same tradeoffs:
    simple, zero-maintenance, not a growing historical archive)."""
    start = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = requests.get(API_URL, params={"start": start}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = [_parse_observation(f) for f in data.get("features", [])]
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")

    # rolling 24h precip total -- this is what risk_index.py actually uses,
    # not the raw hourly figure
    df = df.set_index("datetime")
    df["precip_24h_in"] = df["precip_1h_in"].rolling("24h", min_periods=1).sum().round(2)
    df = df.reset_index()

    df.to_csv(OUTPUT_FILE, index=False)
    write_to_google_sheets(df)
    return df


def fetch_weather_history(start_date: str, end_date: str) -> pd.DataFrame:
    """Historical backfill counterpart, same idea as
    usgs_history.fetch_iv_history -- NWS observation history is available
    per-station via start/end params, paginated ~500 records at a time.
    Chunk by a few weeks at a time for the same reason usgs_history.py
    chunks iv requests: payload size, not a hard service limit."""
    resp = requests.get(
        API_URL,
        params={
            "start": f"{start_date}T00:00:00Z",
            "end": f"{end_date}T23:59:59Z",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = [_parse_observation(f) for f in data.get("features", [])]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def daily_weather_features(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse hourly/sub-hourly observations to one row per day --
    matching the daily granularity join_features.py joins incidents against.
    precip_24h_in = sum of that day's hourly precip; wind_mph = day's max
    (gusts matter more than the average for river/small-craft safety);
    air_temp_f = day's mean."""
    df = hourly_df.copy()
    df["date"] = pd.to_datetime(df["datetime"]).dt.date
    daily = df.groupby("date").agg(
        air_temp_f=("air_temp_f", "mean"),
        wind_mph=("wind_mph", "max"),
        precip_24h_in=("precip_1h_in", "sum"),
    ).reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    daily["air_temp_f"] = daily["air_temp_f"].round(1)
    daily["precip_24h_in"] = daily["precip_24h_in"].round(2)
    return daily


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
    sheet.update([df.columns.tolist()] + df.astype(str).values.tolist())
    print(f"Google Sheet updated: tab '{SHEET_TAB}' ({len(df)} rows)")


if __name__ == "__main__":
    result = fetch_recent_weather()
    print(f"Fetched {len(result)} weather observations from {STATION}")

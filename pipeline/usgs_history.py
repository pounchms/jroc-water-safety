"""
USGS historical environmental data ingestion.

This is the one-time / occasional BACKFILL counterpart to usgs_james_river.py
(which only keeps a rolling ~30-day window for the live dashboard). This module
pulls arbitrary historical date ranges from any USGS NWIS station so the
incident-environmental join has something to join against for dates further
back than 30 days.

Key finding (2026-07-13): the NWIS Instantaneous Values (iv) service returns
full period-of-record history for gage height (00065) when called with
startDT/endDT, not just a rolling window -- the 30-day limit in the live
script is a parameter choice (PERIOD = "P30D"), not a service limitation.
Daily Values (dv) has NO gage-height series for the Richmond site (00065 dv
is empty) but DOES have discharge (00060) daily means for the full period of
record -- useful as a compact proxy series when only daily granularity is
needed.

Stations used by this project:
    RICHMOND_STATION = "02037500"  -- James River near Richmond (the main gauge)
    BENT_CREEK_STATION = "02026000"  -- James River at Bent Creek, VA, ~60 mi
        upstream -- see upstream_lag.py for why this station and lag=1 day.

Usage:
    from usgs_history import fetch_iv_history, fetch_dv_discharge_history

    gage_df = fetch_iv_history("2024-01-01", "2024-03-31", param="00065")
    discharge_df = fetch_dv_discharge_history("2021-01-01", "2026-07-13")
    upstream_df = fetch_dv_discharge_history("2021-01-01", "2026-07-13", station=BENT_CREEK_STATION)

Chunk iv requests into short windows (a few days at a time) -- full-year iv
pulls are large (15-min cadence, ~35k rows/year) and slow to page through.
dv requests can span years in one call since they're one row/day.
"""

import json
from datetime import datetime, timedelta

import pandas as pd
import requests

RICHMOND_STATION = "02037500"
BENT_CREEK_STATION = "02026000"
STATION = RICHMOND_STATION  # default, kept for backward compatibility
BASE_URL = "https://waterservices.usgs.gov/nwis"


def _parse_timeseries(data: dict) -> pd.DataFrame:
    series = data["value"]["timeSeries"]
    if not series:
        return pd.DataFrame(columns=["datetime", "value"])
    values = series[0]["values"][0]["value"]
    rows = [
        {"datetime": pd.to_datetime(v["dateTime"]), "value": float(v["value"])}
        for v in values
        if v["value"] != "-999999"
    ]
    return pd.DataFrame(rows)


def fetch_iv_history(start_date, end_date, param="00065", station=RICHMOND_STATION):
    """Instantaneous values (e.g. gage height 00065) for an arbitrary historical
    range, at any station. Keep ranges short (days, not years) -- this is
    15-min cadence data."""
    url = (
        f"{BASE_URL}/iv/?sites={station}&parameterCd={param}"
        f"&startDT={start_date}&endDT={end_date}&format=json"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return _parse_timeseries(resp.json())


def fetch_dv_discharge_history(start_date, end_date, station=RICHMOND_STATION):
    """Daily mean discharge (00060) for an arbitrary historical range, at any
    station. This service has full period-of-record history and is compact
    (1 row/day), so multi-year ranges are fine in a single call. Note: 00065
    gage height has NO daily-value series for the Richmond site -- use
    fetch_iv_history + your own daily resample if you need historical gage
    height instead of flow. (Bent Creek's dv gage-height availability hasn't
    been checked -- assume the same limitation until verified.)"""
    url = (
        f"{BASE_URL}/dv/?sites={station}&parameterCd=00060"
        f"&startDT={start_date}&endDT={end_date}&format=json"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    df = _parse_timeseries(resp.json())
    return df.rename(columns={"value": "discharge_cfs"})


def backfill_daily_gage_height(start_date, end_date, chunk_days=10, station=RICHMOND_STATION):
    """Backfill historical daily gage height by chunking iv requests and
    taking the daily max reading per day. This is the practical way to build
    a multi-year gage-height history without one call per year (iv payloads
    get large fast). Run this once, save the CSV, and reuse it -- don't
    re-run on every pipeline execution."""
    frames = []
    cur = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        df = fetch_iv_history(cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"),
                               param="00065", station=station)
        frames.append(df)
        cur = chunk_end + timedelta(days=1)
    full = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["datetime", "value"])
    if full.empty:
        return full
    full["date"] = full["datetime"].dt.date
    daily = full.groupby("date")["value"].max().reset_index()
    daily = daily.rename(columns={"value": "gage_height_ft"})
    daily["date"] = pd.to_datetime(daily["date"])
    return daily

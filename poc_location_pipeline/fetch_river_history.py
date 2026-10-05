"""
Attach real river conditions to any list of incidents, after the fact.

This replaces the daily cron jobs (disabled October 2026). USGS keeps its full history, so
river conditions for any past date can be pulled when they're needed. A daily copy isn't
needed.

Usage (from this folder):
    python fetch_river_history.py incidents.csv
    python fetch_river_history.py incidents.csv --date-col incident_date --out incidents_with_river.csv
    python fetch_river_history.py incidents.csv --station 02026000      # Bent Creek, ~60 mi upstream

Input: any CSV with a date column (YYYY-MM-DD, or a date-time; only the date part is used).
Output: the same rows plus
    discharge_cfs                  USGS daily mean flow (cubic feet per second) on that date
    delta_discharge_24h_cfs / _72h change from 1 / 3 days earlier
    rapid_rise_flag_discharge      rose >= 1,500 cfs in 24 h       (placeholder thresholds,
    deceptive_calm_flag_discharge  <= 3,000 cfs but rose >= 1,000   never validated: see README)
and a copy of the daily series it used (river_daily_<station>.csv), so the join can be checked.

Data source: USGS daily values, statistic 00003 (daily mean), parameter 00060 (discharge).
Tries the current API first (api.waterdata.usgs.gov) and falls back to the legacy NWIS
service (waterservices.usgs.gov), which USGS is phasing out. Needs: pip install pandas requests
"""
import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline"))
from join_features import build_environmental_features  # noqa: E402

RAPID_RISE_CFS, CALM_MAX_CFS, CALM_DELTA_CFS = 1500, 3000, 1000  # same as build_historical_incidents.py
NEW_API = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"
LEGACY_API = "https://waterservices.usgs.gov/nwis/dv/"
HEADERS = {"User-Agent": "jroc-water-safety (github.com/pounchms/jroc-water-safety)"}


def _get(url, params, attempts=4):
    """USGS sometimes answers 5xx for a few minutes; back off and retry before giving up."""
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=60)
            if r.status_code < 500 and r.status_code != 429:
                r.raise_for_status()
                return r
            err = requests.HTTPError(f"{r.status_code} from {url}")
        except (requests.ConnectionError, requests.Timeout) as e:
            err = e
        if i < attempts - 1:
            time.sleep(10 * 2 ** i)
    raise err


def fetch_new(station, start, end):
    params = {"monitoring_location_id": f"USGS-{station}", "parameter_code": "00060",
              "statistic_id": "00003", "datetime": f"{start}/{end}", "f": "json", "limit": 50000}
    url, rows = NEW_API, []
    while url:
        data = _get(url, params).json()
        rows += [{"date": f["properties"]["time"][:10], "discharge_cfs": float(f["properties"]["value"])}
                 for f in data.get("features", []) if f["properties"].get("value") not in (None, "")]
        url = next((l["href"] for l in data.get("links", []) if l.get("rel") == "next"), None)
        params = None  # the "next" link already carries the query
    return pd.DataFrame(rows)


def fetch_legacy(station, start, end):
    data = _get(LEGACY_API, {"sites": station, "parameterCd": "00060", "statCd": "00003",
                             "startDT": start, "endDT": end, "format": "json"}).json()
    ts = data["value"]["timeSeries"]
    vals = ts[0]["values"][0]["value"] if ts else []
    return pd.DataFrame([{"date": v["dateTime"][:10], "discharge_cfs": float(v["value"])}
                         for v in vals if v["value"] not in ("-999999", "")])


def fetch_daily(station, start, end):
    for name, fn in (("api.waterdata.usgs.gov", fetch_new), ("waterservices.usgs.gov (legacy)", fetch_legacy)):
        try:
            df = fn(station, start, end)
            if len(df):
                print(f"Fetched {len(df)} daily values for USGS {station} from {name}")
                return df.drop_duplicates("date").sort_values("date")
            print(f"{name}: no values returned, trying the next source")
        except Exception as e:  # report and fall through to the next source
            print(f"{name} failed: {e}")
    sys.exit("Couldn't get river data from either USGS service. Try again later.")


def attach(df, daily, date_col):
    env = build_environmental_features(daily, level_col="discharge_cfs", rapid_rise_threshold=RAPID_RISE_CFS,
                                       deceptive_calm_local_max=CALM_MAX_CFS, deceptive_calm_delta_min=CALM_DELTA_CFS)
    env["date"] = env["date"].dt.strftime("%Y-%m-%d")
    env = env.rename(columns={"delta_24h": "delta_discharge_24h_cfs", "delta_72h": "delta_discharge_72h_cfs",
                              "rapid_rise_flag": "rapid_rise_flag_discharge",
                              "deceptive_calm_flag": "deceptive_calm_flag_discharge"})
    cols = ["discharge_cfs", "delta_discharge_24h_cfs", "delta_discharge_72h_cfs",
            "rapid_rise_flag_discharge", "deceptive_calm_flag_discharge"]
    key = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
    out = df.drop(columns=[c for c in cols if c in df.columns])
    return out.join(env.set_index("date")[cols], on=key)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("csv")
    ap.add_argument("--date-col", default="incident_date")
    ap.add_argument("--station", default="02037500", help="USGS site number (default: James River near Richmond)")
    ap.add_argument("--out")
    a = ap.parse_args()

    df = pd.read_csv(a.csv)
    dates = pd.to_datetime(df[a.date_col])
    start = (dates.min() - pd.Timedelta(days=3)).strftime("%Y-%m-%d")  # 3 days back for the 72 h change
    end = dates.max().strftime("%Y-%m-%d")
    daily = fetch_daily(a.station, start, end)
    daily.to_csv(f"river_daily_{a.station}.csv", index=False)

    out = attach(df, daily, a.date_col)
    path = a.out or str(Path(a.csv).with_name(Path(a.csv).stem + "_with_river.csv"))
    out.to_csv(path, index=False)
    hit = out.discharge_cfs.notna()
    print(f"{hit.sum()} of {len(out)} rows got a river reading ({hit.mean():.1%}). Wrote {path}")
    if (~hit).any():
        print("No reading for:", ", ".join(sorted(set(pd.to_datetime(out.loc[~hit, a.date_col]).dt.strftime('%Y-%m-%d')))[:10]),
              "(future dates, or USGS hasn't published them yet)")


if __name__ == "__main__":
    main()

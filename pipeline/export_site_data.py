"""
Builds docs/data/latest.json for the static GitHub Pages dashboard
(docs/index.html) from the same local CSVs the three fetch scripts already
write and commit to this repo -- no Google Sheets read needed here, so the
public site keeps working even if a Sheets/gspread call ever has a bad day.
Looker Studio (reading the Sheet) and this static site (reading these CSVs)
are two independent views of the same underlying data, not one depending on
the other.

Run last in the daily sequence, same slot as compute_daily_risk_index.py
(see .github/workflows/compute_risk_index.yml), so all three CSVs are fresh
by the time this reads them.

The "current" full risk score below is a SECOND, independent computation of
the same rules as compute_daily_risk_index.py -- not a read of its output --
specifically so the public site has no runtime dependency on Sheets at all.
The two should agree; if they ever don't, that's a bug worth chasing, not
expected behavior.
"""

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from risk_index import RiskInputs, score as compute_risk_index, public_risk_level

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # pipeline/ -> repo root
RIVER_CSV = os.path.join(REPO_ROOT, "usgs_james_river_conditions.csv")
UPSTREAM_CSV = os.path.join(REPO_ROOT, "usgs_bentcreek_conditions.csv")
WEATHER_CSV = os.path.join(REPO_ROOT, "nws_weather_conditions.csv")
OUTPUT_PATH = os.path.join(REPO_ROOT, "docs", "data", "latest.json")

UPSTREAM_LAG_DAYS = 1
UPSTREAM_LAG_TOLERANCE_HOURS = 2
TREND_DAYS = 14  # how much history ships to the browser -- keep the JSON small and the chart readable


def _safe_float(v):
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _parse_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def load_csv(path: str, tz_naive: bool = True) -> pd.DataFrame:
    """Loads one of the three fetch scripts' own CSVs. Missing file (e.g. a
    brand-new repo, or a feed that hasn't run yet) returns an empty frame
    rather than raising -- the site should render partial data, not crash."""
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["datetime"])
    if tz_naive and isinstance(df["datetime"].dtype, pd.DatetimeTZDtype):
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df.sort_values("datetime").reset_index(drop=True)


def get_latest_river(river_df: pd.DataFrame) -> dict:
    if river_df.empty:
        return None
    latest = river_df.iloc[-1]
    return {
        "datetime": latest["datetime"],
        "gage_height_ft": _safe_float(latest.get("gage_height_ft")),
        "discharge_cfs": _safe_float(latest.get("discharge_cfs")),
        "rapid_rise_flag": _parse_bool(latest.get("rapid_rise_flag", False)),
        "deceptive_calm_flag": _parse_bool(latest.get("deceptive_calm_flag", False)),
    }


def get_latest_weather(weather_df: pd.DataFrame) -> dict:
    if weather_df.empty:
        return {"precip_24h_in": None, "wind_mph": None, "air_temp_f": None, "datetime": None}
    latest = weather_df.iloc[-1]
    return {
        "datetime": latest["datetime"],
        "precip_24h_in": _safe_float(latest.get("precip_24h_in")),
        "wind_mph": _safe_float(latest.get("wind_mph")),
        "air_temp_f": _safe_float(latest.get("air_temp_f")),
    }


def get_lagged_upstream(upstream_df: pd.DataFrame, reference_dt, lag_days=UPSTREAM_LAG_DAYS, tolerance_hours=UPSTREAM_LAG_TOLERANCE_HOURS) -> dict:
    """Mirrors compute_daily_risk_index.get_lagged_upstream_signal, reading
    the local CSV instead of the Sheet. Gracefully returns flag=False (not
    an error) if the column doesn't exist yet (e.g. usgs_upstream.py hasn't
    been re-run since the rapid-rise flag was added) or nothing is within
    tolerance -- a missing upstream signal shouldn't take down the site."""
    if upstream_df.empty or "upstream_rapid_rise_flag" not in upstream_df.columns:
        return {"upstream_rapid_rise_flag": False, "matched_datetime": None}
    target = reference_dt - pd.Timedelta(days=lag_days)
    idx = (upstream_df["datetime"] - target).abs().idxmin()
    row = upstream_df.loc[idx]
    if abs((row["datetime"] - target).total_seconds()) > tolerance_hours * 3600:
        return {"upstream_rapid_rise_flag": False, "matched_datetime": None}
    return {
        "upstream_rapid_rise_flag": _parse_bool(row["upstream_rapid_rise_flag"]),
        "matched_datetime": row["datetime"],
    }


def build_trend(river_df: pd.DataFrame, days: int = TREND_DAYS) -> list:
    """One point per calendar day (the day's last reading) for the last
    `days` days -- keeps the JSON small (a couple dozen points instead of
    thousands of 15-min readings) while still showing the shape of a rise
    or fall over time. Uses the river script's own risk_index_1_10 (river
    signal only, computed every ~15 min) for the trend line, since the full
    5-input score only exists once a day -- see module docstring."""
    if river_df.empty:
        return []
    d = river_df.copy()
    d["day"] = d["datetime"].dt.date
    daily = d.groupby("day").last().reset_index()
    daily = daily.sort_values("day").tail(days)
    return [
        {
            "date": str(row["day"]),
            "gage_height_ft": _safe_float(row.get("gage_height_ft")),
            "discharge_cfs": _safe_float(row.get("discharge_cfs")),
            "risk_index_1_10": int(row["risk_index_1_10"]) if not pd.isna(row.get("risk_index_1_10")) else None,
        }
        for _, row in daily.iterrows()
    ]


def build_site_data() -> dict:
    river_df = load_csv(RIVER_CSV)
    weather_df = load_csv(WEATHER_CSV)
    upstream_df = load_csv(UPSTREAM_CSV)

    river = get_latest_river(river_df)
    if river is None:
        raise ValueError(f"'{RIVER_CSV}' has no data -- nothing to publish. Has usgs_james_river.py run yet?")

    weather = get_latest_weather(weather_df)
    upstream = get_lagged_upstream(upstream_df, river["datetime"])

    inputs = RiskInputs(
        gage_height_ft=river["gage_height_ft"],
        rapid_rise_flag=river["rapid_rise_flag"],
        deceptive_calm_flag=river["deceptive_calm_flag"],
        precip_24h_in=weather["precip_24h_in"],
        wind_mph=weather["wind_mph"],
        upstream_rapid_rise_flag=upstream["upstream_rapid_rise_flag"],
    )
    result = compute_risk_index(inputs)

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "current": {
            "datetime": river["datetime"].strftime("%Y-%m-%d %H:%M"),
            "gage_height_ft": river["gage_height_ft"],
            "discharge_cfs": river["discharge_cfs"],
            "rapid_rise_flag": river["rapid_rise_flag"],
            "deceptive_calm_flag": river["deceptive_calm_flag"],
            "precip_24h_in": weather["precip_24h_in"],
            "wind_mph": weather["wind_mph"],
            "air_temp_f": weather["air_temp_f"],
            "upstream_rapid_rise_flag": upstream["upstream_rapid_rise_flag"],
            "risk_index_1_10": result["risk_index_1_10"],
            "risk_index_level": public_risk_level(result["risk_index_1_10"]),
            "risk_index_reasons": result["reasons"],
        },
        "trend": build_trend(river_df),
    }


def main():
    site_data = build_site_data()
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(site_data, f, indent=2)
    print(f"Wrote {OUTPUT_PATH}")
    print(f"  current: {site_data['current']['risk_index_1_10']} ({site_data['current']['risk_index_level']})")
    print(f"  trend points: {len(site_data['trend'])}")


if __name__ == "__main__":
    main()

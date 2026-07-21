"""
Combines all three live feeds into the FULL risk_index_1_10 -- gage height,
rapid-rise, and deceptive-calm from the Richmond gauge (already computed by
usgs_james_river.py), PLUS the weather bonus (precip/wind from Weather Live)
and the upstream-surge bonus (Bent Creek, lagged 1 day per upstream_lag.py).

Why a separate script instead of folding this into usgs_james_river.py:
that script runs FIRST (7:00am ET), before Upstream Live (7:15am) and
Weather Live (7:30am) have today's readings. This script is scheduled to run
LAST (7:45am ET, see .github/workflows/compute_risk_index.yml) specifically
so all three tabs already have today's data by the time it reads them back.

Reads (via gspread, not a fresh API pull):
  - "USGS Live Data"  -> latest row: gage_height_ft, rapid_rise_flag, deceptive_calm_flag
  - "Weather Live"    -> latest row: precip_24h_in, wind_mph
  - "Upstream Live"   -> row closest to (river reading time - 1 day): upstream_rapid_rise_flag

Writes:
  - "Risk Index Live" Sheet tab -- one row per day (today's run replaces
    today's row if this script is re-run same day, rather than duplicating).
    This is meant to be the actual source Looker Studio points to for the
    public 1-10 score, since it's the only place all signals are combined.

Missing/stale inputs degrade gracefully: a missing weather or upstream
reading just means that bonus doesn't apply (None precip/wind, upstream flag
defaults False) rather than crashing the whole score -- see the *_signal
functions below.
"""

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from risk_index import RiskInputs, score as compute_risk_index, public_risk_level

RIVER_TAB = "USGS Live Data"
WEATHER_TAB = "Weather Live"
UPSTREAM_TAB = "Upstream Live"
OUTPUT_TAB = "Risk Index Live"
UPSTREAM_LAG_DAYS = 1  # see upstream_lag.py -- empirical, not a guess
UPSTREAM_LAG_TOLERANCE_HOURS = 2  # how far off a matched upstream reading can be before we discard it


def _parse_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() == "true"


def _parse_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def sheet_values_to_df(values: list[list[str]]) -> pd.DataFrame:
    """gspread's get_all_values() returns [header_row, *data_rows], all
    strings (matches how every ingestion script writes -- .astype(str)
    before sheet.update()). Ragged rows (shorter than header) are padded."""
    if not values or len(values) < 2:
        return pd.DataFrame()
    header, rows = values[0], values[1:]
    fixed_rows = [r + [""] * (len(header) - len(r)) for r in rows]
    return pd.DataFrame(fixed_rows, columns=header)


def get_latest_river_signal(river_df: pd.DataFrame) -> dict:
    """Required signal -- if this is missing, there's nothing to score."""
    if river_df is None or river_df.empty:
        raise ValueError(f"'{RIVER_TAB}' is empty -- nothing to score. Has usgs_james_river.py run today?")
    d = river_df.copy()
    d["dt"] = pd.to_datetime(d["datetime"])
    latest = d.sort_values("dt").iloc[-1]
    return {
        "datetime": latest["dt"],
        "gage_height_ft": _parse_float(latest["gage_height_ft"]),
        "rapid_rise_flag": _parse_bool(latest.get("rapid_rise_flag", False)),
        "deceptive_calm_flag": _parse_bool(latest.get("deceptive_calm_flag", False)),
    }


def get_latest_weather_signal(weather_df: pd.DataFrame) -> dict:
    """Optional signal -- missing/empty just means no weather bonus applies."""
    if weather_df is None or weather_df.empty:
        return {"precip_24h_in": None, "wind_mph": None, "datetime": None}
    d = weather_df.copy()
    d["dt"] = pd.to_datetime(d["datetime"])
    latest = d.sort_values("dt").iloc[-1]
    return {
        "datetime": latest["dt"],
        "precip_24h_in": _parse_float(latest.get("precip_24h_in")),
        "wind_mph": _parse_float(latest.get("wind_mph")),
    }


def get_lagged_upstream_signal(
    upstream_df: pd.DataFrame,
    reference_dt,
    lag_days: int = UPSTREAM_LAG_DAYS,
    tolerance_hours: float = UPSTREAM_LAG_TOLERANCE_HOURS,
) -> dict:
    """Optional signal -- finds the upstream reading closest to
    (reference_dt - lag_days). If nothing is within tolerance (stale/missing
    Upstream Live tab), returns flag=False rather than raising -- a surge
    bonus we can't confirm shouldn't block the rest of the score."""
    if upstream_df is None or upstream_df.empty:
        return {"upstream_rapid_rise_flag": False, "matched_datetime": None}
    d = upstream_df.copy()
    d["dt"] = pd.to_datetime(d["datetime"])
    d = d.sort_values("dt")
    target = reference_dt - pd.Timedelta(days=lag_days)
    idx = (d["dt"] - target).abs().idxmin()
    row = d.loc[idx]
    if abs((row["dt"] - target).total_seconds()) > tolerance_hours * 3600:
        return {"upstream_rapid_rise_flag": False, "matched_datetime": None}
    return {
        "upstream_rapid_rise_flag": _parse_bool(row.get("upstream_rapid_rise_flag", False)),
        "matched_datetime": row["dt"],
    }


def build_and_score(river_signal: dict, weather_signal: dict, upstream_signal: dict) -> dict:
    inputs = RiskInputs(
        gage_height_ft=river_signal["gage_height_ft"],
        rapid_rise_flag=river_signal["rapid_rise_flag"],
        deceptive_calm_flag=river_signal["deceptive_calm_flag"],
        precip_24h_in=weather_signal["precip_24h_in"],
        wind_mph=weather_signal["wind_mph"],
        upstream_rapid_rise_flag=upstream_signal["upstream_rapid_rise_flag"],
    )
    result = compute_risk_index(inputs)
    return {
        "date": river_signal["datetime"].strftime("%Y-%m-%d"),
        "datetime": river_signal["datetime"].strftime("%Y-%m-%d %H:%M"),
        "gage_height_ft": river_signal["gage_height_ft"],
        "rapid_rise_flag": river_signal["rapid_rise_flag"],
        "deceptive_calm_flag": river_signal["deceptive_calm_flag"],
        "precip_24h_in": weather_signal["precip_24h_in"],
        "wind_mph": weather_signal["wind_mph"],
        "upstream_rapid_rise_flag": upstream_signal["upstream_rapid_rise_flag"],
        "upstream_reading_used": (
            upstream_signal["matched_datetime"].strftime("%Y-%m-%d %H:%M")
            if upstream_signal["matched_datetime"] is not None else ""
        ),
        "risk_index_1_10": result["risk_index_1_10"],
        "risk_index_level": public_risk_level(result["risk_index_1_10"]),
        "risk_index_reasons": "; ".join(result["reasons"]),
        "computed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _cell_str(v) -> str:
    """None -> "" (blank cell), not the literal string "None" -- matches the
    fillna("") pattern the other two scripts use before writing to Sheets."""
    return "" if v is None else str(v)


def write_result_row(sheet, result_row: dict):
    """Appends today's row, or replaces it if this script already ran today
    (re-running shouldn't produce duplicate rows for the same date)."""
    existing = sheet.get_all_values()
    header = list(result_row.keys())

    if not existing:
        sheet.update([header, [_cell_str(v) for v in result_row.values()]])
        return

    old_header = existing[0]
    rows = existing[1:]
    date_idx = old_header.index("date") if "date" in old_header else 0

    new_row = [_cell_str(result_row.get(h)) for h in header]
    replaced = False
    out_rows = []
    for r in rows:
        if len(r) > date_idx and r[date_idx] == result_row["date"]:
            out_rows.append(new_row)
            replaced = True
        else:
            out_rows.append(r + [""] * (len(header) - len(r)) if len(r) < len(header) else r[:len(header)])
    if not replaced:
        out_rows.append(new_row)

    sheet.clear()
    sheet.update([header] + out_rows)


def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not creds_json or not sheet_id:
        print("GOOGLE_CREDENTIALS or GOOGLE_SHEET_ID not set -- cannot read/write Sheets, exiting.")
        return

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    river_df = sheet_values_to_df(spreadsheet.worksheet(RIVER_TAB).get_all_values())
    weather_df = sheet_values_to_df(spreadsheet.worksheet(WEATHER_TAB).get_all_values())
    upstream_df = sheet_values_to_df(spreadsheet.worksheet(UPSTREAM_TAB).get_all_values())

    river_signal = get_latest_river_signal(river_df)
    weather_signal = get_latest_weather_signal(weather_df)
    upstream_signal = get_lagged_upstream_signal(upstream_df, river_signal["datetime"])

    result_row = build_and_score(river_signal, weather_signal, upstream_signal)

    try:
        output_sheet = spreadsheet.worksheet(OUTPUT_TAB)
    except gspread.WorksheetNotFound:
        output_sheet = spreadsheet.add_worksheet(title=OUTPUT_TAB, rows=400, cols=20)

    write_result_row(output_sheet, result_row)

    print(f"Risk Index Live updated for {result_row['date']}:")
    print(f"  risk_index_1_10 = {result_row['risk_index_1_10']} ({result_row['risk_index_level']})")
    print(f"  {result_row['risk_index_reasons']}")
    print(f"  weather: precip_24h_in={result_row['precip_24h_in']} wind_mph={result_row['wind_mph']}")
    print(f"  upstream_rapid_rise_flag={result_row['upstream_rapid_rise_flag']} "
          f"(reading used: {result_row['upstream_reading_used'] or 'none within tolerance'})")


if __name__ == "__main__":
    main()

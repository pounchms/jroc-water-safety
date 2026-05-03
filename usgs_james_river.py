"""
USGS James River Gauge Data Fetcher
Station: 02037500 - James River Near Richmond, VA
Pulls gage height (ft) and discharge (cfs) for the past 30 days
Output: usgs_james_river_conditions.csv (Tableau-ready)

Run manually before any presentation to refresh data.
Schedule via Windows Task Scheduler or cron for automation.
"""

import requests
import pandas as pd
from datetime import datetime
import os

STATION = "02037500"
PERIOD = "P30D"  # Last 30 days — change to P7D for 7 days, P365D for a year
OUTPUT_FILE = "usgs_james_river_conditions.csv"

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
    
    latest = rows[-1]
    print(f"\nDone. {len(rows)} records written to {OUTPUT_FILE}")
    print(f"Latest reading: {latest['datetime']}")
    print(f"  Gage height: {latest['gage_height_ft']} ft")
    print(f"  Discharge:   {latest['discharge_cfs']} cfs")
    print(f"  Risk level:  {latest['risk_level']}")
    print(f"\nTableau: connect to {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    fetch_usgs_data()

"""
Open a terminal and run:

"pip install requests pandas
python usgs_james_river.py"

to generate output file
"""
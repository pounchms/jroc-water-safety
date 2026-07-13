"""
Quick verification pass on join_features.py and risk_index.py.
Not a full test suite -- a sanity gate before treating today's build as done.
"""

import pandas as pd
from join_features import build_environmental_features, join_incidents, attach_weather
from risk_index import RiskInputs, score, public_risk_level

failures = []


def check(name, condition):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}")
    if not condition:
        failures.append(name)


# 1. join_incidents refuses to double-join a table that already has environmental columns
already_joined = pd.DataFrame({"incident_date": ["2024-01-01"], "gage_height_ft": [5.0]})
try:
    join_incidents(already_joined, pd.DataFrame({"date": ["2024-01-01"], "gage_height_ft": [5.0]}))
    check("join_incidents raises on pre-joined input", False)
except ValueError:
    check("join_incidents raises on pre-joined input", True)

# 2. build_environmental_features tolerates a missing day (gap) via interpolation
gapped = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02", "2024-01-04"],  # note: 01-03 missing
    "gage_height_ft": [4.0, 4.5, 6.0],
})
feats = build_environmental_features(gapped)
check("build_environmental_features fills gap day", len(feats) == 4 and feats["gage_height_ft"].isna().sum() == 0)

# 3. rapid_rise_flag actually fires when the threshold is crossed, and not otherwise
rising = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "gage_height_ft": [3.0, 5.0],  # +2.0ft in 24h, threshold is 1.5ft
})
feats2 = build_environmental_features(rising)
check("rapid_rise_flag fires above threshold", bool(feats2.iloc[1]["rapid_rise_flag"]))

flat = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "gage_height_ft": [3.0, 3.1],
})
feats3 = build_environmental_features(flat)
check("rapid_rise_flag does not fire below threshold", not bool(feats3.iloc[1]["rapid_rise_flag"]))

# 4. join_incidents produces the response_time_min / incident_duration_min fields
bare = pd.DataFrame({
    "incident_id": ["X1"],
    "incident_date": ["2024-01-02"],
    "alarm_datetime": ["2024-01-02 10:00"],
    "arrival_datetime": ["2024-01-02 10:09"],
    "cleared_datetime": ["2024-01-02 11:15"],
})
env = build_environmental_features(pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "gage_height_ft": [4.0, 4.2],
}))
joined = join_incidents(bare, env)
check("response_time_min computed correctly", joined.iloc[0]["response_time_min"] == 9.0)
check("incident_duration_min computed correctly", joined.iloc[0]["incident_duration_min"] == 75.0)

# 5. risk_index.py has no CODE dependency on the synthetic dataset -- it may
# (and should) mention "synthetic" in comments/docstrings as a warning to
# future readers; what matters is it never loads or reads the synthetic file.
with open("risk_index.py") as f:
    src = f.read()
check("risk_index.py never loads the synthetic dataset",
      "read_csv" not in src and "synthetic_rfd" not in src)

# 6. risk_index output schema has the fields a Looker feed / real join would need
result = score(RiskInputs(gage_height_ft=9.0, rapid_rise_flag=True))
check("risk_index output has required keys",
      {"risk_index_1_10", "base_score", "bonus", "reasons"} <= result.keys())
check("risk_index_1_10 stays within 1-10 bounds", 1 <= result["risk_index_1_10"] <= 10)
expected_categories = {"Low", "Moderate", "High", "Flood"}
check("public_risk_level returns a known category",
      public_risk_level(result["risk_index_1_10"]) in expected_categories)

# 7. attach_weather correctly attaches same-day weather and refuses a double-attach
weather_daily = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "air_temp_f": [45.0, 50.0],
    "wind_mph": [12.0, 25.0],
    "precip_24h_in": [0.1, 1.5],
})
with_weather = attach_weather(joined, weather_daily)
check("attach_weather adds weather columns",
      {"air_temp_f", "wind_mph", "precip_24h_in"} <= set(with_weather.columns))
check("attach_weather matches the correct day's values",
      with_weather.iloc[0]["wind_mph"] == 25.0)  # incident is on 2024-01-02

try:
    attach_weather(with_weather, weather_daily)
    check("attach_weather raises on double-attach", False)
except ValueError:
    check("attach_weather raises on double-attach", True)

# 8. risk_index actually uses precip/wind now (not just accepting and ignoring them)
no_weather = score(RiskInputs(gage_height_ft=3.5))
with_rain = score(RiskInputs(gage_height_ft=3.5, precip_24h_in=1.5))
with_wind = score(RiskInputs(gage_height_ft=3.5, wind_mph=25))
check("heavy rain increases the score", with_rain["risk_index_1_10"] > no_weather["risk_index_1_10"])
check("high wind increases the score", with_wind["risk_index_1_10"] > no_weather["risk_index_1_10"])

light_rain = score(RiskInputs(gage_height_ft=3.5, precip_24h_in=0.2))
check("light rain does NOT trigger the bonus", light_rain["risk_index_1_10"] == no_weather["risk_index_1_10"])

# 9. attach_upstream shifts the upstream signal forward by lag_days and refuses double-attach
from join_features import attach_upstream

upstream_raw = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
    "gage_height_ft": [3.0, 6.0, 6.2],  # a rapid rise on 01-02 (Bent Creek time)
})
upstream_features = build_environmental_features(upstream_raw)
with_upstream = attach_upstream(with_weather, upstream_features, lag_days=1)
check("attach_upstream adds prefixed columns",
      "upstream_rapid_rise_flag" in with_upstream.columns and "upstream_gage_height_ft" in with_upstream.columns)
# incident is 2024-01-02 -- with lag_days=1, it should see Bent Creek's 2024-01-01 reading (no rise yet)
check("attach_upstream shifts dates forward by lag_days, not same-day",
      with_upstream.iloc[0]["upstream_gage_height_ft"] == 3.0)

try:
    attach_upstream(with_upstream, upstream_features)
    check("attach_upstream raises on double-attach", False)
except ValueError:
    check("attach_upstream raises on double-attach", True)

# 10. risk_index's upstream-surge bonus fires correctly
no_upstream = score(RiskInputs(gage_height_ft=3.5))
with_upstream_surge = score(RiskInputs(gage_height_ft=3.5, upstream_rapid_rise_flag=True))
check("upstream surge increases the score", with_upstream_surge["risk_index_1_10"] > no_upstream["risk_index_1_10"])

print()
if failures:
    print(f"{len(failures)} check(s) failed: {failures}")
else:
    print("All checks passed.")

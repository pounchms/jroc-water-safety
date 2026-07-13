"""
Incident <-> environmental join and feature engineering.

Design intent: RFD incident records do NOT include river/weather conditions
at all (confirmed in Divya's NFIRS/NERIS crosswalk -- "Environmental
conditions are not captured in the incident record"). So this module takes
SEPARATE inputs -- a bare incident table (date/time/location only), a local
river conditions series, an optional weather series, and an optional
upstream-gauge series -- and does the join + derived-feature work ourselves.
It never reads pre-computed environmental columns off an incident table.

This matters for the synthetic dataset specifically: Divya's synthetic file
bakes gage_height_ft/discharge_cfs/delta_gage_24h_ft/rapid_rise_flag directly
onto each incident row, which is fine for a demo but means those columns are
circular by construction (see risk_index.py docstring). Real RFD data will
NOT come with those columns attached -- this module is what attaches them,
and it's built and tested against real USGS history, not synthetic shortcuts.

Usage:
    from join_features import build_environmental_features, join_incidents, attach_weather, attach_upstream

    env = build_environmental_features(daily_env_df)  # adds deltas/flags
    joined = join_incidents(incidents_df, env)
    joined = attach_weather(joined, weather_daily)     # optional
    joined = attach_upstream(joined, upstream_features) # optional, leading indicator
"""

import pandas as pd

RAPID_RISE_THRESHOLD_FT = 1.5  # gage-height rise over 24h that counts as "rapid"
DECEPTIVE_CALM_LOCAL_MAX_FT = 6.0  # local gage still reads Low/Moderate...
DECEPTIVE_CALM_DELTA_MIN_FT = 1.0  # ...but has already risen this much in 24h


def build_environmental_features(
    env_daily: pd.DataFrame,
    level_col: str = "gage_height_ft",
    rapid_rise_threshold: float = RAPID_RISE_THRESHOLD_FT,
    deceptive_calm_local_max: float = DECEPTIVE_CALM_LOCAL_MAX_FT,
    deceptive_calm_delta_min: float = DECEPTIVE_CALM_DELTA_MIN_FT,
) -> pd.DataFrame:
    """Given a daily environmental series (columns: date, <level_col>),
    derive rate-of-rise and danger-signal flags. Requires the series sorted
    by date with no assumption of even spacing -- missing days are tolerated
    (short gaps are interpolated), since real USGS history occasionally has
    gaps.

    level_col defaults to gage_height_ft (the real danger signal per the
    project plan) but the thresholds are parameterized so the same function
    can run against discharge_cfs or any other level series (e.g. an
    upstream gauge's own readings) -- useful today since real historical
    gage height requires a chunked iv backfill (see
    usgs_history.backfill_daily_gage_height) while discharge has full
    daily-value history available in one call. Threshold defaults are tuned
    for gage-height feet; pass different thresholds for other units.
    """
    df = env_daily.sort_values("date").reset_index(drop=True).copy()
    df["date"] = pd.to_datetime(df["date"])

    df = df.set_index("date")
    full_index = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full_index)
    df.index.name = "date"
    df[level_col] = df[level_col].interpolate(limit=3)

    df["level_24h_ago"] = df[level_col].shift(1)
    df["level_72h_ago"] = df[level_col].shift(3)
    df["delta_24h"] = (df[level_col] - df["level_24h_ago"]).round(2)
    df["delta_72h"] = (df[level_col] - df["level_72h_ago"]).round(2)

    df["rapid_rise_flag"] = df["delta_24h"] >= rapid_rise_threshold
    df["deceptive_calm_flag"] = (
        (df[level_col] <= deceptive_calm_local_max)
        & (df["delta_24h"] >= deceptive_calm_delta_min)
    )

    return df.drop(columns=["level_24h_ago", "level_72h_ago"]).reset_index()


def join_incidents(incidents: pd.DataFrame, env_features: pd.DataFrame,
                    incident_date_col: str = "incident_date") -> pd.DataFrame:
    """Attach same-day environmental features to each incident. incidents
    must NOT already carry environmental columns -- this is the attach step,
    not a merge-and-hope. Raises if incidents already has gage_height_ft to
    catch accidental double-joins."""
    already_joined = {"gage_height_ft", "discharge_cfs", "delta_gage_24h_ft", "rapid_rise_flag"} & set(incidents.columns)
    if already_joined:
        raise ValueError(
            f"incidents table already has environmental column(s) {already_joined} -- "
            "pass the bare incident intake (no pre-attached environmental columns)."
        )

    inc = incidents.copy()
    inc["_join_date"] = pd.to_datetime(inc[incident_date_col])
    env = env_features.copy()
    env["date"] = pd.to_datetime(env["date"])

    merged = inc.merge(env, left_on="_join_date", right_on="date", how="left")
    merged = merged.drop(columns=["_join_date"])

    # response/duration features derived from the incident's own timestamps,
    # not the environmental join -- kept here since they're part of the same
    # "features ready for modeling" output
    for col in ("alarm_datetime", "arrival_datetime", "cleared_datetime"):
        if col in merged.columns:
            merged[col] = pd.to_datetime(merged[col])

    if {"alarm_datetime", "arrival_datetime"}.issubset(merged.columns):
        merged["response_time_min"] = (
            (merged["arrival_datetime"] - merged["alarm_datetime"]).dt.total_seconds() / 60
        ).round(1)
    if {"alarm_datetime", "cleared_datetime"}.issubset(merged.columns):
        merged["incident_duration_min"] = (
            (merged["cleared_datetime"] - merged["alarm_datetime"]).dt.total_seconds() / 60
        ).round(1)

    return merged


def attach_weather(joined: pd.DataFrame, weather_daily: pd.DataFrame,
                    incident_date_col: str = "incident_date") -> pd.DataFrame:
    """Attach same-day weather (air_temp_f, wind_mph, precip_24h_in) to a
    table that has already been through join_incidents(). Kept as a separate
    step rather than folded into join_incidents() -- river data and weather
    data come from two different APIs (USGS vs NWS, see usgs_history.py and
    nws_weather.py) on two different schedules, so they're attached one at a
    time rather than forced through a single merge.

    weather_daily must have columns: date, air_temp_f, wind_mph,
    precip_24h_in (see nws_weather.daily_weather_features).
    """
    already = {"air_temp_f", "wind_mph", "precip_24h_in"} & set(joined.columns)
    if already:
        raise ValueError(
            f"table already has weather column(s) {already} -- attach_weather "
            "already ran, or these came from somewhere else."
        )

    out = joined.copy()
    out["_join_date"] = pd.to_datetime(out[incident_date_col])
    weather = weather_daily.copy()
    weather["date"] = pd.to_datetime(weather["date"])

    merged = out.merge(
        weather[["date", "air_temp_f", "wind_mph", "precip_24h_in"]],
        left_on="_join_date", right_on="date", how="left", suffixes=("", "_weather"),
    )
    merged = merged.drop(columns=["_join_date"])
    if "date_weather" in merged.columns:
        merged = merged.drop(columns=["date_weather"])
    return merged


def attach_upstream(joined: pd.DataFrame, upstream_features: pd.DataFrame,
                     lag_days: int = 1, incident_date_col: str = "incident_date") -> pd.DataFrame:
    """Attach an UPSTREAM gauge's own delta/rapid-rise signal to each
    incident, shifted forward by lag_days -- i.e. an incident on date X gets
    the upstream gauge's reading from date (X - lag_days), because that's
    what the water was doing before it reached Richmond.

    upstream_features must already be run through build_environmental_features
    (so it has date, <level_col>, delta_24h, delta_72h, rapid_rise_flag,
    deceptive_calm_flag). All of those columns get an "upstream_" prefix here
    to keep them distinct from the local river's own columns.

    lag_days defaults to 1, based on a real cross-correlation of 2024 Bent
    Creek vs Richmond discharge (see upstream_lag.py for the analysis and
    important precision caveats -- this is an evidence-based placeholder,
    not a settled physical constant).
    """
    already = {c for c in joined.columns if c.startswith("upstream_")}
    if already:
        raise ValueError(
            f"table already has upstream column(s) {already} -- attach_upstream "
            "already ran."
        )

    upstream = upstream_features.copy()
    upstream["date"] = pd.to_datetime(upstream["date"])
    # shift the upstream reading FORWARD by lag_days so it lines up with the
    # Richmond-side date it's predictive of
    upstream["effective_date"] = upstream["date"] + pd.Timedelta(days=lag_days)

    rename_cols = {c: f"upstream_{c}" for c in upstream.columns if c not in ("date", "effective_date")}
    upstream = upstream.rename(columns=rename_cols)

    out = joined.copy()
    out["_join_date"] = pd.to_datetime(out[incident_date_col])

    merged = out.merge(
        upstream.drop(columns=["date"]),
        left_on="_join_date", right_on="effective_date", how="left",
    )
    merged = merged.drop(columns=["_join_date", "effective_date"])
    return merged

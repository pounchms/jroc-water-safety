"""
Public river risk index (1-10), rule-based, per the project plan (section 6.4):
"a composite 1-10 risk index built first from defensible rules, then
validated and re-weighted against historical incidents as data arrives."

IMPORTANT -- what this file is NOT:
It is not tuned, fit, or validated against Divya's synthetic dataset's
risk_index_1_10 column. That column correlates with gage_height_ft/
discharge_cfs/delta_gage_24h_ft at r=0.84-0.89 by construction (she built
incident severity to track the environmental fields she generated), so
"matching" it would prove nothing about whether these weights are good --
it would just prove the synthetic data is internally consistent with
itself. Treat any comparison against it as a plumbing/plausibility check,
not validation. Real validation has to wait for actual RFD incidents
([[jroc-rfd-data-status]]).

Components (weights are placeholders, documented so they're easy to revisit
once real incidents can be regressed against):
  - base level score (0-6): from gage height thresholds, reusing the same
    breakpoints as the live dashboard's get_risk_level() in usgs_james_river.py
    (Low <4ft, Moderate 4-8ft, High 8-14ft, Flood 14ft+) so the public-facing
    index and the live dashboard's risk_level never disagree with each other.
  - rate-of-rise bonus (0-2): rewards rapid_rise_flag -- the "deceptively
    calm" danger the whole project is built around.
  - deceptive-calm bonus (0-2): extra weight specifically when the local
    reading looks safe but the rise signal says otherwise, since sponsor
    interviews called this out as the dangerous edge case, not just "high
    water is dangerous" (which is already captured by the base score).
  - heavy-rain bonus (0-1): recent rain is a LEADING indicator -- it predicts
    a rise that the gage hasn't shown yet. Deliberately small (the gage-based
    signals above are the ones the project plan calls dominant); this is
    meant to nudge the score a little early, not compete with rate-of-rise.
  - wind bonus (0-1): higher wind raises capsizing/hypothermia risk for
    swimmers and small craft independent of river level -- a real but
    secondary factor per the project plan (river level/rate-of-rise are the
    dominant signal, weather is supporting context).
  - upstream-surge bonus (0-2): the James can rise 3-4ft in hours when rain
    falls in the watershed upstream while Richmond still looks calm -- this
    is literally the project's founding example of the danger this tool
    exists to catch. An upstream gauge (Bent Creek, 02026000) already
    rising fast is a stronger, more direct early-warning signal than local
    weather, because it measures water already moving downstream instead of
    modeling where rain will end up. See upstream_lag.py for why lag=1 day
    is used (empirical, from real 2024 discharge cross-correlation, not a
    guess) and its precision caveats.

Weather (air temp, precipitation, wind) and the upstream gauge signal now
feed scoring via nws_weather.py / usgs_upstream.py + join_features.py's
attach_weather() / attach_upstream(). IMPORTANT CAVEAT: nws_weather.py was
written but never fetched real data in this session -- api.weather.gov was
unreachable from this sandbox (see nws_weather.py docstring). The upstream
gauge, by contrast, WAS fetched and tested against real 2024 data (see
upstream_lag.py) -- so the upstream bonus rests on firmer ground than the
weather bonuses, even though all of these weights are still placeholders
pending real incident validation.
"""

from dataclasses import dataclass


BASE_SCORE_BREAKPOINTS = [
    (4.0, 1),   # Low: gage height < 4 ft
    (8.0, 3),   # Moderate: 4-8 ft
    (14.0, 5),  # High: 8-14 ft
    (float("inf"), 6),  # Flood: 14 ft+
]

RAPID_RISE_BONUS = 2
DECEPTIVE_CALM_BONUS = 2
HEAVY_RAIN_BONUS = 1
HEAVY_RAIN_THRESHOLD_IN = 1.0  # NWS "heavy rain" is loosely >0.3in/hr; 1in/24h as a rough proxy
HIGH_WIND_BONUS = 1
HIGH_WIND_THRESHOLD_MPH = 20.0  # roughly NWS small-craft-advisory territory
UPSTREAM_SURGE_BONUS = 2  # matches rapid_rise/deceptive_calm weight -- this is a primary signal, not a minor one


@dataclass
class RiskInputs:
    gage_height_ft: float
    rapid_rise_flag: bool = False
    deceptive_calm_flag: bool = False
    precip_24h_in: float | None = None
    wind_mph: float | None = None
    upstream_rapid_rise_flag: bool = False


def _base_score(gage_height_ft: float) -> int:
    for breakpoint, score in BASE_SCORE_BREAKPOINTS:
        if gage_height_ft < breakpoint:
            return score
    return BASE_SCORE_BREAKPOINTS[-1][1]


def score(inputs: RiskInputs) -> dict:
    """Returns the 1-10 index plus a breakdown, so the public-facing display
    can show *why* the number is what it is -- transparency was an explicit
    design goal (interpretable score over black-box model, per project plan
    section 6.4)."""
    base = _base_score(inputs.gage_height_ft)
    bonus = 0
    reasons = [f"base level score {base} (gage height {inputs.gage_height_ft:.1f} ft)"]

    if inputs.rapid_rise_flag:
        bonus += RAPID_RISE_BONUS
        reasons.append(f"+{RAPID_RISE_BONUS} rapid rise in last 24h")
    if inputs.deceptive_calm_flag:
        bonus += DECEPTIVE_CALM_BONUS
        reasons.append(f"+{DECEPTIVE_CALM_BONUS} deceptive calm (local level low/moderate but rising fast)")
    if inputs.precip_24h_in is not None and inputs.precip_24h_in >= HEAVY_RAIN_THRESHOLD_IN:
        bonus += HEAVY_RAIN_BONUS
        reasons.append(f"+{HEAVY_RAIN_BONUS} heavy rain in last 24h ({inputs.precip_24h_in:.1f} in) -- rise may not show in gage yet")
    if inputs.wind_mph is not None and inputs.wind_mph >= HIGH_WIND_THRESHOLD_MPH:
        bonus += HIGH_WIND_BONUS
        reasons.append(f"+{HIGH_WIND_BONUS} high wind ({inputs.wind_mph:.0f} mph)")
    if inputs.upstream_rapid_rise_flag:
        bonus += UPSTREAM_SURGE_BONUS
        reasons.append(f"+{UPSTREAM_SURGE_BONUS} upstream gauge (Bent Creek) rising fast -- expect a local rise in ~1 day")

    total = max(1, min(10, base + bonus))
    return {
        "risk_index_1_10": total,
        "base_score": base,
        "bonus": bonus,
        "reasons": reasons,
    }


def public_risk_level(risk_index_1_10: int) -> str:
    """Maps the 1-10 index to the same plain-language categories already
    used on the live dashboard, so the two labeling schemes stay consistent
    for the public."""
    if risk_index_1_10 <= 2:
        return "Low"
    if risk_index_1_10 <= 5:
        return "Moderate"
    if risk_index_1_10 <= 8:
        return "High"
    return "Flood"


if __name__ == "__main__":
    # a few illustrative cases, not a fit to any dataset
    examples = [
        RiskInputs(gage_height_ft=3.5),
        RiskInputs(gage_height_ft=3.5, rapid_rise_flag=True, deceptive_calm_flag=True),
        RiskInputs(gage_height_ft=9.0),
        RiskInputs(gage_height_ft=15.0, rapid_rise_flag=True),
        RiskInputs(gage_height_ft=3.5, precip_24h_in=1.5, wind_mph=25),
        RiskInputs(gage_height_ft=3.2, upstream_rapid_rise_flag=True),
    ]
    for ex in examples:
        result = score(ex)
        print(f"gage={ex.gage_height_ft}ft rapid_rise={ex.rapid_rise_flag} "
              f"deceptive_calm={ex.deceptive_calm_flag} upstream_rapid_rise={ex.upstream_rapid_rise_flag} -> "
              f"index={result['risk_index_1_10']} ({public_risk_level(result['risk_index_1_10'])})")
        for r in result["reasons"]:
            print(f"    {r}")

"""
Empirical basis for the upstream lag used in join_features.attach_upstream()
and risk_index.py's upstream-surge bonus.

Method (run 2026-07-13, real data, not assumed): pulled real 2024 daily mean
discharge (00060) for both the Richmond gauge (02037500) and the Bent Creek
gauge (02026000, ~60 river miles upstream) directly from USGS NWIS, then
cross-correlated Bent Creek's discharge at lag L against Richmond's same-day
discharge, for L = 0..5 days, on the full 366-day 2024 series.

Results:
    lag=0d  corr=0.8313
    lag=1d  corr=0.8842   <- best
    lag=2d  corr=0.6812
    lag=3d  corr=0.5258
    lag=4d  corr=0.4016
    lag=5d  corr=0.2876

UPSTREAM_LAG_DAYS = 1 is therefore an evidence-based choice, not a guess --
a surge at Bent Creek today is most predictive of Richmond's flow tomorrow.

Important precision caveat: this analysis used DAILY-averaged discharge.
Daily averaging can only resolve lag to whole-day granularity -- the true
physical travel time could be anywhere from ~12 to ~36 hours and still show
up as "1 day" in this analysis. If/when finer-grained (hourly or 15-min)
gage height history is backfilled for both stations, re-run this
cross-correlation at sub-daily resolution for a tighter estimate. Treat
UPSTREAM_LAG_DAYS=1 as "good enough to build the feature and test the
pipeline," not as a validated precise travel time.

Also note: this correlates DISCHARGE (cfs) at both stations, not GAGE HEIGHT
(ft) -- discharge was used because it has full daily-value history in one
API call, while gage height doesn't (see usgs_history.py docstring). Gage
height and discharge move together but aren't identical; re-validate with
gage height once a historical gage-height backfill exists for both stations.
"""

UPSTREAM_LAG_DAYS = 1

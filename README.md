# JROC James River Water Safety: Practicum Proof of Concept

VCU MDA practicum, 2026, for the James River Outdoor Coalition (JROC). Team: Matt Pounch, Divya, Jon, Paris.

**The question JROC asked:** when someone is rescued or drowns on the James in Richmond, where did it happen? The answer lives in free-text fire department notes. This repo shows those notes can be turned into map points automatically, and measures how often that goes wrong.

**Status, December 2026:** proof of concept on **synthetic** incidents. No real Richmond Fire Department data was ever shared. The map demonstrates the method; it is not a picture of where people actually get hurt.

## What's here

| Folder / file | What it is | Runs by itself? |
|---|---|---|
| `poc_location_pipeline/` | The main deliverable: synthetic incidents, firefighter-style notes, the location parser, its evaluation, and the file the Tableau dashboard reads (`incidents_public.csv`). **Start with its README.** | No, run by hand (see below) |
| `poc_location_pipeline/fetch_river_history.py` | Adds real USGS river flow, and its 24/72-hour change, to any CSV of incidents by date. **Use this when real incident data arrives.** | Run by hand |
| `.github/workflows/` + `usgs_james_river.py`, `pipeline/usgs_upstream.py`, `pipeline/nws_weather.py` | Three daily jobs that saved Richmond river readings (USGS 02037500), the upstream gauge at Bent Creek (USGS 02026000), and Richmond weather (NWS KRIC). **Disabled October 2026**; see below. | No (disabled) |
| `pipeline/` | Shared code. `join_features.py` computes the 24/72-hour river changes; `fetch_river_history.py` and the fixes in `poc_location_pipeline` use it. `risk_index.py` is the July rule-based risk score; it was never validated, and only the disabled river job still imports it. `usgs_history.py`, `upstream_lag.py` and `build_historical_incidents.py` are the one-time backfill and analysis scripts behind the incident dataset. | Run by hand |
| `james_river_incidents_full.csv` | 40 historical incidents from the American Whitewater accident database. Set aside as too sparse; kept for reference. | n/a |

## The location pipeline in one paragraph

Each note is read by a free AI model running on a local computer (Qwen 2.5 14B via Ollama, no paid API, nothing sent to the cloud). The model only names which of 12 river sites the person was at. Plain code does everything that must be exact: finding GPS or what3words strings, looking up coordinates, and rejecting any point more than 500 m from the river. On 39 notes written by a different AI system (ChatGPT), the parser found the right site 37 times, against 31 for a rules-only parser. Details, every decision, and all caveats: `poc_location_pipeline/README.md`.

To rerun it (Windows, from `poc_location_pipeline/`, with [Ollama](https://ollama.com) installed and `ollama pull qwen2.5:14b` done):

```
set LLM_BACKEND=ollama
python llm_extract.py mainllm
python extract_locations.py
python evaluate_parsers.py
```

Results are cached, so a rerun on unchanged notes takes seconds.

## Keeping it running after handoff

- **Nothing in this repo runs on a schedule anymore.** From July to October 2026, three daily jobs saved river and weather readings. They were disabled on purpose in October 2026 for three reasons:
    - **USGS already keeps its full history,** so a daily copy adds nothing. `fetch_river_history.py` pulls flow for any past dates when it's needed. Its first live run gave all 519 incidents a reading.
    - **A failed run emails the repo owner,** which after handoff would be JROC volunteers.
    - **The jobs call a USGS service** (`waterservices.usgs.gov`) that USGS is replacing, so they would eventually break.

  The workflow files are still here. To turn a job back on: GitHub, then **Actions**, then pick the workflow, then **Enable workflow**. While they ran, they worked unattended. Failures were rare and came from short USGS outages, which a retry fix in September 2026 covered.
- **Ownership:** this repo, the `jrocwatersafety@gmail.com` account and the Tableau Public dashboard must belong to JROC, not a student. The checklist and account details are in `JROC_Handoff_Guide.md`, which is kept **outside** this repo on purpose. Never commit passwords, API keys or service-account files here; `.gitignore` blocks the common ones.
- **Retired in September and October 2026:** the Google Sheets sync, the daily risk-index job, the GitHub Pages site, the planned near-miss QR form, and the early demo and test scripts. Their code is still in git history: `git checkout 12c0286` shows the repo as it was before the cleanup.

## What would make this real

Real RFD incident narratives under a data-sharing agreement, and a count of how many people are on the river each day. Without the second, river conditions can't be compared fairly against incident counts (see the exposure section of the location pipeline README).

Map data © OpenStreetMap contributors (ODbL). River data: U.S. Geological Survey. Weather: National Weather Service.

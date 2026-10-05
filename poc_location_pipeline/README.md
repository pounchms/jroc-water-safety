# POC location pipeline: from firefighter notes to map points

> **Start here (updated 2026-10-05).** This README grew as the project did. Sections are in the order things happened, so **the early results tables are out of date**. The current numbers are in the last sections: *Second run*, *Site check*, *Dates and river readings fixed* and *Cross-model test set*.
>
> | Test set | Notes | Rules F1 | Local AI F1 |
> |---|---|---|---|
> | Cross-model (ChatGPT-written, labels checked) | 39 | 0.80 | 0.97 |
> | Claude-written, cancelled calls excluded | 374 | 0.90 | 0.98 |
> | Hard set | 38 | 0.70 | 0.95 |
>
> **Current run order** (Windows cmd, this folder, Ollama running): `set LLM_BACKEND=ollama`, then `python llm_extract.py mainllm`, `python llm_extract.py hard`, `python llm_extract.py holdout`, `python extract_locations.py`, `python evaluate_parsers.py`. **Don't rerun** `generate_narratives.py`, `generate_narratives_llm.py`, `migrate_site_coords.py` or `fill_discharge_and_trim.py`. They're one-time steps; rerunning them rebuilds the data and breaks the match between notes and answer key.

**What it shows:** incident locations can be pulled automatically out of free-text narratives written in the formats firefighters actually use (what3words, GPS, place names, vague descriptions). The output is a CSV you can open in Tableau.

**What it does NOT show:** where incidents really happen. The incidents and their coordinates are synthetic (from Divya's generator). A map made from them only reflects the assumptions built into the generator. Say this plainly in the report.

## Run

```
python generate_narratives.py   # adds firefighter_narrative + truth columns
python extract_locations.py     # parses narratives -> map_lat / map_lon
```

Needs pandas only. With no `W3W_API_KEY` set, what3words runs in **mock mode**: the generator makes up three-word strings and stores them in `w3w_cache.json`. They are not real addresses.

For a live run, open a terminal in this folder and run `set W3W_API_KEY=yourkey` (Windows cmd), then delete `w3w_cache.json` and rerun both steps. **Never write the key into a file inside `data/`. That folder is a git repo.** The free plan doesn't include conversion in either direction. Basic ($9.99/mo, 1,000 requests) covers both directions. A full run uses under 100 requests, and every result is cached, so one month of the plan covers the whole project.

## Where simulated incidents are placed

`generate_narratives.py` puts every simulated incident **on the water**, using the James River centerline in `river_centerline.csv` (86 points from OpenStreetMap). **Map data © OpenStreetMap contributors (ODbL). The dashboard must credit this.**

- 65% of incidents fall near their site, typically within ~150 m along the river.
- 35% fall anywhere along the river between Huguenot and below Ancarrow's Landing. They're attributed to the nearest site.
- Every incident sits within 50 m of the centerline.

These shares are assumptions (`SPREAD_SHARE`, `SITE_SPREAD_M`, `BANK_OFFSET_M`). The off-river check also uses the centerline now: exact points more than 500 m from the river are rejected.

## what3words licence rules (read before the live run)

The [API licence](https://what3words.com/api-licence-agreement) puts two limits on this project:

- **6.3(b):** You can't show a three-word address next to its coordinates to anyone outside the team. So `extract_locations.py` writes two files. `incidents_geocoded.csv` is **internal only**. `incidents_public.csv` has every `///address` replaced with `///[w3w]`. **Tableau Public only gets the public file.**
- **6.3(e):** Converted coordinates can only be stored for up to 30 days. **Delete `w3w_cache.json` within 30 days of the live run.** Don't commit it to git and don't email it around. The map points in the public file were derived from those coordinates, so plan to publish the final dashboard close to the live run.

Put this in the report too. It's a real adoption barrier for RFD and JROC, alongside cost.

## Files

| File | Purpose |
|---|---|
| `river_sites.csv` | Place-name lookup table: 12 river sites plus aliases and common misspellings. **Coordinates are the centroids of Divya's synthetic points and haven't been checked on a map yet (`coords_verified = no`). Checking them is task V-03 on the project board.** |
| `w3w_client.py` | what3words lookups plus cache (mock or live) |
| `generate_narratives.py` | Writes synthetic narratives. The `FORMAT_MIX` shares (how often each location format appears) are assumptions, not data. |
| `extract_locations.py` | Parser. Tries what3words first, then GPS, then an exact place-name match, then a fuzzy match. Anything left is unplaced. |
| `incidents_geocoded.csv` | Internal full output, including the what3words addresses. **Don't publish it.** |
| `incidents_public.csv` | **Tableau input.** Same columns, with the addresses redacted. Use `map_lat`/`map_lon`, filter on `placed = TRUE`, and color or filter by `location_method` |
| `location_match_summary.csv` | Placement rate and error distance for each method |

## Results from the current run (seed 42, mock mode)

| Method | Share of incidents | Median error |
|---|---|---|
| Place name (exact + fuzzy) | 46% | ~520 m |
| what3words | 14% | 0 m (mock, see below) |
| GPS | 10% | <1 m |
| what3words typo caught (off-river or unresolved) | 2% | not placed, by design |
| Unplaced (vague or no location) | 28% | not placed |

70% of incidents get placed on the map. For the report, the takeaway is the **30% that can't be placed plus the ~520 m error on place names**. That's the case for asking RFD to use a consistent location convention, and it holds whether or not the convention is what3words.

## Frozen schema for the dashboard

The columns in `incidents_public.csv` won't change when the live what3words run happens. Only the values in the what3words rows will. The dashboard can be built now against the mock file.

| Column | Use in Tableau |
|---|---|
| `map_lat`, `map_lon` | Map point. Only filled in when `placed = TRUE` |
| `location_method` | what3words / gps / landmark_exact / landmark_fuzzy / w3w_off_river / w3w_unresolved / unplaced |
| `location_confidence` | 1.0 for exact matches, the fuzzy-match score for typo'd place names, 0 when unplaced |
| `map_layer` | `site_circle` (place-name hits, all on a site's center: draw **one circle per site, sized by count**) · `precise_point` (what3words/GPS: draw **individual dots**) · `not_mapped`. Don't use a density heatmap. It would turn the 243 site-center hits into 12 hotspots and make the data look more precise than it is. |
| `sim_error_m` | How far the parsed point is from the simulated true location. A test score for the simulation, not something RFD data would have |
| `firefighter_narrative` | Tooltip text |
| `parsed_site` | The river site nearest the parsed point. Use it for site rollups |
| `incident_class`, `fatality_count`, `rescued_count`, `alarm_datetime`, `discharge_cfs`, ... | Divya's original columns, unchanged |
| *(removed)* | The simulation's answer key (`truth_lat`/`truth_lon`, `location_format_true`, `landmark_cross_street`, `river_section`, `location_quality`) exists only in the internal `incidents_geocoded.csv`. Real RFD data wouldn't have these columns, so the dashboard never sees them. |

## Caveats to state before a grader does

1. **The 0 m what3words error is built in.** Mock mode stores the true point. Live what3words returns the center of a 3 m square, so real error is about 2 m at most.
2. **Typos are handled with a distance check.** With the real service, a misspelled address often turns out to be a *different valid address* somewhere else. This is the main published criticism of what3words for emergency use. The extractor now rejects any what3words or GPS point more than 500 m from the river centerline (`w3w_off_river`). Mock mode imitates the problem by sending 70% of typo'd addresses to a random location far away. Live mode needs no imitation.
3. **The ~520 m place-name error comes from two things.** First, 35% of simulated incidents happen between sites. A firefighter names the nearest site, so the point lands at that site's center. Second, the site coordinates haven't been verified, and several are hundreds of meters from the river (task V-03). Once V-03 is done, expect this number to drop. Report it as "hundreds of meters," not as a precise figure.
4. **There's no evidence RFD uses what3words.** Present it as one of several formats the pipeline can handle, not as how RFD works today.

## LLM parser and parser comparison (optional, for the NLP analysis)

`llm_extract.py` uses Claude to **read** each note. It never produces coordinates. The model must answer through a fixed tool schema: a site ID from `river_sites.csv`, an off-list place name, a copied what3words address or GPS string, and the exact words from the note that support its answer. Coordinates then come from the same deterministic code as the rule parser: site centers, the what3words lookup, and the 500 m off-river check. So the model can't invent a point on the river. The design is **rules → LLM → rules**.

```
set ANTHROPIC_API_KEY=sk-ant-...        (never put the key in a file)
python llm_extract.py hard              # 38 hard notes, costs a few cents
python llm_extract.py main --limit 100  # first 100 simulated notes
python evaluate_parsers.py              # precision / recall / F1 for each parser
```

- **Model:** defaults to `claude-sonnet-5`. To use another, set `ANTHROPIC_MODEL`. Haiku 4.5 is cheaper, but it's scheduled to retire as early as 2026-10-15, which is before the presentation.
- **Cache:** every answer is saved in `llm_cache.json`, so reruns are free and results can be reproduced.
- **`hard_test_notes.csv`:** 38 hand-written notes built to break the rules: negation or corrections, two places in one note, places not on the site list, abbreviations, relative directions, noisy GPS, and typos. The `gold_site` column is **where the patient was located**. `OFFLIST` and `NONE` mean the right answer is to leave the note off the map.
- **spaCy baseline:** optional. Install with `pip install spacy` and `python -m spacy download en_core_web_sm`. It hasn't been test-run yet because spaCy couldn't be installed in the build environment.

**Rule parser scores so far:**

| Test set | Precision | Recall | F1 |
|---|---|---|---|
| Simulated notes (first 100) | 1.00 | 1.00 | 1.00 |
| Hard notes (38) | 0.77 | 0.71 | 0.74 |

The perfect score on simulated notes happens because those notes were generated from the same templates and site nicknames the parser uses. The hard set is the honest test. The rules fail on notes that mention two places, like "Pt lives on Pony Pasture Rd … found at Reedy Creek." They also fail on abbreviations like "PP rapids."

## Claude-written narratives (the honest main test set)

The template notes above are written from the same nicknames the rule parser matches, so its 1.00 on them measures nothing. `generate_narratives_llm.py` rewrites every note with Claude from the incident's facts and the **canonical** site name only. It never sees the alias list. Incidents, true points, location formats and the exact what3words/GPS strings are copied unchanged, so the only thing that differs between the two files is the wording.

```
set ANTHROPIC_API_KEY=sk-ant-...
python generate_narratives_llm.py --dry-run      # show prompts, no API calls
python generate_narratives_llm.py --limit 100    # or no --limit for all 525
python llm_extract.py mainllm --limit 100
python evaluate_parsers.py                       # adds a main_llm row + per-variant table
```

- **Answer key comes from the script, never the model.** `gold_site`, `decoy_site` and `style_variant` are drawn with a fixed seed before any API call. If the model drops a required ///address or GPS string, it's retried once and then flagged `gen_ok = False`; the evaluator excludes those rows.
- **Style variants** (`STYLE_MIX`, an assumption): plain, dispatch_mismatch (wrong dispatch site + real location), residence (patient's home street), relative ("150 yds below X", place-name notes only), shorthand.
- **Limitation to state:** the same model family writes and reads these notes. The generator/parser separation (different prompts, no shared alias list, gold outside the model) reduces that, but doesn't remove it. The human-written holdout set is the independent check.
- Output: `incidents_with_narratives_llm.csv` (keeps the template text in `firefighter_narrative_template`), cache in `gen_cache.json`.

## Running for free

**Parser on your own GPU (Ollama).** Install Ollama for Windows (ollama.com), then in cmd:

```
ollama pull qwen2.5:14b
set LLM_BACKEND=ollama
python llm_extract.py hard
python llm_extract.py mainllm --limit 100
python evaluate_parsers.py
```

Same prompt and answer fields as the Claude path; the answer is forced into a JSON schema instead of a tool call. Run `ollama ps` while it works: the PROCESSOR column should say GPU. If it says CPU, the Radeon isn't being used and it will be slow. `set OLLAMA_MODEL=llama3.1:8b` to try a smaller model. Note that `llm_*_results.csv` holds whichever backend ran last.

For the report: a local model is a point in its own right. Real RFD narratives couldn't be sent to a cloud API without a data agreement; a model on a department PC could read them.

**Narratives without an API key.** `--export` writes the prompts to a file, a Claude chat session writes the notes, `--import` loads them. The same verbatim-address checks and answer key apply. Say in the report that the notes were written in a Claude session from these prompts.

## Decisions after the first scoring run (2026-09-28)

- **What the map shows:** where crews reached a person. Calls cancelled before contact (`incident_class = good_intent_cancelled`) are left off the map using that structured field, not by parsing the note, and aren't scored. The dashboard must filter them out the same way. NFIRS already records cancellation, so a parser shouldn't be asked to infer it.
- **Vague notes regenerated (prompt g2).** The first prompt used "downstream of the island" as an example of a vague location, but to a Richmond reader that means Belle Isle. The answer key called those notes unplaceable, so LLM parser "false alarms" on them were partly the key's fault. All 75 vague notes were rewritten by a fresh sub-agent with no nameable features. First-run scores are kept in this section's history for the report.
- **First-run result (before these fixes), 525 notes:** rules F1 0.912, local Qwen 2.5 14B F1 0.949. On dispatch-mismatch notes, 55/58 for the LLM vs 32/58 for rules.
- **Second run (after both fixes), 376 notes with cancelled calls excluded:** rules F1 0.902; local Qwen 2.5 14B F1 0.981 (precision 0.977, recall 0.985, 5 false alarms). Dispatch mismatch: 44/45 LLM vs 23/45 rules. Report both runs, not only this one.

## Dashboard input (updated 2026-09-28)

`python extract_locations.py` now builds `incidents_public.csv` from the Claude-written notes and the LLM parser's placements (`llm_mainllm_results.csv`). The rules parser's answer is kept alongside in `rules_*` columns. `--rules` rebuilds the old way. New columns:
- `on_map`: **filter the dashboard on this.** It's TRUE only when a point was placed AND the call wasn't cancelled before contact.
- `parser`: which parser produced the point.
- `map_layer`: `site_circle` / `precise_point` / `not_mapped` (cancelled calls are `not_mapped`).

## what3words: staying in mock mode

Decision: no paid what3words plan. The three-word addresses in the data are made up by `w3w_client.py` and are **not real addresses**; they resolve only through `w3w_cache.json`. The report must say so. What this means for the results: the parser's job with a what3words note (spot it, copy it, send it to a lookup) is tested for real; the lookup itself is simulated, including the known failure where a typo resolves to a valid address somewhere else. The API licence limits above only apply to a live run.

## Test notes and site check

- `HOLDOUT_INSTRUCTIONS.md` → team fills `holdout_notes.csv` → `python llm_extract.py holdout` (with `set LLM_BACKEND=ollama`) → `python evaluate_parsers.py` adds a `holdout` row and a table by author.
- `SITE_CHECK_INSTRUCTIONS.md` → task V-03. Afterward, rerun only `extract_locations.py` and `evaluate_parsers.py`.

## Site check (V-03) and re-placement, 2026-09-28

All 12 site coordinates were checked by hand (`coords_verified = yes`). Four moved a long way: Z-Dam ~9 km (it's the Williams Island dam in the upper river, not downtown), Bosher Dam ~4 km, 42nd St ~2 km, Pony Pasture ~1.7 km. The simulated incidents had been placed around the old points, so `migrate_site_coords.py` was run once:
- `river_centerline.csv` extended ~1 km upstream to Bosher Dam with a **straight segment** (the OSM extract stopped short of the dam). Approximation; say so.
- Every incident's true point re-placed around its site's new coordinates, keeping the site named in its note. Only the what3words/GPS tokens in 125 notes changed; every other word is untouched. Z-Dam's `river_section` corrected to Upper Richmond.
- what3words/GPS answer-key sites recomputed from the new points.
- Pre-migration files are in `_pre_sitecheck/`, so the earlier scores can be reproduced.

Median distance from a place-name incident to its site is now 100–480 m (it was up to 9 km for Z-Dam). The Pipeline Rapids point sits ~460 m from the river centerline, which is likely on the walkway, not the water; worth a second look.

## Dates and river readings fixed, 2026-09-28 (`fill_discharge_and_trim.py`)

- Dropped the 6 synthetic incidents dated after 2026-09-28 (Divya's generator ran through Dec 2026). 519 incidents remain.
- Filled river readings for 38 of the 41 incidents after 2026-07-12, where the one-time USGS pull ended. Source: `richmond_discharge_2021_2026_ext.csv` = the original USGS daily values plus daily means of the 15-minute readings the Richmond cron committed to the `data/` repo every day since July. Where both exist (Jul 1–12), they agree within 0.6%. This is the one place the proof of concept actually uses the cron data.
- 3 incidents (Sep 26–28) still have no reading: the cron failed Sep 26–27 on the USGS 503s, and today's run isn't pulled locally yet.
- Originals in `_pre_datefix/`.

## Cross-model test set, 2026-10-05

`holdout_notes.csv` currently holds 39 notes written by ChatGPT (fresh chat, no project context; prompt asked for local nicknames and shorthand). Matt checked every label. One exact duplicate was removed, and in one note the site code "ANC", which GPT copied from the prompt, was changed to "Ancarrows". The original is in `_pre_datefix/holdout_notes_as_received.csv`. **These notes are not human-written.** The team didn't contribute notes.

| Parser | Correct | P | R | F1 |
|---|---|---|---|---|
| Rules | 31/39 | 0.909 | 0.714 | 0.800 |
| Local LLM (qwen2.5:14b) | 37/39 | 0.933 | 1.000 | 0.966 |

- **Rules misses (8):** 6 local short forms not on the alias list ("Pony", "Belle", "Browns Is", "Reedy access", "Hug"), plus 2 multi-place notes where it picked the first place mentioned (Z dam→Pony, Tredegar→Pipe).
- **LLM misses (2):** GP08 "seated on rock mid river", no place named, guessed Belle Isle. GP35 "Floodwall Walk", a real place not on the list, mapped to the nearest known site (Tredegar) instead of off-list. Both are the same failure: it places a point when it should decline.
- n = 39, so one note ≈ 3 points of F1.

## River data after the crons (October 2026)

The daily cron jobs are disabled; see the repo README. To add river conditions to any incident list, including real RFD data, run `python fetch_river_history.py <file.csv>` (needs `pandas` and `requests`). It pulls USGS daily mean flow for the needed dates, computes the 24- and 72-hour changes and the two flags with the same code and thresholds as before, and writes `<file>_with_river.csv` plus the daily series it used. It tries the current USGS API first and falls back to the legacy one. Checked offline against `incidents_public.csv`: identical values on all 519 rows. Not yet run against the live USGS API from this machine, because this session's network couldn't reach USGS. Run it once to confirm.

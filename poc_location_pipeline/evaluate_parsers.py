"""
Compare location parsers on two test sets and report precision / recall / F1.

Parsers
  rules  - extract_locations.py (regex + gazetteer + fuzzy string match)
  spacy  - regex for what3words/GPS, then spaCy pretrained NER for place names,
           each entity matched against the gazetteer (optional: needs
           `pip install spacy` and `python -m spacy download en_core_web_sm`)
  llm    - llm_extract.py results (run that first; skipped if its CSV is missing)

Test sets
  hard  - hard_test_notes.csv: 38 hand-written notes built to break the rules
          (negation, two places, off-list places, abbreviations, directions, typos)
  main  - the first N simulated notes (N = rows in llm_main_results.csv, else 100)

Scoring is by SITE: a predicted point counts as the nearest river site.
Gold is where the patient was located. OFFLIST / NONE = the right answer is "don't place it".
  precision = correct / everything placed        (how often a map point is right)
  recall    = correct / notes with a known site  (how many findable locations were found)

Output: printed tables + parser_eval.csv
"""
from pathlib import Path

import pandas as pd

import w3w_client
from extract_locations import (GPS_RE, W3W_RE, extract, haversine_m, load_gazetteer,
                               match_landmark, near_river)

SITES = pd.read_csv("river_sites.csv")
NAME_TO_ID = dict(zip(SITES.site_name, SITES.site_id))
ALIASES = load_gazetteer()
W3W = w3w_client.load_cache()


def nearest_site(lat, lon):
    if lat is None or pd.isna(lat):
        return None
    return min(SITES.itertuples(), key=lambda s: haversine_m(lat, lon, s.latitude, s.longitude)).site_id


# ---------- parsers ----------
def rules(notes):
    return [nearest_site(r["lat"], r["lon"]) for r in (extract(t, ALIASES, W3W) for t in notes)]


def load_spacy():
    try:
        import spacy
        return spacy.load("en_core_web_sm")
    except Exception:
        return None


def spacy_parser(notes, nlp):
    out = []
    for t in notes:
        m = W3W_RE.search(t)
        if m:
            res = w3w_client.words_to_coords(m.group(1), W3W)
            out.append(nearest_site(*res[:2]) if res and near_river(res[0], res[1], ALIASES) else None)
            continue
        m = GPS_RE.search(t)
        if m:
            lat, lon = float(m.group(1)), -abs(float(m.group(2)))
            out.append(nearest_site(lat, lon) if near_river(lat, lon, ALIASES) else None)
            continue
        pred = None
        for ent in nlp(t).ents:
            if ent.label_ in ("GPE", "LOC", "FAC", "ORG", "PERSON"):  # small model often mislabels place names
                hit = match_landmark(ent.text, ALIASES)
                if hit:
                    pred = hit[0].site_id
                    break
        out.append(pred)
    return out


def llm(path, idx):
    """idx = row positions (in the source notes file) of the notes being scored."""
    if not Path(path).exists():
        return None
    df = pd.read_csv(path)
    if len(df) <= max(idx):
        return None
    df = df.iloc[list(idx)]
    return [nearest_site(a, b) for a, b in zip(df.map_lat, df.map_lon)]


# ---------- scoring ----------
def score(gold, pred):
    c = w = fa = miss = abst = 0
    for g, p in zip(gold, pred):
        has_site = g not in ("NONE", "OFFLIST")
        if p is None:
            miss += has_site
            abst += not has_site
        elif not has_site:
            fa += 1
        elif p == g:
            c += 1
        else:
            w += 1
    placed = c + w + fa
    n_site = c + w + miss
    P = c / placed if placed else float("nan")
    R = c / n_site if n_site else float("nan")
    F = 2 * P * R / (P + R) if P + R else float("nan")
    return dict(n=len(gold), correct=c, wrong_site=w, false_alarm=fa, missed=miss,
                correct_abstain=abst, precision=round(P, 3), recall=round(R, 3), f1=round(F, 3))


def main():
    nlp = load_spacy()
    results, per_cat = [], []

    hard = pd.read_csv("hard_test_notes.csv")
    main_df = pd.read_csv("incidents_with_narratives.csv")
    n_main = len(pd.read_csv("llm_main_results.csv")) if Path("llm_main_results.csv").exists() else 100
    main_df = main_df.head(n_main)
    # Map definition (decided 2026-09-28): the map shows where crews reached a person. Calls
    # cancelled before contact (incident_class good_intent_cancelled, NFIRS 611-type) are left off
    # the map using the STRUCTURED field, not by parsing the note, so they aren't scored here.
    main_df = main_df[main_df.incident_class != "good_intent_cancelled"]
    # Place-name notes: gold = the named site. what3words/GPS notes: gold = the site nearest the
    # true point (the simulated scatter can put a point closer to a neighbouring site).
    main_gold = [NAME_TO_ID[s] if f == "landmark" else nearest_site(la, lo) if f in ("w3w", "gps") else "NONE"
                 for s, f, la, lo in zip(main_df.landmark_cross_street, main_df.location_format_true,
                                         main_df.truth_lat, main_df.truth_lon)]

    sets = [
        ("hard", hard.note.tolist(), hard.gold_site.tolist(), "llm_hard_results.csv", hard.category.tolist(), range(len(hard))),
        ("main", main_df.firefighter_narrative.tolist(), main_gold, "llm_main_results.csv", None, main_df.index),
    ]
    # Claude-written notes: same incidents, unseen phrasing. Gold comes from the generator script,
    # never the model. Rows where the model dropped the required address/coords are excluded.
    if Path("incidents_with_narratives_llm.csv").exists():
        g = pd.read_csv("incidents_with_narratives_llm.csv")
        g = g[(g.gen_ok.astype(str) == "True") & (g.incident_class != "good_intent_cancelled")]
        sets.append(("main_llm", g.firefighter_narrative.tolist(), g.gold_site.tolist(),
                     "llm_mainllm_results.csv", g.style_variant.tolist(), g.index))

    # Holdout (holdout_notes.csv). Current contents: 39 GPT-written notes, labels checked by Matt -
    # a cross-model test set, NOT human-written. See README.
    if Path("holdout_notes.csv").exists():
        h = pd.read_csv("holdout_notes.csv").dropna(subset=["note"]).reset_index(drop=True)
        h["gold_site"] = h.gold_site.astype(str).str.strip().str.upper()
        if len(h):
            sets.append(("holdout", h.note.tolist(), h.gold_site.tolist(), "llm_holdout_results.csv",
                         h.author.fillna("?").tolist(), h.index))

    for set_name, notes, gold, llm_path, cats, idx in sets:
        preds = {"rules": rules(notes)}
        if nlp:
            preds["spacy"] = spacy_parser(notes, nlp)
        lp = llm(llm_path, idx)
        if lp:
            preds["llm"] = lp
        for name, p in preds.items():
            results.append({"test_set": set_name, "parser": name, **score(gold, p)})
            if cats:
                for cat in dict.fromkeys(cats):
                    idx = [i for i, c in enumerate(cats) if c == cat]
                    s = score([gold[i] for i in idx], [p[i] for i in idx])
                    per_cat.append({"test_set": set_name, "parser": name, "category": cat, "n": s["n"],
                                    "right": s["correct"] + s["correct_abstain"]})
        if set_name == "hard":
            detail = hard.assign(**{f"pred_{k}": v for k, v in preds.items()})
            detail.to_csv("hard_test_predictions.csv", index=False)

    res = pd.DataFrame(results)
    res.to_csv("parser_eval.csv", index=False)
    print(res.to_string(index=False))
    if per_cat:
        allc = pd.DataFrame(per_cat)
        for ts, label in [("hard", "Hard set - notes handled correctly, by category:"),
                          ("main_llm", "Claude-written notes - handled correctly, by style variant:"),
                          ("holdout", "Cross-model test set (GPT-written, labels checked by Matt) - handled correctly, by author:")]:
            sub = allc[allc.test_set == ts]
            if sub.empty:
                continue
            pc = sub.pivot(index="category", columns="parser", values="right")
            pc.insert(0, "n", sub.groupby("category").n.first())
            print("\n" + label)
            print(pc.to_string())
    if not nlp:
        print("\n(spaCy baseline skipped: pip install spacy && python -m spacy download en_core_web_sm)")
    if "llm" not in res.parser.values:
        print("(LLM skipped: run llm_extract.py hard / main first)")


if __name__ == "__main__":
    main()

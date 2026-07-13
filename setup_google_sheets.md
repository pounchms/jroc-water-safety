# Google Sheets + Looker Studio Setup

One-time setup. Takes about 25 minutes (was 20 -- two more tabs and two more
scripts got added 2026-07-13).

---

## Step 1: Create the Google Spreadsheet

1. Go to [sheets.google.com](https://sheets.google.com) — sign in as **jrocwatersafety@gmail.com** (the dedicated JROC account, NOT your VCU account — see `JROC_Handoff_Guide.md` for why: GCP/IAM ownership doesn't transfer the same way as Drive files, so keeping everything under one JROC-controlled account from the start avoids a harder untangling job at handoff)
2. Create a new spreadsheet named **JROC Water Safety Data**
3. Rename the default tab to `USGS Live Data`
4. Add these additional tabs (exact names -- the scripts write to these by name):
   - `Weather Live` (fed by `nws_weather.py`, new)
   - `Upstream Live` (fed by `usgs_upstream.py`, new)
   - `Historical Incidents` (for the eventual RFD data / synthetic data today)
5. Copy your spreadsheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/`**`THIS_IS_YOUR_SHEET_ID`**`/edit`
   Save this — you'll need it later. One spreadsheet ID covers all tabs/scripts.

**Upload historical incidents (one-time):**
- In the `Historical Incidents` tab: File → Import → Upload → select `james_river_incidents_full.csv`
  (or the synthetic dataset if you want to prototype dashboard views before real RFD data arrives)
- Import settings: replace current sheet, detect automatically

---

## Step 2: Create a Google Cloud Service Account

The service account is what allows GitHub Actions to write to the sheet without a password.

1. Go to [console.cloud.google.com](https://console.cloud.google.com) — **make sure you're signed in as jrocwatersafety@gmail.com** (check the account switcher, top right — if you're signed into multiple Google accounts in this browser, it's easy to accidentally create the project under the wrong one)
2. Create a new project named **jroc-water-safety**
3. In the left menu: **APIs & Services → Library**
   - Search for and enable **Google Sheets API**
   - Search for and enable **Google Drive API**
4. In the left menu: **APIs & Services → Credentials**
   - Click **Create Credentials → Service Account**
   - Name: `github-actions-writer`
   - Role: **Editor**
   - Click Done
5. Click on the service account you just created
6. Go to the **Keys** tab → **Add Key → Create new key → JSON**
7. A JSON file downloads to your computer — **keep this file secure, treat it like a password**

This ONE service account and ONE JSON key covers all three scripts (river, weather,
upstream) -- you don't need a separate service account per script.

---

## Step 3: Share the Sheet with the Service Account

1. Open the JSON key file in a text editor
2. Find the `client_email` field — it looks like:
   `github-actions-writer@jroc-water-safety.iam.gserviceaccount.com`
3. Open your **JROC Water Safety Data** spreadsheet
4. Click **Share** (top right)
5. Paste the service account email, set role to **Editor**, uncheck "Notify people", click Share

---

## Step 4: Add GitHub Actions Secrets

1. Go to your GitHub repo: `github.com/pounchms/jroc-water-safety`
2. **Settings → Secrets and variables → Actions → New repository secret**

Add two secrets (shared by all three scripts -- river, weather, upstream all read
the same two env vars):

| Name | Value |
|---|---|
| `GOOGLE_CREDENTIALS` | The entire contents of the JSON key file (copy-paste everything) |
| `GOOGLE_SHEET_ID` | Your spreadsheet ID from Step 1 |

---

## Step 5: Test the Pipeline

**Locally:**
```bash
pip install requests pandas gspread google-auth
export GOOGLE_CREDENTIALS='<paste JSON key contents>'
export GOOGLE_SHEET_ID='<your sheet ID>'
python usgs_james_river.py
python data/pipeline/usgs_upstream.py
python data/pipeline/nws_weather.py
```
Open the spreadsheet — `USGS Live Data`, `Upstream Live`, and `Weather Live` tabs
should each populate. NOTE (2026-07-13): `nws_weather.py` has never been
successfully test-run against the live NWS API -- it was written and unit-tested
with fake data, but the sandbox it was built in couldn't reach api.weather.gov.
Run this one first and check its output carefully before trusting it.

**Via GitHub Actions:**
- `usgs_james_river.py` already has a workflow (`update_usgs.yml`) — go to
  **Actions → Refresh USGS James River Data → Run workflow**
- `usgs_upstream.py` and `nws_weather.py` do NOT have workflow files yet --
  they'll only run if you invoke them manually (above) until someone adds
  `.github/workflows/` entries for them, mirroring `update_usgs.yml` but
  pointing at the new scripts. That's a separate remaining step.

---

## Step 6: Connect Looker Studio

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com) — sign in as **jrocwatersafety@gmail.com**
2. **Create → Report**
3. **Add data → Google Sheets**
4. Select **JROC Water Safety Data** → `USGS Live Data` tab
5. Click **Add** — Looker Studio will detect your columns automatically
6. Repeat to add `Weather Live`, `Upstream Live`, and `Historical Incidents` as
   additional data sources

Looker Studio refreshes from Google Sheets every time the report is viewed (or on a schedule you set). The sheet updates daily from GitHub Actions (river script only, until the other two get workflow files). The loop is closed for the river data; weather and upstream still need a manual run or a workflow file to stay current.

---

## Step 7: Set Up the Near-Miss Form

See `near_miss_form_schema.md` for the full field list.

1. Go to [forms.google.com](https://forms.google.com) — sign in as **jrocwatersafety@gmail.com**
2. Create a new form named **James River Near-Miss Report**
3. Add the fields per the schema
4. In form settings: **Responses → Link to Sheets** → create new sheet named **JROC Near-Miss Reports**
5. Add that sheet as a fourth data source in your Looker Studio report
6. Copy the form's share URL and run:
   ```bash
   pip install qrcode[pil]
   python generate_qr.py "https://forms.gle/YOUR_URL" jroc_near_miss_qr.png
   ```

Print the QR code at 3×3 inches minimum for outdoor use.

---

## Summary: What Runs Automatically After Setup

| What | How often | Who triggers it |
|---|---|---|
| USGS river data refresh | Daily 7am ET | GitHub Actions (automatic) |
| USGS upstream / NWS weather refresh | Manual only (for now) | You, until workflow files are added |
| Google Sheet update | Same time as each script runs | Same script run |
| Looker Studio refresh | On page view | Nobody — happens automatically |
| Near-miss form submissions | Continuous | Park users scanning QR codes |

**What still needs a human:** creating the Google Cloud service account (Step 2),
sharing the sheet (Step 3), and adding the GitHub secrets (Step 4) all require
signing into your own Google/GitHub accounts -- these can't be done on your
behalf. Everything else (the scripts themselves, this doc, the workflow files)
is ready and waiting on those three steps.

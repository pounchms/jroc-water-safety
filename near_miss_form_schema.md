# Near-Miss Report Form Schema

Google Form: **James River Near-Miss Report**
Response sheet: **JROC Near-Miss Reports**

**Validate the location list with JROC on May 6** — they know the access points better than we do.

---

## Fields

### 1. Timestamp
- Type: Auto-captured by Google Forms
- Column name in sheet: `Timestamp`

---

### 2. Location *
- Type: Multiple choice (dropdown)
- Label: "Where did this happen?"
- Options (validate with JROC):
  - Pipeline Falls
  - Pony Pasture
  - 42nd Street
  - Hollywood Rapids
  - Belle Isle
  - Reedy Creek
  - Texas Beach
  - Other (please describe below)
- Column name: `location`

---

### 3. What happened *
- Type: Multiple choice
- Label: "What happened?"
- Options:
  - Near-drowning / swimmer in distress
  - Capsized or swamped watercraft
  - Got stuck on rocks
  - Observed someone else in danger
  - RFD rescue or response
  - Other (please describe below)
- Column name: `incident_type`

---

### 4. Water conditions *
- Type: Multiple choice
- Label: "How would you describe the river conditions at the time?"
- Options:
  - Low / calm
  - Normal
  - High / fast
  - Flooding
  - I don't know
- Column name: `water_conditions`

---

### 5. Activity type *
- Type: Multiple choice
- Label: "What were you or the people involved doing?"
- Options:
  - Swimming
  - Kayaking or canoeing
  - Stand-up paddleboarding
  - Tubing
  - Wading or crossing
  - Fishing
  - Other
- Column name: `activity_type`

---

### 6. Number of people involved *
- Type: Short answer (numeric)
- Label: "Approximately how many people were directly involved?"
- Validation: number, greater than 0
- Column name: `people_involved`

---

### 7. Outcome *
- Type: Multiple choice
- Label: "What was the outcome?"
- Options:
  - Everyone safe, no injuries
  - Minor injury (no hospital)
  - Serious injury or hospitalization
  - RFD or rescue was called
  - Unknown
- Column name: `outcome`

---

### 8. Date and time (if different from now)
- Type: Short answer
- Label: "If this happened at a different time, when? (e.g., yesterday around 3pm)"
- Required: No
- Column name: `incident_time_note`

---

### 9. Additional notes
- Type: Paragraph
- Label: "Anything else you want to add? (conditions, contributing factors, what would have helped)"
- Required: No
- Column name: `notes`

---

### 10. Contact email
- Type: Short answer (email validation)
- Label: "Optional: your email if you're willing to be contacted for follow-up"
- Required: No
- Column name: `contact_email`

---

## Form Settings

- **Confirmation message:** "Thank you. Your report helps JROC understand river conditions and improve safety at James River Park. Stay safe out there."
- **Allow response editing:** Yes (so people can correct mistakes)
- **Collect email addresses:** Off (contact is optional, field above handles it)
- **Restrict to VCU:** Off (public form — anyone scanning the QR should be able to submit)
- **Progress bar:** Off (short form, not needed)

---

## Looker Studio Mapping

When connecting the response sheet to Looker Studio, rename columns for readability:

| Sheet column | Looker Studio display name |
|---|---|
| `Timestamp` | Submission Date/Time |
| `location` | Location |
| `incident_type` | Incident Type |
| `water_conditions` | Water Conditions |
| `activity_type` | Activity |
| `people_involved` | People Involved |
| `outcome` | Outcome |
| `notes` | Notes |

Exclude `contact_email` from Looker Studio entirely — treat it as private.

---

## QR Code Deployment Notes

- Print at minimum 3×3 inches for reliable outdoor scanning
- Laminate or use weatherproof signage — these will be outside
- Place at trailhead kiosks and access point entry points, not riverside (phones get wet)
- Suggested text for the sign: **"Had a close call on the James? Report it here."**
- Whoever at JROC owns sign placement also owns form field updates — establish this on May 6

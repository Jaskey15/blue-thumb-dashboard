# Unknown Sites Investigation — FeatureServer Sync (#10)

**Date:** 2026-03-20
**Status:** Investigation complete — structural gap identified, resolution pending
**Issue:** [#10 — FeatureServer sync drops rows when SiteName not in DB](https://github.com/Jaskey15/blue-thumb-dashboard/issues/10)
**Branch:** `fix/issue-10-unknown-site-resolution`

---

## Summary

After PR #12 added deterministic site resolution (exact → normalized → explicit alias),
4 sites remain unresolvable. This document records the full investigation into whether
these are aliases, pipeline bugs, or genuinely new monitoring locations — and identifies
the structural gap that will cause this to recur.

**Conclusion: these are new monitoring locations that started in May–July 2025, just
before or after the CSV pipeline snapshot was taken. They are not aliases of existing
sites, and no PR introduced a bug causing them to be dropped — they were always being
dropped silently since PR #4. The underlying issue is a structural gap: the FeatureServer
sync updates chemical data for known sites but has no path to discover or register new
sites.**

---

## The 4 Remaining Unknown Sites

| FeatureServer `SiteName` | Total FS records | Earliest FS record | Records in DB |
|---|---|---|---|
| `Bull Creek: Excelsior` | 7 | 2025-05-26 | 0 |
| `Fisher Creek: Hwy 51` | 10 | 2025-05-23 | 0 |
| `Tony Hollow Creek: E1350Rd` | 2 | 2025-07-16 | 0 |
| `Turkey Creek: E630Rd` | 8 | 2025-07-02 | 0 |

All 27 total records span May 2025 – Mar 2026 and are QAQC-complete. None have been
inserted into `chemical_collection_events`.

The 15 objectids reported in issue #10 are a subset. Full set from live FeatureServer
(verified 2026-03-20):

```
Bull Creek: Excelsior      — objectids: 3916, 3917, 4000, 4014, 4063, 4169, 4170
Fisher Creek: Hwy 51       — objectids: 3857, 3898, 3954, 4009, 4065, 4119, 4163, 4222, 4275, 4327
Tony Hollow Creek: E1350Rd — objectids: 3935, 4006
Turkey Creek: E630Rd       — objectids: 3929, 4005, 4077, 4175, 4176, 4231, 4333, 4334
```

---

## Investigation: Are These Aliases of Existing Sites?

Searched the `sites` table exhaustively using the following terms:
`Bull Creek`, `Fisher Creek`, `Tony Hollow`, `Turkey Creek`, `Excelsior`,
`Hwy 51`, `E1350`, `E630`, and all county variants.

**Results:**
- `Bull Creek` — no match in any form
- `Fisher Creek` — no match (`Fish Creek: NS393` and `Cow Creek: Hwy 51` exist but
  are unrelated waterbodies in different counties)
- `Tony Hollow Creek` — no match (other `*Hollow*` sites exist but none are this creek)
- `Turkey Creek: E630Rd` — two Turkey Creek sites exist (`Turkey Creek` id=302 and
  `Turkey Creek: Bike Path` id=303, both Washington county) but `E630Rd` is a distinct
  access point not covered by either

**Alias mapping cannot fix this.** `SITE_ALIASES` maps a FeatureServer name to an
existing canonical DB name. All 4 sites have no canonical counterpart to map to.

---

## Investigation: Pipeline Bug or New Sites?

### CSV pipeline date range

The `cleaned_updated_chemical_data.csv` (last seen in git at commit `2465410^`) ran from
2015-06-24 to **2025-05-28** (2,739 rows). None of the 4 unknown sites appear in it at
all — confirmed by full-text search of the git-historical CSV.

### FeatureServer first-submission dates vs CSV cutoff

| Site | First FS record | Days relative to CSV end (2025-05-28) |
|---|---|---|
| `Fisher Creek: Hwy 51` | 2025-05-23 | **5 days before** |
| `Bull Creek: Excelsior` | 2025-05-26 | **2 days before** |
| `Tony Hollow Creek: E1350Rd` | 2025-07-16 | 7 weeks after |
| `Turkey Creek: E630Rd` | 2025-07-02 | 5 weeks after |

Fisher Creek and Bull Creek first submitted data 2–5 days before the CSV cutoff but are
absent from the CSV. Two plausible explanations:

1. **QAQC cleared after export** — the CSV was exported before those submissions passed
   QAQC review, so they didn't appear in the export even though the sampling dates overlap.
2. **Site list was frozen before chemical data** — if `consolidated_sites.csv` was built
   from a master site list that predated these volunteers registering their sites, neither
   the site entry nor its chemical data would appear regardless of QAQC timing.

Either way the outcome is the same: these sites were never in `consolidated_sites.csv`,
never in the `sites` table, and the FeatureServer has been submitting data for them for
10 months with no path for it to enter the DB.

### Was this introduced by PR #12?

No. PR #4's `insert_processed_data_to_db` used exact-match only:

```python
if site_name not in site_lookup:
    logger.warning(f"Site {site_name} not found in database - skipping")
    continue
```

These sites were being silently dropped from the very first FeatureServer sync. PR #12
added normalized matching and alias resolution (reducing the unknown list from 10 sites
to 4), made skips actionable (counts + sample_ids surfaced in sync metadata), and added
watermark/backfill safety. PR #12 improved the situation; it did not cause it.

---

## Root Cause: Structural Gap

The site registration pipeline has a fundamental asymmetry:

- **Chemical data** — continuously synced from FeatureServer via Cloud Function
- **Site registry** — only updated by running the full local ETL pipeline
  (`reset_database`) and uploading the rebuilt DB to GCS

When a new Blue Thumb volunteer starts monitoring a new location, their chemical data
flows through the sync immediately. But the site doesn't exist in the `sites` table,
so every record is dropped — permanently, until someone manually rebuilds and redeploys
the DB.

This gap will produce a new batch of unknown sites every time volunteers are onboarded
to new locations without a corresponding DB rebuild. It is not self-healing.

---

## Resolution Plan

### Immediate (unblock the 27 known missing records)

1. **Confirm with Blue Thumb coordinator** that these 4 are intentional new monitoring
   locations (not data-entry errors on the FeatureServer)
2. **Obtain coordinates** — the FeatureServer returns geometry with each record; this PR
   scaffolds `fetch_geometry_for_objectids()` in `arcgis_sync.py` to retrieve lat/lon
   for specific objectids without running a full sync
3. **Add sites to the pipeline** — add rows to `data/interim/consolidated_sites.csv`
   with verified coordinates, county, and river basin; run `python -m database.reset_database`;
   upload rebuilt DB to GCS
4. **Trigger backfill** — `needs_backfill: true` and `backfill_since_date` are already
   set in sync metadata; once sites exist in the DB the next scheduled run (or a manual
   invocation with `since_date`) will recover all skipped records automatically

### Structural (prevent recurrence — decision pending)

**Option A — Operator review workflow:** Surface persistent unknown sites more prominently
(e.g., alert after N consecutive syncs with the same unknowns). Operator adds sites
manually. Low complexity, requires human in the loop, acceptable latency if cadence is
monthly.

**Option B — Auto-register from FeatureServer geometry:** When site resolution fails
and the record carries valid geometry, tentatively register the site using FeatureServer
coordinates flagged as `pending_coordinator_review`. Removes human gating, higher
complexity, risk of polluting the map with unverified locations.

**Option C — Coordinator-managed alias/site file in GCS:** Maintain a `new_sites.csv`
or `site_aliases.csv` in the GCS bucket that the Cloud Function reads at sync time.
Coordinator adds new sites there without requiring a DB rebuild or code deploy. Moderate
complexity, preserves human oversight, eliminates the rebuild-and-redeploy cycle.

The right option depends on how frequently new sites are onboarded and whether
coordinator sign-off before data appears on the dashboard is a requirement.

---

## Why Not Auto-Insert New Sites Without Review

Auto-inserting unverified FeatureServer site names into the `sites` table would:
- Create incomplete site records (unknown county, river basin, ecoregion classification)
- Bypass coordinator verification that ensures data quality and naming consistency
- Risk polluting the map and downstream queries with unvetted monitoring locations

The current pattern (skip + report + backfill) is the right guard rail. The gap is
that the problem accumulates silently across sync runs until someone investigates.

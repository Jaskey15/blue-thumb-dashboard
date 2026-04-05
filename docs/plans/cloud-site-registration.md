# Cloud Site Registration — Planning Doc

## Problem

The site registry is a one-way, local-only pipeline. New monitoring locations that appear on the FeatureServer get their chemical data silently dropped because the `sites` table only gets updated via manual `reset_database` runs. This gap grows over time as new volunteers register locations.

PR #15 (ImmortalDemonGod) investigated and confirmed that the 4 currently unknown sites — Fisher Creek: Hwy 51, Bull Creek: Excelsior, Tony Hollow Creek: E1350Rd, Turkey Creek: E630Rd — are genuinely new locations (started May–July 2025), not aliases or duplicates.

## Goal

Eliminate the need for `reset_database` as a regular operation by handling new site discovery in the cloud sync. A human-in-the-loop review step is required before new sites go live — we don't want every FeatureServer submission auto-creating sites.

## Approach

### Prerequisites (one-time, before merge)

1. Run `reset_database` locally with the latest CSV data to get a clean, complete site list
2. Upload the fresh DB to GCS
3. This eliminates the current backlog so no backfill logic is needed

### Cloud Sync Changes

When the daily sync encounters a site name not in the `sites` table:

#### 1. Normalize the incoming site name

Reuse the same logic as the local pipeline (`_normalize_site_name` in `arcgis_sync.py`, equivalent to `clean_site_name` in `data_loader.py`):
- Strip leading/trailing whitespace
- Collapse multiple whitespace to single space

This already happens in `translate_to_pipeline_schema()` — no change needed here.

#### 2. Check for coordinate-based duplicates

Before staging a site as "pending", check if it's within 50m of an existing site using the same Haversine distance check from `merge_sites.py`. This catches cases where volunteers register the same physical location under a slightly different name.

This requires requesting geometry from the FeatureServer (`returnGeometry=true` in the query params), which is not currently done. The geometry comes back as `{x: lon, y: lat}` on each feature.

If a coordinate match is found, log it and map the data to the existing site instead of creating a pending entry.

#### 3. Stage genuinely new sites in `pending_sites`

New DB table:

```sql
CREATE TABLE IF NOT EXISTS pending_sites (
    pending_site_id INTEGER PRIMARY KEY,
    site_name TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    first_seen_date TEXT NOT NULL,
    source TEXT DEFAULT 'feature_server',
    status TEXT DEFAULT 'pending',  -- pending | approved | rejected
    reviewed_date TEXT,
    notes TEXT,
    UNIQUE(site_name)
)
```

When a site is genuinely new (no name match, no coordinate match):
- Insert into `pending_sites` with status='pending'
- Log the pending site in the sync response metadata
- Chemical data for this site continues to be skipped (same as today)

#### 4. Approval flow (future, keep simple for now)

For the initial implementation, the review process is:
- Sync metadata/logs surface which sites are pending
- Coordinator manually approves by updating `pending_sites.status` to 'approved'
- Next sync run checks for approved pending sites, moves them to `sites`, and data flows naturally on the following sync (idempotent insertion picks up the previously-skipped records via editdate lookback)

A dashboard admin tab for this can come later. The important thing is that the data pipeline doesn't lose track of these sites.

## Files to modify

| File | Change |
|------|--------|
| `database/db_schema.py` | Add `pending_sites` table to schema |
| `data_processing/arcgis_sync.py` | Add `returnGeometry=true` to fetch params; expose lat/lon on records |
| `cloud_functions/survey123_sync/chemical_processor.py` | On unknown site: normalize name, Haversine check against existing sites, insert into `pending_sites` if genuinely new |
| `cloud_functions/survey123_sync/main.py` | Surface pending site info in sync response; on each run, promote any `approved` pending sites to `sites` table |
| `cloud_functions/survey123_sync/deploy.sh` | No change needed (already stages `database/` and `data_processing/`) |

## What this replaces from PR #15

- The `unknown_site_coordinates` capture in `chemical_processor.py` is replaced by the `pending_sites` table
- The `fetch_geometry_for_objectids()` utility is unnecessary — we get geometry on every fetch instead
- The investigation doc and root cause analysis from PR #15 remain valuable context

## Design decisions

**Why `pending_sites` table vs. GCS blob?**
- Lives with the data, gets backed up automatically with the DB
- Queryable — dashboard can surface it later
- Simpler than managing a separate JSON file

**Why not auto-approve?**
- Blue Thumb is a citizen science program — site names and locations need coordinator validation
- Prevents test submissions or errors from polluting the site registry

**Why Haversine check in the cloud sync?**
- Volunteers sometimes register the same physical site under different names
- Without this check, the pending queue would fill with false "new" sites
- 50m threshold is already validated in the local pipeline (`merge_sites.py`)

**What happens to chemical data while a site is pending?**
- Dropped, same as today. No staged measurements.
- Once approved and promoted to `sites`, the next sync's editdate lookback window (or 7-day default) will pick up recent data. Older data can be recovered by a one-time wider lookback if needed.

## Open questions

1. **FeatureServer geometry fields** — Need to verify the exact response structure when `returnGeometry=true`. Expected: `feature.geometry.x` (longitude) and `feature.geometry.y` (latitude).
2. **County / river basin / ecoregion** — The FeatureServer may not include these fields. If not, pending sites would have coordinates only, and metadata would need to be filled in during review. Check the full 171-field schema.
3. **Approval mechanism** — For v1, direct DB update is fine. Should we add a simple script or Cloud Function endpoint for approvals?
4. **Notification** — How should the coordinator know there are pending sites? Sync logs only? Email? Dashboard indicator?

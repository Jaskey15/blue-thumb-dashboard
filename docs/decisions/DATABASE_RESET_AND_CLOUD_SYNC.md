# Database Reset and Cloud Sync — Decision Context

**Date**: 2026-02-27

## Background

The production database lives in Google Cloud Storage (`gs://blue-thumb-database/blue_thumb.db`). Two processes interact with it:

- **Cloud Run (dashboard)**: Downloads the DB on startup and polls GCS every 5 minutes for updates.
- **Cloud Function (daily sync)**: Downloads the DB, inserts new chemical data from the ArcGIS FeatureServer, and uploads the updated DB back to GCS.

The Cloud Function **only syncs chemical data**. It does not run the site pipeline — no CSV consolidation, no Haversine coordinate deduplication, no site merging. It assumes the `sites` table is already correct and skips any FeatureServer record whose site name doesn't match an existing site.

The full pipeline (`python -m database.reset_database`) runs locally and rebuilds everything: site consolidation, Haversine dedup, all monitoring data types (chemical, fish, macro, habitat), and active/historic classification.

## The Gap

The Haversine-only site deduplication (commit `8a74bff`, Feb 2026) has never been applied to the production database. The production `sites` table was built from an earlier local reset — before the Haversine-only change — and the Cloud Function has been incrementally adding chemical records on top of that original site table ever since.

This means the production DB may contain duplicate site entries that would be merged under the current dedup logic.

## How to Apply a Database Reset to Production

A local `reset_database` rebuilds the DB from CSV source files only. It will not contain any FeatureServer-synced chemical records that arrived after the last CSV update. To propagate a reset to production:

1. **Run the reset locally**: `python -m database.reset_database`
2. **Upload the reset DB to GCS**: Replace `blue_thumb.db` in the `blue-thumb-database` bucket.
3. **Delete the sync metadata blob**: Remove `sync_metadata/last_feature_server_sync.json` from the bucket. This forces the Cloud Function back to its first-run `day` strategy.
4. **Wait for the next Cloud Function run** (or trigger manually). It will query `MAX(collection_date)` from the reset DB — which will be the date of the latest CSV record — and backfill all FeatureServer data since then.

Cloud Run will pick up the new DB within 5 minutes via its background refresh thread.

## Sync Strategy Detail

The Cloud Function selects between two strategies based on whether the metadata blob exists:

| Condition | Strategy | Queries by | Effect |
|-----------|----------|------------|--------|
| Metadata blob **missing** | `day` | `MAX(collection_date)` in DB | Full backfill from DB's latest date |
| Metadata blob **exists** | `editdate` | `last_sync_timestamp` from blob | Incremental — only records edited since last sync |

Deleting the metadata blob after a reset is what triggers the full backfill.

## Risks and Verification

- **Data loss window**: FeatureServer records synced after the last CSV update but before the reset are temporarily absent. The backfill on the next Cloud Function run restores them, but there is a brief gap between upload and sync completion.
- **Site name matching**: After Haversine dedup merges sites, the canonical site names in the DB must still match what the FeatureServer returns. If a merged site's preferred name differs from the FeatureServer's `SiteName` field, those records will be skipped as unknown. Verify with a dry run before uploading.
- **Non-chemical data**: Fish, macro, and habitat data come only from CSVs processed during `reset_database`. The Cloud Function does not sync these — they are only refreshed by a local reset.

## Status

**Not yet executed.** This document records the process for when a production reset is needed to incorporate the Haversine deduplication or other site pipeline changes.

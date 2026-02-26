# Remove Rounding-Based Dedup, Make Haversine-Only

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the legacy `boundary_safe=False` rounding dedup path from `merge_sites.py` and make Haversine distance clustering the sole implementation.

**Architecture:** The rounding path (`ROUND(lat, 3)` binning) was the original dedup approach. The Haversine path (floor-bin + neighbor-bin + distance clustering) was added to fix the rounding boundary problem and is already the default. We're deleting the rounding branch entirely — the Haversine path already has its own efficient spatial indexing via floor bins, so rounding adds nothing.

**Tech Stack:** Python, pandas, SQLite, unittest

---

### Task 1: Simplify `find_duplicate_coordinate_groups` in merge_sites.py

**Files:**
- Modify: `data_processing/merge_sites.py:37-183`

**Step 1: Remove `boundary_safe` and `scale` parameters, simplify SQL query**

Remove `boundary_safe` and `scale` from the function signature (keep `distance_threshold_m`). Hardcode `scale = 1000` inside the function. Remove `rounded_lat`/`rounded_lon` from the SQL SELECT and ORDER BY. Remove the `if not boundary_safe:` early-return branch (lines 102-104).

```python
def find_duplicate_coordinate_groups(conn=None, distance_threshold_m=50.0):
    """Find candidate duplicate sites by Haversine distance clustering.

    Uses a two-stage approach:
      1) Candidate generation by binning coordinates into fixed floor bins:
         lat_bin = floor(latitude * 1000), lon_bin = floor(longitude * 1000).
         Bins correspond to ~0.001 degrees.
      2) For each site, compares against sites in the same bin and the 8 neighboring
         bins and computes Haversine distance. Pairs within distance_threshold_m are
         unioned into clusters via union-find (transitive).

    Args:
        conn: Optional SQLite connection. If omitted, opens/closes its own.
        distance_threshold_m: Distance threshold in meters for clustering (default 50.0).

    Returns:
        A pandas DataFrame of candidate duplicate sites with a group_id column
        identifying each cluster. Empty DataFrame when no duplicates detected.
    """
    if conn is None:
        conn = get_connection()
        should_close = True
    else:
        should_close = False

    try:
        query = """
        SELECT
            site_id,
            site_name,
            latitude,
            longitude,
            county,
            river_basin,
            ecoregion
        FROM sites
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY site_name
        """

        df = pd.read_sql_query(query, conn)
        df = df.reset_index(drop=True)

        scale = 1000
        bin_to_indices = {}
        lat_bins = [0] * len(df)
        lon_bins = [0] * len(df)
        for i, row in df.iterrows():
            lat_bin = math.floor(row['latitude'] * scale)
            lon_bin = math.floor(row['longitude'] * scale)
            lat_bins[i] = lat_bin
            lon_bins[i] = lon_bin
            bin_to_indices.setdefault((lat_bin, lon_bin), []).append(i)

        parent = list(range(len(df)))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra = find(a)
            rb = find(b)
            if ra != rb:
                parent[rb] = ra

        def haversine_m(lat1, lon1, lat2, lon2):
            R = 6371000.0
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)
            a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
            return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        for i, row in df.iterrows():
            base = (lat_bins[i], lon_bins[i])
            for dlat in (-1, 0, 1):
                for dlon in (-1, 0, 1):
                    nbr = (base[0] + dlat, base[1] + dlon)
                    for j in bin_to_indices.get(nbr, []):
                        if j <= i:
                            continue
                        dist = haversine_m(
                            row['latitude'],
                            row['longitude'],
                            df.at[j, 'latitude'],
                            df.at[j, 'longitude'],
                        )
                        if dist <= distance_threshold_m:
                            union(i, j)

        root_to_members = {}
        for i in range(len(df)):
            root = find(i)
            root_to_members.setdefault(root, []).append(i)

        dupe_groups = [members for members in root_to_members.values() if len(members) > 1]
        if not dupe_groups:
            empty = df.iloc[0:0].copy()
            empty['group_id'] = pd.Series(dtype='int64')
            return empty

        idx_to_group_id = {}
        out_indices = []
        group_id = 0
        for members in dupe_groups:
            for idx in members:
                idx_to_group_id[idx] = group_id
                out_indices.append(idx)
            group_id += 1

        result = df.loc[out_indices].copy()
        result['group_id'] = [idx_to_group_id[i] for i in result.index]
        result = result.sort_values(['group_id', 'site_name'])
        return result
    finally:
        if should_close:
            close_connection(conn)
```

**Step 2: Run existing tests to see what breaks**

Run: `pytest tests/data_processing/test_site_management.py -v 2>&1 | head -60`
Expected: Several failures due to removed parameters and changed SQL columns.

**Step 3: Commit**

```bash
git add data_processing/merge_sites.py
git commit -m "refactor: remove rounding path from find_duplicate_coordinate_groups

Make Haversine distance clustering the sole dedup implementation.
Remove boundary_safe and scale parameters. Simplify SQL query."
```

---

### Task 2: Simplify `analyze_coordinate_duplicates` in merge_sites.py

**Files:**
- Modify: `data_processing/merge_sites.py:185-277`

**Step 1: Remove `boundary_safe` and `scale` parameters, remove branching logic**

Remove the parameters from the signature and the `groupby_cols` branching. Always group by `group_id`, always use `(group_id=N)` coordinate labels.

```python
def analyze_coordinate_duplicates(distance_threshold_m=50.0):
    """Analyze duplicate groups without mutating the database.

    Read-only preview that detects duplicate groups and predicts which site
    would be kept by the merge process.

    Args:
        distance_threshold_m: Distance threshold in meters for clustering.

    Returns:
        A dictionary with summary statistics and per-group site lists.
        Returns None on unexpected errors.
    """
    logger.info("Analyzing coordinate duplicates...")

    try:
        site_data_df, updated_chemical_df, chemical_data_df = load_csv_files()

        updated_chemical_sites = set(updated_chemical_df['Site Name'].apply(clean_site_name))
        chemical_data_sites = set(chemical_data_df['SiteName'].apply(clean_site_name))

        conn = get_connection()
        duplicate_groups_df = find_duplicate_coordinate_groups(
            conn,
            distance_threshold_m=distance_threshold_m,
        )
        close_connection(conn)

        if duplicate_groups_df.empty:
            logger.info("No coordinate duplicate sites found")
            return {
                'total_duplicate_sites': 0,
                'duplicate_groups': 0,
                'examples': []
            }

        duplicate_groups_summary = []
        total_duplicate_sites = len(duplicate_groups_df)
        group_count = 0

        for group_key, group in duplicate_groups_df.groupby('group_id'):
            group_count += 1
            sites_in_group = list(group['site_name'])

            preferred_site, _, reason = determine_preferred_site(
                group, updated_chemical_sites, chemical_data_sites
            )

            coordinates = f"(group_id={group_key})"

            group_info = {
                'coordinates': coordinates,
                'site_count': len(group),
                'sites': sites_in_group,
                'would_keep': preferred_site['site_name'],
                'reason': reason
            }

            duplicate_groups_summary.append(group_info)

        logger.info(f"Found {total_duplicate_sites} duplicate sites in {group_count} coordinate groups")
        if total_duplicate_sites > group_count:
            logger.info(f"Would delete {total_duplicate_sites - group_count} duplicate sites")

        return {
            'total_duplicate_sites': total_duplicate_sites,
            'duplicate_groups': group_count,
            'examples': duplicate_groups_summary[:5],
            'all_groups': duplicate_groups_summary
        }

    except Exception as e:
        logger.error(f"Error analyzing coordinate duplicates: {e}")
        return None
```

**Step 2: Commit**

```bash
git add data_processing/merge_sites.py
git commit -m "refactor: remove rounding path from analyze_coordinate_duplicates"
```

---

### Task 3: Simplify `merge_duplicate_sites` in merge_sites.py

**Files:**
- Modify: `data_processing/merge_sites.py:485-600`

**Step 1: Remove `boundary_safe` and `scale` parameters, remove branching logic**

```python
def merge_duplicate_sites(distance_threshold_m=50.0):
    """Merge duplicate sites by transferring monitoring data and deleting extras.

    Mutates the SQLite database by:
    - Grouping nearby sites via Haversine distance clustering.
    - Selecting a preferred site per group via determine_preferred_site().
    - Reassigning all monitoring data from duplicates to the preferred site.
    - Deleting the now-empty duplicate site rows.
    - Updating cleaned interim CSVs to replace deleted site names.

    Args:
        distance_threshold_m: Distance threshold in meters for clustering (default 50.0).

    Returns:
        A dictionary with counts of processed groups, deleted sites, and
        transferred records.
    """
    logger.info("Starting coordinate-based site merge process...")

    try:
        site_data_df, updated_chemical_df, chemical_data_df = load_csv_files()

        updated_chemical_sites = set(updated_chemical_df['Site Name'].apply(clean_site_name))
        chemical_data_sites = set(chemical_data_df['SiteName'].apply(clean_site_name))

        conn = get_connection()
        cursor = conn.cursor()

        duplicate_groups_df = find_duplicate_coordinate_groups(
            conn,
            distance_threshold_m=distance_threshold_m,
        )

        groups_processed = 0
        sites_deleted = 0
        total_records_transferred = 0
        site_mapping = {}

        try:
            if not duplicate_groups_df.empty:
                logger.info(f"Found {len(duplicate_groups_df.groupby('group_id'))} coordinate groups with duplicates")

                for _, group in duplicate_groups_df.groupby('group_id'):
                    preferred_site, sites_to_merge, reason = determine_preferred_site(
                        group, updated_chemical_sites, chemical_data_sites
                    )

                    if not preferred_site is None and sites_to_merge:
                        preferred_site_id = int(preferred_site['site_id'])

                        cursor.execute("SELECT site_name FROM sites WHERE site_id = ?", (preferred_site_id,))
                        preferred_site_check = cursor.fetchone()

                        if not preferred_site_check:
                            logger.error(f"CRITICAL: Preferred site_id {preferred_site_id} ('{preferred_site['site_name']}') not found in database!")
                            raise Exception(f"Preferred site_id {preferred_site_id} not found in database")

                        for site_to_merge in sites_to_merge:
                            from_site_id = int(site_to_merge['site_id'])

                            transfer_counts = transfer_site_data(cursor, from_site_id, preferred_site_id)
                            total_records_transferred += sum(transfer_counts.values())

                            cursor.execute("DELETE FROM sites WHERE site_id = ?", (from_site_id,))
                            sites_deleted += 1

                            old_site_name = site_to_merge['site_name']
                            new_site_name = preferred_site['site_name']
                            site_mapping[old_site_name] = new_site_name

                        update_site_metadata(cursor, preferred_site_id, site_data_df, preferred_site['site_name'])

                        groups_processed += 1

            conn.commit()
            logger.info(f"Site merge complete: {groups_processed} groups processed, {sites_deleted} sites deleted, {total_records_transferred} records transferred")

            if site_mapping:
                update_csv_files_with_mapping(site_mapping)

            return {
                'groups_processed': groups_processed,
                'sites_deleted': sites_deleted,
                'records_transferred': total_records_transferred
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"Error during site merge: {e}")
            raise

    except Exception as e:
        logger.error(f"Error in coordinate merge process: {e}")
        raise
    finally:
        if 'conn' in locals():
            close_connection(conn)
```

**Step 2: Commit**

```bash
git add data_processing/merge_sites.py
git commit -m "refactor: remove rounding path from merge_duplicate_sites"
```

---

### Task 4: Update tests in test_site_management.py

**Files:**
- Modify: `tests/data_processing/test_site_management.py`

**Step 1: Remove `rounded_lat`/`rounded_lon` from all test DataFrames**

In `setUp` — remove `rounded_lat` and `rounded_lon` keys from `self.sample_sites_with_duplicates` (lines 87-88).

In every test that creates a local DataFrame with these columns (`near_boundary_sites`, `far_sites`, `chain_sites`, `negative_lon_sites`, and the boundary-safe analysis/merge fixtures) — remove the `rounded_lat` and `rounded_lon` keys.

**Step 2: Remove rounding-mode comparison assertions**

In these three tests, delete the lines that call `find_duplicate_coordinate_groups(boundary_safe=False)` and assert the rounding mode misses duplicates:
- `test_find_duplicate_coordinate_groups_boundary_safe_detects_rounding_boundary` — remove lines 463-465
- `test_find_duplicate_coordinate_groups_boundary_safe_transitive_closure` — remove lines 518-520
- `test_find_duplicate_coordinate_groups_boundary_safe_negative_longitude_bins` — remove lines 547-549

**Step 3: Remove explicit `boundary_safe=True` args from test calls**

- `test_find_duplicate_coordinate_groups_boundary_safe_respects_threshold` line 493: change `find_duplicate_coordinate_groups(boundary_safe=True, distance_threshold_m=10.0)` to `find_duplicate_coordinate_groups(distance_threshold_m=10.0)`
- `test_analyze_coordinate_duplicates_boundary_safe_with_data` line 694: change `analyze_coordinate_duplicates(boundary_safe=True)` to `analyze_coordinate_duplicates()`
- `test_merge_duplicate_sites_boundary_safe_merges_one_group` line 753: change `merge_duplicate_sites(boundary_safe=True)` to `merge_duplicate_sites()`

**Step 4: Update `determine_preferred_site` tests to filter by `site_id` instead of `rounded_lat`**

`test_determine_preferred_site_updated_chemical_priority` (line 558-559):
```python
group = self.sample_sites_with_duplicates[
    self.sample_sites_with_duplicates['site_id'].isin([1, 2])
].copy()
```

`test_determine_preferred_site_chemical_data_priority` (line 573-574):
```python
group = self.sample_sites_with_duplicates[
    self.sample_sites_with_duplicates['site_id'].isin([3, 4])
].copy()
```

**Step 5: Rename test methods to remove "boundary_safe" qualifier**

| Old name | New name |
|----------|----------|
| `test_find_duplicate_coordinate_groups_boundary_safe_detects_rounding_boundary` | `test_find_duplicate_coordinate_groups_detects_nearby_sites` |
| `test_find_duplicate_coordinate_groups_boundary_safe_respects_threshold` | `test_find_duplicate_coordinate_groups_respects_threshold` |
| `test_find_duplicate_coordinate_groups_boundary_safe_transitive_closure` | `test_find_duplicate_coordinate_groups_transitive_closure` |
| `test_find_duplicate_coordinate_groups_boundary_safe_negative_longitude_bins` | `test_find_duplicate_coordinate_groups_negative_longitude_bins` |
| `test_analyze_coordinate_duplicates_boundary_safe_with_data` | `test_analyze_coordinate_duplicates_with_group_labels` |
| `test_merge_duplicate_sites_boundary_safe_merges_one_group` | `test_merge_duplicate_sites_merges_one_group` |

**Step 6: Run tests**

Run: `pytest tests/data_processing/test_site_management.py -v`
Expected: All PASS.

**Step 7: Commit**

```bash
git add tests/data_processing/test_site_management.py
git commit -m "test: update site management tests for haversine-only dedup"
```

---

### Task 5: Update tests in test_site_consolidation.py

**Files:**
- Modify: `tests/data_processing/test_site_consolidation.py`

**Step 1: Remove `rounded_lat`/`rounded_lon` from `sample_sites_with_duplicates` fixture** (lines 86-87)

**Step 2: Update `determine_preferred_site` tests to filter by `site_id`**

`test_determine_preferred_site_updated_chemical_priority` (line 434-435):
```python
group = self.sample_sites_with_duplicates[
    self.sample_sites_with_duplicates['site_id'].isin([1, 2])
].copy()
```

`test_determine_preferred_site_chemical_data_priority` (line 449-450):
```python
group = self.sample_sites_with_duplicates[
    self.sample_sites_with_duplicates['site_id'].isin([3, 4])
].copy()
```

**Step 3: Run tests**

Run: `pytest tests/data_processing/test_site_consolidation.py -v`
Expected: All PASS.

**Step 4: Commit**

```bash
git add tests/data_processing/test_site_consolidation.py
git commit -m "test: update site consolidation tests for haversine-only dedup"
```

---

### Task 6: Update documentation

**Files:**
- Modify: `docs/architecture/DATA_PIPELINE.md:42,78`
- Modify: `docs/decisions/SITE_PROCESSING_PIPELINE.md:98`

**Step 1: Update DATA_PIPELINE.md line 42**

Change:
```
| `merge_sites.py` | Find coordinate duplicates via boundary-safe Haversine clustering (50m default threshold, floor-bin + neighbor-bin expansion, union-find transitive grouping). Merge to preferred site, reassign all monitoring data. Legacy rounding mode available via `boundary_safe=False` |
```
To:
```
| `merge_sites.py` | Find coordinate duplicates via Haversine distance clustering (50m default threshold, floor-bin + neighbor-bin expansion, union-find transitive grouping). Merge to preferred site, reassign all monitoring data |
```

**Step 2: Update DATA_PIPELINE.md line 78**

Remove the legacy rounding paragraph entirely:
```
A legacy rounding mode (`boundary_safe=False`) groups by identical `ROUND(latitude, 3)` / `ROUND(longitude, 3)` bins but can miss near-duplicates on rounding boundaries. See `docs/RFC_PIPELINE_HARDENING_VALIDATION.md` for the design rationale.
```

**Step 3: Update SITE_PROCESSING_PIPELINE.md line 98**

Remove the legacy mode bullet:
```
- **Legacy mode**: `boundary_safe=False` falls back to strict `ROUND(latitude, 3)` / `ROUND(longitude, 3)` bin matching, which can miss near-duplicates on rounding boundaries.
```

**Step 4: Commit**

```bash
git add docs/architecture/DATA_PIPELINE.md docs/decisions/SITE_PROCESSING_PIPELINE.md
git commit -m "docs: remove references to legacy rounding dedup mode"
```

---

### Task 7: Full test suite verification

**Step 1: Run full test suite**

Run: `pytest -v`
Expected: All tests pass. No references to `boundary_safe` remain in non-RFC code.

**Step 2: Grep for any remaining references**

Run: `grep -r "boundary_safe" --include="*.py" .`
Expected: No matches.

**Step 3: Final commit if any fixups needed**

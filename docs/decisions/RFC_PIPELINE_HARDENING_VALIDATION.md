# RFC: Pipeline Hardening & Validation Integration (Discussion Draft)
**Project:** Blue Thumb Water Quality Dashboard  
**Author:** (Prepared for Jacob)  
**Status:** Discussion Draft  

---

## 1. Executive Summary
The Blue Thumb Dashboard already demonstrates a strong modular structure: clear separation between data processing, database access, visualization utilities, and tab-level UI composition. As the system scales toward dependable daily synchronization and stakeholder-facing reporting, this RFC proposes a small set of targeted improvements that preserve the existing architecture while reducing operational risk.

**Goals**
1. Improve site deduplication precision beyond coordinate rounding edge cases.
2. Ensure Cloud Function reliability by making shared-module imports robust under deployment isolation.
3. (Optional) Add a “trust anchor” visualization layer using validated-sites output from the BlueStream validation project (Miguel Ingram).

---

## 2. Infrastructure Optimizations

### 2.1 Optimization: Boundary-Safe Site Deduplication

#### Current Context (code-verified)
The site deduplication candidate selection in `data_processing/merge_sites.py` uses coordinate binning via SQL rounding:

- `ROUND(latitude, 3) as rounded_lat`
- `ROUND(longitude, 3) as rounded_lon`

This is a reasonable first-pass clustering approach, but it can miss near-duplicates that lie on opposite sides of the rounding boundary.

#### The Edge Case (“Boundary Problem”)
Two sites can be physically extremely close (sub-meter to a few meters) while rounding into different `ROUND(x, 3)` bins.

#### Proposed Enhancement (repo-compatible; no new heavy dependencies)
Implement a **two-stage dedupe**:

- **Stage A: Candidate generation (keep existing approach)**
  - Use the current rounding/binning logic to get likely duplicates quickly.

- **Stage B: Boundary-safe expansion + verification (new)**
  - For each bin, also compare sites against the **8 neighboring bins** (±1 bin in lat and lon).
  - Compute an actual **Haversine distance** between candidate pairs.
  - Flag pairs within a configurable threshold (e.g., **50m**).

**Value**
- Prevents missed duplicates caused purely by rounding boundaries.
- Keeps the existing architecture and performance profile (still local comparisons).
- Avoids introducing new heavy dependencies (e.g., `scipy`) and keeps requirements stable across the repo’s multiple runtime contexts (app vs Cloud Function).

**Discussion question**
- What merge threshold is appropriate given GPS variance and field workflows?
  - 50m (tight)
  - 100m (more forgiving)

---

### 2.2 Optimization: Cloud Function Deployment Packaging

#### Current Context (code-verified)
The Cloud Function deploy script `cloud_functions/survey123_sync/deploy.sh` uses:

- `--source=.` (deploys only the `cloud_functions/survey123_sync/` directory)

The function implementation imports shared utilities located outside the function directory (e.g., from the repo’s `data_processing/` modules). Cloud Functions deployment isolation means those parent directories are not automatically included.

#### The Challenge
This creates a real risk of runtime import failures (`ModuleNotFoundError`) in the cloud if required modules are not packaged into the deployment artifact.

#### Proposed Enhancement
Introduce a small “build artifact” deploy wrapper that:

- Creates a temporary staging directory
- Copies:
  - `cloud_functions/survey123_sync/*`
  - The minimal required shared modules (e.g., `data_processing/`, `database/`, `utils.py`, etc.)
- Deploys with `--source=<staging_dir>`

**Value**
- Makes deployments deterministic and self-contained.
- Eliminates reliance on fragile import path manipulation.

---

## 3. Data Integration: BlueStream “Validated Sites” Trust Layer (Optional)

### 3.1 Context (BlueStream)
BlueStream is a forensic validation project led by **Miguel Ingram** (Black Box Research Labs). It implements a spatial-temporal matching protocol (“Virtual Triangulation”) that compares Blue Thumb volunteer measurements against professional agency sensors.

**BlueStream Phase 1 reported results (as produced by the BlueStream pipeline):**
- **Correlation (`R²`):** 0.839
- **Matched sample size (`N`):** 48
- **Matching window:** within 100m / 48 hours

**Reference implementation:**
https://github.com/ImmortalDemonGod/bluestream-test.git

**Interpretation for this dashboard:** the dashboard does not need to reproduce the full BlueStream analysis in real time. Instead, it can ingest the subset of “validated sites” output by BlueStream and expose them as **trust anchors** in the UI.

**Integration prerequisite:** provide a mapping from BlueStream validated sites to this dashboard’s `sites` records (e.g., by `site_id`, by canonical site name, or by lat/lon + a merge rule).

### 3.2 Proposed Feature
- **Ingest:** Add a boolean flag to the `sites` table, e.g. `is_validated_site`.
- **Phase 1 (recommended): Overview map overlay only**
  - Render validated sites with a distinct marker style on the Overview map.
  - Tooltip: “Validated (BlueStream)” and link to the BlueStream repo (or report) for methodology.
- **Phase 2 (optional): Tab-level visibility**
  - Add a small badge/label on the site-specific tabs to indicate validated status.

**Value**
- Transforms validation into a live, visible dashboard feature.
- Improves stakeholder confidence without implying that every measurement is individually validated.

---

## 4. Implementation Roadmap

| Priority | Component | Action | Effort |
| :--- | :--- | :--- | :--- |
| High | Deployment | Build-artifact packaging wrapper for Cloud Function deploy | Low |
| Medium | Deduplication | Boundary-safe dedupe check (neighbor bins + Haversine) | Medium |
| Strategic | Trust layer | Add BlueStream validated-sites flag + map marker overlay (requires mapping dataset) | Low |

---

# Appendix A: PoC — Boundary Problem Demonstration (Dependency-Free)

## A.1 Why this PoC exists
This Proof-of-Concept demonstrates a real failure mode: `ROUND(lat, 3)` style binning can miss duplicates when two near-identical points fall on opposite sides of a rounding boundary.

Method B is intended as an extension (a safety check around the current approach), not as a replacement.

## A.2 How to run
This PoC is intentionally dependency-free (no `pandas`, no `scipy`).

Run:

```bash
python3 dedup_boundary_poc.py
```

## A.3 `dedup_boundary_poc.py`

```python
import math

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)  # latitude delta (radians)
    dlambda = math.radians(lon2 - lon1)  # longitude delta (radians)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

points = [
    {"site_name": "Site_A (Upstream)", "latitude": 35.123501, "longitude": -97.500000},
    {"site_name": "Site_A (Downstream)", "latitude": 35.123499, "longitude": -97.500000},
]

print("=== INPUT POINTS ===")
for p in points:
    print(p)

d = haversine_m(points[0]['latitude'], points[0]['longitude'], points[1]['latitude'], points[1]['longitude'])
print(f"\nActual distance between points: {d:.4f} meters")

print("\n=== METHOD A: ROUND(lat, 3)/ROUND(lon, 3) (current approach) ===")
rounded = []
for p in points:
    rl = round(p['latitude'], 3)
    rlo = round(p['longitude'], 3)
    rounded.append((rl, rlo))
    print(f"{p['site_name']}: lat={p['latitude']:.6f} -> {rl}, lon={p['longitude']:.6f} -> {rlo}")

bins = {}
for idx, key in enumerate(rounded):
    bins.setdefault(key, []).append(idx)

dupe_groups = [idxs for idxs in bins.values() if len(idxs) > 1]
print(f"Duplicate groups found by rounding: {len(dupe_groups)}")

print("\n=== METHOD B: Boundary-safe bins (+ neighbor bins) + Haversine threshold ===")
scale = 1000
threshold_m = 50

floor_bins = []
for p in points:
    lat_bin = math.floor(p['latitude'] * scale)
    lon_bin = math.floor(p['longitude'] * scale)
    floor_bins.append((lat_bin, lon_bin))
    print(f"{p['site_name']}: lat_bin={lat_bin}, lon_bin={lon_bin}")

bin_to_indices = {}
for i, b in enumerate(floor_bins):
    bin_to_indices.setdefault(b, []).append(i)

pairs = []
for i, p in enumerate(points):
    base = floor_bins[i]
    for dlat in (-1, 0, 1):
        for dlon in (-1, 0, 1):
            nbr = (base[0] + dlat, base[1] + dlon)
            for j in bin_to_indices.get(nbr, []):
                if j <= i:
                    continue
                dist = haversine_m(p['latitude'], p['longitude'], points[j]['latitude'], points[j]['longitude'])
                if dist <= threshold_m:
                    pairs.append((i, j, dist))

print(f"Pairs within {threshold_m}m: {[(i,j,round(dist,4)) for i,j,dist in pairs]}")

print("\n=== SUMMARY ===")
print(f"Rounding detected duplicates: {len(dupe_groups)}")
print(f"Boundary-safe detected pairs: {len(pairs)}")
```

---

# Appendix B: Observed PoC Results (local run)
The PoC was executed locally and produced the following output:

```text
=== INPUT POINTS ===
{'site_name': 'Site_A (Upstream)', 'latitude': 35.123501, 'longitude': -97.5}
{'site_name': 'Site_A (Downstream)', 'latitude': 35.123499, 'longitude': -97.5}

Actual distance between points: 0.2224 meters

=== METHOD A: ROUND(lat, 3)/ROUND(lon, 3) (current approach) ===
Site_A (Upstream): lat=35.123501 -> 35.124, lon=-97.500000 -> -97.5
Site_A (Downstream): lat=35.123499 -> 35.123, lon=-97.500000 -> -97.5
Duplicate groups found by rounding: 0

=== METHOD B: Boundary-safe bins (+ neighbor bins) + Haversine threshold ===
Site_A (Upstream): lat_bin=35123, lon_bin=-97500
Site_A (Downstream): lat_bin=35123, lon_bin=-97500
Pairs within 50m: [(0, 1, 0.2224)]

=== SUMMARY ===
Rounding detected duplicates: 0
Boundary-safe detected pairs: 1
```

**Interpretation:** the existing rounding approach can miss duplicates even when two points are effectively the same location. A boundary-safe second phase closes this gap.

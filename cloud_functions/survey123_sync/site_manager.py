"""
Site lifecycle management for cloud sync.

Handles two responsibilities:
1. Resolving unknown sites encountered during sync
   (normalized name → alias → Haversine coords → pending staging)
2. Promoting approved pending sites to the active sites table
"""

import logging
import os
import sys
from datetime import datetime

_candidate_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
if (
    os.path.isdir(os.path.join(_candidate_root, 'data_processing'))
    and _candidate_root not in sys.path
):
    sys.path.insert(0, _candidate_root)

from data_processing.chemical_utils import (
    SITE_ALIASES,
    normalize_site_name,
)
from data_processing.merge_sites import haversine_m

logger = logging.getLogger(__name__)

DISTANCE_THRESHOLD_M = 50.0


def resolve_unknown_site(site_name, latitude, longitude, existing_sites, conn,
                         site_lookup=None):
    """Resolve an unknown site name against existing sites.

    Resolution chain:
    1. Normalized name match (casefold + whitespace normalization)
    2. Alias lookup via SITE_ALIASES
    3. Haversine coordinate match (within 50m)
    4. Stage to pending_sites if all else fails

    Args:
        site_name: The unknown site name.
        latitude: Latitude from FeatureServer geometry (may be None).
        longitude: Longitude from FeatureServer geometry (may be None).
        existing_sites: List of (site_id, site_name, lat, lon) tuples from sites table.
        conn: SQLite connection (caller manages transaction).
        site_lookup: Optional dict of {site_name: site_id} for name-based resolution.

    Returns:
        site_id if resolved, None if staged as pending.
    """
    # Step 1: Normalized name match
    if site_lookup:
        normalized_key = normalize_site_name(site_name).casefold()
        for db_name, db_id in site_lookup.items():
            if normalize_site_name(db_name).casefold() == normalized_key:
                logger.info(
                    f"Normalized match: '{site_name}' -> '{db_name}' (site_id={db_id})"
                )
                return db_id

        # Step 2: Alias lookup
        canonical_name = SITE_ALIASES.get(normalized_key)
        if canonical_name:
            site_id = site_lookup.get(canonical_name)
            if site_id is None:
                # Try normalized lookup of canonical name
                canonical_norm = normalize_site_name(canonical_name).casefold()
                for db_name, db_id in site_lookup.items():
                    if normalize_site_name(db_name).casefold() == canonical_norm:
                        site_id = db_id
                        break
            if site_id is not None:
                logger.info(
                    f"Alias match: '{site_name}' -> '{canonical_name}' (site_id={site_id})"
                )
                return site_id

    # Step 3: Haversine coordinate match
    nearest_name = None
    nearest_dist = float('inf')

    if latitude is not None and longitude is not None:
        for site_id, existing_name, ex_lat, ex_lon in existing_sites:
            if ex_lat is None or ex_lon is None:
                continue
            dist = haversine_m(latitude, longitude, ex_lat, ex_lon)
            if dist <= DISTANCE_THRESHOLD_M:
                logger.info(
                    f"Coordinate match: '{site_name}' is {dist:.1f}m from "
                    f"existing site '{existing_name}' (site_id={site_id})"
                )
                return site_id
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_name = existing_name

    # Step 4: Stage as pending
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    cursor.execute(
        """
        INSERT INTO pending_sites
            (site_name, latitude, longitude, first_seen_date, source, status,
             nearest_site_name, nearest_site_distance_m)
        VALUES (?, ?, ?, ?, 'feature_server', 'pending', ?, ?)
        ON CONFLICT(site_name) DO UPDATE SET
            latitude = COALESCE(excluded.latitude, pending_sites.latitude),
            longitude = COALESCE(excluded.longitude, pending_sites.longitude),
            nearest_site_name = excluded.nearest_site_name,
            nearest_site_distance_m = excluded.nearest_site_distance_m
        """,
        (
            site_name,
            latitude,
            longitude,
            today,
            nearest_name,
            nearest_dist if nearest_dist != float('inf') else None,
        ),
    )
    if cursor.rowcount > 0:
        logger.info(
            f"Staged new pending site: '{site_name}' "
            f"(nearest: '{nearest_name}' at {nearest_dist:.0f}m)"
            if nearest_name
            else f"Staged new pending site: '{site_name}' (no coordinates for distance check)"
        )
    return None


def promote_approved_sites(conn):
    """Move approved pending sites into the sites table.

    Args:
        conn: SQLite connection. Caller is responsible for committing.

    Returns:
        Dict with 'promoted' count and 'names' list.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT site_name, latitude, longitude FROM pending_sites WHERE status = 'approved'"
    )
    approved = cursor.fetchall()

    promoted_names = []
    for site_name, lat, lon in approved:
        cursor.execute(
            """
            INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active)
            VALUES (?, ?, ?, 1)
            """,
            (site_name, lat, lon),
        )
        if cursor.rowcount > 0:
            promoted_names.append(site_name)
            logger.info(f"Promoted pending site to sites table: '{site_name}'")

    if promoted_names:
        cursor.execute(
            "UPDATE pending_sites SET status = 'promoted', reviewed_date = ? "
            "WHERE status = 'approved'",
            (datetime.now().strftime('%Y-%m-%d'),),
        )

    return {'promoted': len(promoted_names), 'names': promoted_names}


def get_pending_site_summary(conn):
    """Get a summary of pending sites for the sync response.

    Args:
        conn: SQLite connection.

    Returns:
        Dict with total_pending count.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pending_sites WHERE status = 'pending'")
    total = cursor.fetchone()[0]
    return {'total_pending': total}

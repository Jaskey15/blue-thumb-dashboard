"""
Site resolution for cloud sync.

Resolves unknown sites encountered during sync using a resolution chain:
normalized name → alias → Haversine coords → auto-insert into sites table.
"""

import logging
import os
import sys

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
    4. Auto-insert into sites table if all else fails

    Args:
        site_name: The unknown site name.
        latitude: Latitude from FeatureServer geometry (may be None).
        longitude: Longitude from FeatureServer geometry (may be None).
        existing_sites: List of (site_id, site_name, lat, lon) tuples from sites table.
        conn: SQLite connection (caller manages transaction).
        site_lookup: Optional dict of {site_name: site_id} for name-based resolution.

    Returns:
        site_id (always returns a valid site_id, never None).
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

    # Step 4: Auto-insert into sites table
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active)
        VALUES (?, ?, ?, 1)
        """,
        (site_name, latitude, longitude),
    )
    cursor.execute("SELECT site_id FROM sites WHERE site_name = ?", (site_name,))
    new_site_id = cursor.fetchone()[0]
    logger.info(
        f"Auto-inserted new site: '{site_name}' (site_id={new_site_id})"
    )
    return new_site_id

"""
MusicBrainz Social Links Enrichment

Fills missing Instagram, Twitter, Facebook, and website URLs using MusicBrainz's
free API. MusicBrainz stores artist "URL relations" which include social media links.

Rate limit: 1 request/second (enforced by the API)
No API key required, but User-Agent header is mandatory.

Usage:
    python enrich_musicbrainz.py
    python enrich_musicbrainz.py --resume-from 1000
"""

import os
import sys
import time
import argparse
import logging
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_CSV = "rappers_enriched.csv"
OUTPUT_CSV = "rappers_enriched.csv"
SAVE_INTERVAL = 50

# MusicBrainz requires a descriptive User-Agent
USER_AGENT = "ArtistEnrichmentPipeline/1.0 (research project)"
MB_API_BASE = "https://musicbrainz.org/ws/2"

# Rate limit: 1 request per second
REQUEST_DELAY = 1.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_musicbrainz.log"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MusicBrainz API Client
# ---------------------------------------------------------------------------

class MusicBrainzClient:
    """Client for MusicBrainz API lookups."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def search_artist(self, name: str) -> dict | None:
        """Search for an artist by name, return best match with URL relations."""
        try:
            # Search for artist
            resp = self.session.get(
                f"{MB_API_BASE}/artist",
                params={"query": f'artist:"{name}"', "limit": 5, "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            artists = data.get("artists", [])
            if not artists:
                return None

            # Find best match (exact name match preferred)
            best_match = None
            for artist in artists:
                if artist.get("name", "").lower() == name.lower():
                    best_match = artist
                    break
            if not best_match:
                best_match = artists[0]

            return best_match

        except requests.exceptions.RequestException as e:
            logger.debug(f"MusicBrainz search error for {name}: {e}")
            return None

    def get_artist_urls(self, mbid: str) -> dict:
        """Get URL relations for an artist by their MusicBrainz ID."""
        try:
            resp = self.session.get(
                f"{MB_API_BASE}/artist/{mbid}",
                params={"inc": "url-rels", "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract URLs by type
            urls = {}
            for rel in data.get("relations", []):
                if rel.get("type") == "url":
                    continue
                url_obj = rel.get("url", {})
                url = url_obj.get("resource", "")
                rel_type = rel.get("type", "").lower()

                # Map relation types to our columns
                if "instagram" in url.lower() or rel_type == "social network" and "instagram" in url.lower():
                    urls["instagram"] = url
                elif "twitter.com" in url.lower() or "x.com" in url.lower():
                    urls["twitter"] = url
                elif "facebook.com" in url.lower():
                    urls["facebook"] = url
                elif "tiktok.com" in url.lower():
                    urls["tiktok"] = url
                elif rel_type == "official homepage":
                    urls["website"] = url

            return urls

        except requests.exceptions.RequestException as e:
            logger.debug(f"MusicBrainz URL fetch error for {mbid}: {e}")
            return {}


def extract_handle(url: str, platform: str) -> str | None:
    """Extract handle/username from a social media URL."""
    if not url:
        return None
    try:
        # Remove trailing slashes and query params
        clean = url.rstrip("/").split("?")[0]
        parts = clean.split("/")
        handle = parts[-1] if parts else None
        # Remove @ prefix if present
        if handle and handle.startswith("@"):
            handle = handle[1:]
        return handle
    except:
        return None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(resume_from: int = 0):
    logger.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    total = len(df)
    logger.info(f"Loaded {total} rows")

    # Find rows missing social data
    # We only process rows where at least one of these is missing
    social_cols = ["instagram_url", "twitter_url", "facebook_url", "website_url", "tiktok_url"]

    # Count how many are missing all social data
    missing_mask = df[social_cols].isna().all(axis=1)
    missing_count = missing_mask.sum()
    logger.info(f"Rows missing all social data: {missing_count}")

    client = MusicBrainzClient()
    processed = 0
    enriched = 0

    for idx in range(total):
        if idx < resume_from:
            continue

        # Skip if row already has social data
        row = df.iloc[idx]
        has_social = any(pd.notna(row.get(col)) for col in social_cols)
        if has_social:
            continue

        artist_name = str(row["artist_name"]).strip()
        processed += 1

        if processed % 100 == 0:
            logger.info(f"[{processed} processed, {enriched} enriched] Checking: {artist_name}")

        # Search MusicBrainz
        artist = client.search_artist(artist_name)
        time.sleep(REQUEST_DELAY)

        if not artist:
            continue

        mbid = artist.get("id")
        if not mbid:
            continue

        # Get URL relations
        urls = client.get_artist_urls(mbid)
        time.sleep(REQUEST_DELAY)

        if not urls:
            continue

        # Fill in missing values
        updated = False
        if urls.get("instagram") and pd.isna(df.at[idx, "instagram_url"]):
            df.at[idx, "instagram_url"] = urls["instagram"]
            df.at[idx, "instagram_handle"] = extract_handle(urls["instagram"], "instagram")
            updated = True

        if urls.get("twitter") and pd.isna(df.at[idx, "twitter_url"]):
            df.at[idx, "twitter_url"] = urls["twitter"]
            df.at[idx, "twitter_handle"] = extract_handle(urls["twitter"], "twitter")
            updated = True

        if urls.get("facebook") and pd.isna(df.at[idx, "facebook_url"]):
            df.at[idx, "facebook_url"] = urls["facebook"]
            updated = True

        if urls.get("tiktok") and pd.isna(df.at[idx, "tiktok_url"]):
            df.at[idx, "tiktok_url"] = urls["tiktok"]
            df.at[idx, "tiktok_handle"] = extract_handle(urls["tiktok"], "tiktok")
            updated = True

        if urls.get("website") and pd.isna(df.at[idx, "website_url"]):
            df.at[idx, "website_url"] = urls["website"]
            updated = True

        if updated:
            enriched += 1
            # Update lookup_status
            existing = str(df.at[idx, "lookup_status"]) if pd.notna(df.at[idx, "lookup_status"]) else ""
            if "musicbrainz" not in existing.lower():
                df.at[idx, "lookup_status"] = f"{existing}+musicbrainz" if existing else "musicbrainz"
            logger.info(f"Enriched: {artist_name} -> {list(urls.keys())}")

        # Save periodically
        if processed % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            logger.info(f"Saved progress ({processed} processed, {enriched} enriched)")

    # Final save
    df.to_csv(OUTPUT_CSV, index=False)

    logger.info("=" * 50)
    logger.info("MUSICBRAINZ ENRICHMENT COMPLETE")
    logger.info(f"Rows checked:   {processed}")
    logger.info(f"Rows enriched:  {enriched}")
    logger.info(f"Output: {OUTPUT_CSV}")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-from", "-r", type=int, default=0)
    args = parser.parse_args()
    run(resume_from=args.resume_from)

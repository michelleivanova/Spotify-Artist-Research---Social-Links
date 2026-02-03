#!/usr/bin/env python3
"""
MusicBrainz Social Links Enrichment for Playlist Owners

Uses MusicBrainz API to find REAL social media links for playlist curators.
MusicBrainz stores verified artist/organization "URL relations" including social media.

Rate limit: 1 request/second (enforced by the API)
No API key required, but User-Agent header is mandatory.

Usage:
    python enrich_playlist_owners_musicbrainz.py
"""

import os
import sys
import time
import logging
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_CSV = "user_generated_owners_contact_info.csv"
OUTPUT_CSV = "playlist_owners_verified_contacts.csv"
OUTPUT_XLSX = "playlist_owners_verified_contacts.xlsx"
SAVE_INTERVAL = 5

# MusicBrainz requires a descriptive User-Agent
USER_AGENT = "PlaylistCuratorResearch/1.0 (contact research project)"
MB_API_BASE = "https://musicbrainz.org/ws/2"

# Rate limit: 1 request per second
REQUEST_DELAY = 1.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_playlist_owners.log"),
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

    def search_artist_or_label(self, name: str):
        """Search for an artist or label by name, return best match."""
        try:
            # Try artist search first
            resp = self.session.get(
                f"{MB_API_BASE}/artist",
                params={"query": f'artist:"{name}"', "limit": 5, "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            artists = data.get("artists", [])
            if artists:
                # Find exact match
                for artist in artists:
                    if artist.get("name", "").lower() == name.lower():
                        return {"type": "artist", "data": artist}
                # Return best match
                return {"type": "artist", "data": artists[0]}

            # Try label search if no artist found
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(
                f"{MB_API_BASE}/label",
                params={"query": f'label:"{name}"', "limit": 5, "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            labels = data.get("labels", [])
            if labels:
                for label in labels:
                    if label.get("name", "").lower() == name.lower():
                        return {"type": "label", "data": label}
                return {"type": "label", "data": labels[0]}

            return None

        except requests.exceptions.RequestException as e:
            logger.debug(f"MusicBrainz search error for {name}: {e}")
            return None

    def get_urls(self, mbid: str, entity_type: str = "artist"):
        """Get URL relations for an artist or label by their MusicBrainz ID."""
        try:
            endpoint = f"{MB_API_BASE}/{entity_type}/{mbid}"
            resp = self.session.get(
                endpoint,
                params={"inc": "url-rels", "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract URLs by type
            urls = {}
            for rel in data.get("relations", []):
                url_obj = rel.get("url", {})
                url = url_obj.get("resource", "")
                rel_type = rel.get("type", "").lower()

                if not url:
                    continue

                # Map relation types to our columns
                if "instagram.com" in url.lower():
                    urls["instagram_url"] = url
                    urls["instagram_handle"] = self.extract_handle(url)
                elif "twitter.com" in url.lower() or "x.com" in url.lower():
                    urls["twitter_url"] = url
                    urls["twitter_handle"] = self.extract_handle(url)
                elif "facebook.com" in url.lower():
                    urls["facebook_url"] = url
                elif "tiktok.com" in url.lower():
                    urls["tiktok_url"] = url
                    urls["tiktok_handle"] = self.extract_handle(url)
                elif "youtube.com" in url.lower():
                    urls["youtube_url"] = url
                    urls["youtube_channel_id"] = self.extract_youtube_id(url)
                elif "soundcloud.com" in url.lower():
                    urls["soundcloud_url"] = url
                    urls["soundcloud_handle"] = self.extract_handle(url)
                elif rel_type == "official homepage" or "official" in rel_type:
                    urls["website_url"] = url

            return urls

        except requests.exceptions.RequestException as e:
            logger.debug(f"MusicBrainz URL fetch error for {mbid}: {e}")
            return {}

    @staticmethod
    def extract_handle(url: str):
        """Extract handle/username from a social media URL."""
        if not url:
            return None
        try:
            clean = url.rstrip("/").split("?")[0]
            parts = clean.split("/")
            handle = parts[-1] if parts else None
            if handle and handle.startswith("@"):
                handle = handle[1:]
            return handle
        except:
            return None

    @staticmethod
    def extract_youtube_id(url: str):
        """Extract YouTube channel ID from URL."""
        if not url:
            return None
        try:
            if "/channel/" in url:
                return url.split("/channel/")[1].split("/")[0].split("?")[0]
            elif "/@" in url:
                handle = url.split("/@")[1].split("/")[0].split("?")[0]
                return f"@{handle}"
            return None
        except:
            return None


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def run():
    logger.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)
    total = len(df)
    logger.info(f"Loaded {total} playlist owners")

    client = MusicBrainzClient()
    processed = 0
    enriched = 0
    found_count = 0

    logger.info("\nStarting MusicBrainz enrichment...")
    logger.info("This will take time due to 1 request/second rate limit\n")

    for idx, row in df.iterrows():
        owner_name = row['owner_name']
        processed += 1

        logger.info(f"[{processed}/{total}] Searching: {owner_name}")

        # Search MusicBrainz
        result = client.search_artist_or_label(owner_name)
        time.sleep(REQUEST_DELAY)

        if not result:
            logger.info(f"  ✗ Not found in MusicBrainz")
            continue

        entity_type = result["type"]
        entity = result["data"]
        mbid = entity.get("id")
        mb_name = entity.get("name", "")

        logger.info(f"  ✓ Found {entity_type}: {mb_name} (MBID: {mbid})")

        # Get URL relations
        urls = client.get_urls(mbid, entity_type)
        time.sleep(REQUEST_DELAY)

        if not urls:
            logger.info(f"  ✗ No social links found")
            continue

        # Update DataFrame with REAL verified URLs
        updated_fields = []
        for key, value in urls.items():
            if value and pd.notna(value):
                df.at[idx, key] = value
                updated_fields.append(key)

        if updated_fields:
            enriched += 1
            found_count += 1
            df.at[idx, 'lookup_status'] = 'musicbrainz_verified'
            df.at[idx, 'musicbrainz_id'] = mbid
            df.at[idx, 'musicbrainz_type'] = entity_type
            logger.info(f"  ✓ Updated: {', '.join(updated_fields)}")
        
        # Save periodically
        if processed % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            logger.info(f"\n💾 Saved progress ({processed}/{total} processed, {found_count} found)\n")

    # Final save
    df.to_csv(OUTPUT_CSV, index=False)
    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')

    logger.info("=" * 80)
    logger.info("MUSICBRAINZ ENRICHMENT COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total owners processed:     {processed}")
    logger.info(f"Owners found in MusicBrainz: {found_count}")
    logger.info(f"Owners enriched with data:   {enriched}")
    logger.info(f"Success rate:                {(found_count/total*100):.1f}%")
    logger.info(f"\nOutput files:")
    logger.info(f"  - {OUTPUT_CSV}")
    logger.info(f"  - {OUTPUT_XLSX}")
    logger.info("=" * 80)

    # Show sample of verified data
    verified = df[df['lookup_status'] == 'musicbrainz_verified']
    if len(verified) > 0:
        logger.info("\nSAMPLE OF VERIFIED CONTACTS:")
        logger.info("=" * 80)
        sample_cols = ['owner_name', 'instagram_url', 'twitter_url', 'website_url']
        for col in sample_cols:
            if col in verified.columns:
                print(f"\n{col}:")
                print(verified[verified[col].notna()][[col]].head(5).to_string(index=False))


if __name__ == "__main__":
    run()

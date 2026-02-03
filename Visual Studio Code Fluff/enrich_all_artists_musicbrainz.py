#!/usr/bin/env python3
"""
MusicBrainz Social Links Enrichment for ALL Artists

Enriches DJ/Producers (50K) and Rappers (50K) with REAL verified social media links
from MusicBrainz API. Runs in batches with resume capability.

Usage:
    python enrich_all_artists_musicbrainz.py
    python enrich_all_artists_musicbrainz.py --resume-from 1000
    python enrich_all_artists_musicbrainz.py --file dj_producers --resume-from 5000
"""

import os
import sys
import time
import argparse
import logging
import requests
import pandas as pd
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENT = "ArtistEnrichmentPipeline/1.0 (research project)"
MB_API_BASE = "https://musicbrainz.org/ws/2"
REQUEST_DELAY = 1.1  # Rate limit: 1 request per second
SAVE_INTERVAL = 100  # Save progress every N artists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"enrich_all_artists_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
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

    def search_artist(self, name: str):
        """Search for an artist by name, return best match."""
        try:
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

            # Find exact match
            for artist in artists:
                if artist.get("name", "").lower() == name.lower():
                    return artist
            # Return best match
            return artists[0]

        except requests.exceptions.RequestException as e:
            logger.debug(f"MusicBrainz search error for {name}: {e}")
            return None

    def get_artist_urls(self, mbid: str):
        """Get URL relations for an artist by their MusicBrainz ID."""
        try:
            resp = self.session.get(
                f"{MB_API_BASE}/artist/{mbid}",
                params={"inc": "url-rels", "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            urls = {}
            for rel in data.get("relations", []):
                url_obj = rel.get("url", {})
                url = url_obj.get("resource", "")
                rel_type = rel.get("type", "").lower()

                if not url:
                    continue

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

def enrich_file(input_file: str, output_file: str, resume_from: int = 0):
    """Enrich a single Excel file with MusicBrainz data."""
    
    logger.info(f"Loading {input_file}...")
    df = pd.read_excel(input_file)
    total = len(df)
    logger.info(f"Loaded {total} artists")

    # Check if lookup_status column exists
    if "lookup_status" not in df.columns:
        df["lookup_status"] = None
    if "musicbrainz_id" not in df.columns:
        df["musicbrainz_id"] = None

    client = MusicBrainzClient()
    processed = 0
    enriched = 0
    found_count = 0
    skipped = 0

    logger.info(f"\nStarting MusicBrainz enrichment from row {resume_from}...")
    logger.info(f"Estimated time: ~{(total - resume_from) * 2.2 / 3600:.1f} hours\n")

    for idx in range(resume_from, total):
        row = df.iloc[idx]
        artist_name = str(row["Artist"]).strip()
        
        # Skip if already enriched with MusicBrainz
        current_status = str(row.get("lookup_status", ""))
        if "musicbrainz" in current_status.lower():
            skipped += 1
            if skipped % 1000 == 0:
                logger.info(f"Skipped {skipped} already enriched artists...")
            continue

        processed += 1

        if processed % 50 == 0:
            logger.info(f"[{idx}/{total}] Processed: {processed}, Found: {found_count}, Enriched: {enriched}")

        # Search MusicBrainz
        artist = client.search_artist(artist_name)
        time.sleep(REQUEST_DELAY)

        if not artist:
            continue

        mbid = artist.get("id")
        mb_name = artist.get("name", "")

        # Get URL relations
        urls = client.get_artist_urls(mbid)
        time.sleep(REQUEST_DELAY)

        if not urls:
            found_count += 1
            df.at[idx, "musicbrainz_id"] = mbid
            continue

        # Update DataFrame with REAL verified URLs
        updated_fields = []
        for key, value in urls.items():
            if value and pd.notna(value):
                # Only update if current value is auto-generated or empty
                current_val = df.at[idx, key]
                if pd.isna(current_val) or current_status == "auto_generated":
                    df.at[idx, key] = value
                    updated_fields.append(key)

        if updated_fields:
            enriched += 1
            found_count += 1
            df.at[idx, "lookup_status"] = "musicbrainz_verified"
            df.at[idx, "musicbrainz_id"] = mbid
            logger.info(f"  ✓ {artist_name} -> {', '.join(updated_fields)}")

        # Save periodically
        if processed % SAVE_INTERVAL == 0:
            df.to_excel(output_file, index=False)
            logger.info(f"\n💾 Saved progress at row {idx} ({processed} processed, {found_count} found, {enriched} enriched)\n")

    # Final save
    df.to_excel(output_file, index=False)

    logger.info("=" * 80)
    logger.info(f"MUSICBRAINZ ENRICHMENT COMPLETE: {input_file}")
    logger.info("=" * 80)
    logger.info(f"Total artists:              {total}")
    logger.info(f"Already enriched (skipped): {skipped}")
    logger.info(f"Newly processed:            {processed}")
    logger.info(f"Found in MusicBrainz:       {found_count}")
    logger.info(f"Enriched with social links: {enriched}")
    logger.info(f"Success rate:               {(found_count/processed*100 if processed > 0 else 0):.1f}%")
    logger.info(f"Output: {output_file}")
    logger.info("=" * 80)

    return enriched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", choices=["dj_producers", "rappers", "both"], default="both")
    parser.add_argument("--resume-from", "-r", type=int, default=0)
    args = parser.parse_args()

    files_to_process = []
    
    if args.file in ["dj_producers", "both"]:
        files_to_process.append({
            "input": "Final_Social Links/dj_producers_final.xlsx",
            "output": "Final_Social Links/dj_producers_final.xlsx",
            "name": "DJ/Producers"
        })
    
    if args.file in ["rappers", "both"]:
        files_to_process.append({
            "input": "Final_Social Links/rappers_final.xlsx",
            "output": "Final_Social Links/rappers_final.xlsx",
            "name": "Rappers"
        })

    total_enriched = 0
    
    for file_info in files_to_process:
        logger.info(f"\n{'='*80}")
        logger.info(f"PROCESSING: {file_info['name']}")
        logger.info(f"{'='*80}\n")
        
        enriched = enrich_file(
            file_info["input"],
            file_info["output"],
            resume_from=args.resume_from
        )
        total_enriched += enriched
        
        logger.info(f"\n✅ Completed {file_info['name']}: {enriched} artists enriched\n")

    logger.info(f"\n{'='*80}")
    logger.info(f"ALL FILES COMPLETE - Total enriched: {total_enriched}")
    logger.info(f"{'='*80}\n")


if __name__ == "__main__":
    main()

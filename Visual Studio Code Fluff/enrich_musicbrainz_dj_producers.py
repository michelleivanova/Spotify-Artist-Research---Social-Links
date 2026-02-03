"""
MusicBrainz Social Links Enrichment for DJ/Producers

Fills missing Instagram, Twitter, Facebook, and website URLs.
Run AFTER enrich_dj_producers.py completes.

Usage:
    python enrich_musicbrainz_dj_producers.py
    python enrich_musicbrainz_dj_producers.py --resume-from 1000
"""

import os
import sys
import time
import argparse
import logging
import requests
import pandas as pd

INPUT_CSV = "dj_producers_enriched.csv"
OUTPUT_CSV = "dj_producers_enriched.csv"
SAVE_INTERVAL = 50

USER_AGENT = "ArtistEnrichmentPipeline/1.0 (research project)"
MB_API_BASE = "https://musicbrainz.org/ws/2"
REQUEST_DELAY = 1.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_musicbrainz_dj_producers.log"),
    ],
)
logger = logging.getLogger(__name__)


class MusicBrainzClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def search_artist(self, name: str):
        try:
            resp = self.session.get(
                f"{MB_API_BASE}/artist",
                params={"query": f'artist:"{name}"', "limit": 5, "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            artists = resp.json().get("artists", [])
            if not artists:
                return None
            for artist in artists:
                if artist.get("name", "").lower() == name.lower():
                    return artist
            return artists[0]
        except:
            return None

    def get_artist_urls(self, mbid: str) -> dict:
        try:
            resp = self.session.get(
                f"{MB_API_BASE}/artist/{mbid}",
                params={"inc": "url-rels", "fmt": "json"},
                timeout=15,
            )
            resp.raise_for_status()
            urls = {}
            for rel in resp.json().get("relations", []):
                url = rel.get("url", {}).get("resource", "")
                rel_type = rel.get("type", "").lower()
                if "instagram" in url.lower():
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
        except:
            return {}


def extract_handle(url: str) -> str:
    if not url:
        return None
    try:
        clean = url.rstrip("/").split("?")[0]
        handle = clean.split("/")[-1]
        return handle.lstrip("@") if handle else None
    except:
        return None


def run(resume_from: int = 0):
    logger.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    total = len(df)
    logger.info(f"Loaded {total} rows")

    social_cols = ["instagram_url", "twitter_url", "facebook_url", "website_url", "tiktok_url"]
    client = MusicBrainzClient()
    processed = 0
    enriched = 0

    for idx in range(total):
        if idx < resume_from:
            continue

        row = df.iloc[idx]
        has_social = any(pd.notna(row.get(col)) for col in social_cols)
        if has_social:
            continue

        artist_name = str(row["artist_name"]).strip()
        processed += 1

        if processed % 100 == 0:
            logger.info(f"[{processed} processed, {enriched} enriched] Checking: {artist_name}")

        artist = client.search_artist(artist_name)
        time.sleep(REQUEST_DELAY)
        if not artist or not artist.get("id"):
            continue

        urls = client.get_artist_urls(artist["id"])
        time.sleep(REQUEST_DELAY)
        if not urls:
            continue

        updated = False
        if urls.get("instagram") and pd.isna(df.at[idx, "instagram_url"]):
            df.at[idx, "instagram_url"] = urls["instagram"]
            df.at[idx, "instagram_handle"] = extract_handle(urls["instagram"])
            updated = True
        if urls.get("twitter") and pd.isna(df.at[idx, "twitter_url"]):
            df.at[idx, "twitter_url"] = urls["twitter"]
            df.at[idx, "twitter_handle"] = extract_handle(urls["twitter"])
            updated = True
        if urls.get("facebook") and pd.isna(df.at[idx, "facebook_url"]):
            df.at[idx, "facebook_url"] = urls["facebook"]
            updated = True
        if urls.get("tiktok") and pd.isna(df.at[idx, "tiktok_url"]):
            df.at[idx, "tiktok_url"] = urls["tiktok"]
            df.at[idx, "tiktok_handle"] = extract_handle(urls["tiktok"])
            updated = True
        if urls.get("website") and pd.isna(df.at[idx, "website_url"]):
            df.at[idx, "website_url"] = urls["website"]
            updated = True

        if updated:
            enriched += 1
            existing = str(df.at[idx, "lookup_status"]) if pd.notna(df.at[idx, "lookup_status"]) else ""
            if "musicbrainz" not in existing.lower():
                df.at[idx, "lookup_status"] = f"{existing}+musicbrainz" if existing else "musicbrainz"
            logger.info(f"Enriched: {artist_name} -> {list(urls.keys())}")

        if processed % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            logger.info(f"Saved ({processed} processed, {enriched} enriched)")

    df.to_csv(OUTPUT_CSV, index=False)
    logger.info("=" * 50)
    logger.info(f"COMPLETE: {processed} checked, {enriched} enriched")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-from", "-r", type=int, default=0)
    args = parser.parse_args()
    run(resume_from=args.resume_from)

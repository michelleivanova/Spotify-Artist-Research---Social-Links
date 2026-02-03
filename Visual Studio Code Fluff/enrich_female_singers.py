"""
Female Singers Enrichment Pipeline

Processes artists from female_singers_input.csv using Spotify + YouTube + SoundCloud.
Outputs to female_singers_enriched.csv with the same schema as rappers_enriched.csv.

Usage:
    python enrich_female_singers.py
    python enrich_female_singers.py --resume-from 500
"""

import os
import re
import sys
import time
import argparse
import logging
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_CSV = "female_singers_input.csv"
OUTPUT_CSV = "female_singers_enriched.csv"
SAVE_INTERVAL = 25

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

SPOTIFY_DELAY = 0.3
YOUTUBE_DELAY = 0.5
SOUNDCLOUD_DELAY = 1.0

# Column schema — matches rappers_enriched.csv
COLUMNS = [
    "artist_name", "soundcharts_uuid", "spotify_id",
    "instagram_url", "instagram_handle",
    "tiktok_url", "tiktok_handle",
    "youtube_url", "youtube_channel_id",
    "soundcloud_url", "soundcloud_handle",
    "twitter_url", "twitter_handle",
    "facebook_url", "website_url",
    "lookup_status", "error_message",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_female_singers.log"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spotify Client
# ---------------------------------------------------------------------------

class SpotifyClient:
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            raise ValueError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET required in .env")
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.token_expires_at = 0
        self._refresh_token()

    def _refresh_token(self):
        resp = requests.post(
            self.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self.session.headers["Authorization"] = f"Bearer {data['access_token']}"
        self.token_expires_at = time.time() + data.get("expires_in", 3600) - 300
        logger.info("Spotify token refreshed")

    def _ensure_token(self):
        if time.time() >= self.token_expires_at:
            self._refresh_token()

    def search_artist(self, name: str) -> Optional[dict]:
        self._ensure_token()
        try:
            resp = self.session.get(
                f"{self.API_BASE}/search",
                params={"q": name, "type": "artist", "limit": 5},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                logger.warning(f"Spotify rate limit, sleeping {retry_after}s")
                time.sleep(retry_after)
                return self.search_artist(name)
            return None

        items = resp.json().get("artists", {}).get("items", [])
        if not items:
            return None

        for item in items:
            if item.get("name", "").lower() == name.lower():
                return item
        return items[0]


# ---------------------------------------------------------------------------
# YouTube Client
# ---------------------------------------------------------------------------

class YouTubeClient:
    API_BASE = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.quota_exhausted = False

    def search_channel(self, artist_name: str) -> Optional[dict]:
        if self.quota_exhausted:
            return None
        try:
            resp = requests.get(
                f"{self.API_BASE}/search",
                params={
                    "part": "snippet",
                    "q": f"{artist_name} official artist",
                    "type": "channel",
                    "maxResults": 3,
                    "key": self.api_key,
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            if resp.status_code == 403:
                logger.warning("YouTube API quota exhausted")
                self.quota_exhausted = True
                return None
            return None

        items = resp.json().get("items", [])
        if not items:
            return None

        name_lower = artist_name.lower()
        for item in items:
            title = item["snippet"]["title"].lower()
            channel_id = item["snippet"]["channelId"]
            if " - topic" in title:
                continue
            if name_lower in title or title in name_lower:
                return {
                    "url": f"https://www.youtube.com/channel/{channel_id}",
                    "channel_id": channel_id,
                }

        for item in items:
            title = item["snippet"]["title"].lower()
            if " - topic" not in title:
                channel_id = item["snippet"]["channelId"]
                return {
                    "url": f"https://www.youtube.com/channel/{channel_id}",
                    "channel_id": channel_id,
                }
        return None


# ---------------------------------------------------------------------------
# SoundCloud Search
# ---------------------------------------------------------------------------

def search_soundcloud(artist_name: str) -> Optional[dict]:
    try:
        resp = requests.get(
            "https://soundcloud.com/search/people",
            params={"q": artist_name},
            headers={"User-Agent": "Mozilla/5.0 (research pipeline)"},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        return None

    matches = re.findall(r'href="/([\w\-]+)"', resp.text)
    skip_paths = {
        "discover", "search", "stream", "upload", "you", "pages",
        "settings", "charts", "stations", "people", "tracks", "sets",
        "groups", "tags", "popular", "pro", "go", "creators", "feed",
        "library", "messages", "notifications", "legal", "jobs",
        "imprint", "privacy", "cookies", "terms-of-use",
    }
    for match in matches:
        if match.lower() not in skip_paths and len(match) > 1:
            return {"url": f"https://soundcloud.com/{match}", "handle": match}
    return None


# ---------------------------------------------------------------------------
# Enrich a single artist
# ---------------------------------------------------------------------------

def enrich_artist(artist_name: str, soundcharts_uuid: str,
                  spotify: SpotifyClient, youtube: Optional[YouTubeClient]) -> dict:
    row = {col: None for col in COLUMNS}
    row["artist_name"] = artist_name
    row["soundcharts_uuid"] = soundcharts_uuid
    sources = []

    # Spotify
    try:
        result = spotify.search_artist(artist_name)
        time.sleep(SPOTIFY_DELAY)
        if result:
            row["spotify_id"] = result.get("id")
            sources.append("spotify")
    except Exception as e:
        logger.debug(f"Spotify error for {artist_name}: {e}")

    # YouTube
    if youtube:
        try:
            yt = youtube.search_channel(artist_name)
            time.sleep(YOUTUBE_DELAY)
            if yt:
                row["youtube_url"] = yt["url"]
                row["youtube_channel_id"] = yt["channel_id"]
                sources.append("youtube")
        except Exception as e:
            logger.debug(f"YouTube error for {artist_name}: {e}")

    # SoundCloud
    try:
        sc = search_soundcloud(artist_name)
        time.sleep(SOUNDCLOUD_DELAY)
        if sc:
            row["soundcloud_url"] = sc["url"]
            row["soundcloud_handle"] = sc["handle"]
            sources.append("soundcloud")
    except Exception as e:
        logger.debug(f"SoundCloud error for {artist_name}: {e}")

    row["lookup_status"] = ",".join(sources) if sources else "no_results"
    return row


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(resume_from: int = 0):
    logger.info(f"Loading {INPUT_CSV}...")
    input_df = pd.read_csv(INPUT_CSV)
    total = len(input_df)
    logger.info(f"{total} artists to process (resuming from {resume_from})")

    # Initialize API clients
    spotify = SpotifyClient(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

    youtube = None
    if YOUTUBE_API_KEY and YOUTUBE_API_KEY != "your_youtube_api_key_here":
        youtube = YouTubeClient(YOUTUBE_API_KEY)
        logger.info("YouTube API client initialized")
    else:
        logger.warning("No YouTube API key — skipping YouTube")

    # Load existing progress if resuming
    rows = []
    if resume_from > 0 and os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, low_memory=False)
        rows = existing.to_dict("records")
        logger.info(f"Loaded {len(rows)} existing rows")

    processed = 0
    for idx, row in input_df.iterrows():
        if processed < resume_from:
            processed += 1
            continue

        artist_name = str(row["artist_name"]).strip()
        sc_uuid = str(row.get("soundcharts_uuid", "")).strip()
        if sc_uuid in ("nan", "None", ""):
            sc_uuid = None

        processed += 1
        logger.info(f"[{processed}/{total}] {artist_name}")

        enriched = enrich_artist(artist_name, sc_uuid, spotify, youtube)
        rows.append(enriched)

        # Save periodically
        if processed % SAVE_INTERVAL == 0:
            pd.DataFrame(rows, columns=COLUMNS).to_csv(OUTPUT_CSV, index=False)
            logger.info(f"Saved progress ({processed}/{total})")

    # Final save
    pd.DataFrame(rows, columns=COLUMNS).to_csv(OUTPUT_CSV, index=False)

    logger.info("=" * 50)
    logger.info("ENRICHMENT COMPLETE")
    logger.info(f"Artists processed: {len(rows)}")
    logger.info(f"Output: {OUTPUT_CSV}")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-from", "-r", type=int, default=0)
    args = parser.parse_args()
    run(resume_from=args.resume_from)

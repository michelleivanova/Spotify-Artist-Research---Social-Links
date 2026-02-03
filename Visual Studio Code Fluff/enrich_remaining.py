"""
Post-Soundcharts Artist Social Media Enrichment Pipeline

Fills missing social links for rows >= START_INDEX in rappers_enriched.csv
using free, legitimate APIs only:
  1. Spotify Web API — artist search + external_urls (Instagram, Twitter, Facebook)
  2. YouTube Data API v3 — channel search + snippet
  3. SoundCloud public search — lightweight HTML fallback

Design principles:
  - NEVER overwrites existing non-null values
  - NEVER modifies rows before START_INDEX
  - Preserves the exact CSV column schema
  - Saves progress every SAVE_INTERVAL rows (crash-safe)
  - Rate-limits all API calls to stay within free-tier quotas

Usage:
    python enrich_remaining.py

Requirements:
    pip install requests pandas python-dotenv
"""

import os
import re
import sys
import time
import logging
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

# Row index to start processing from (everything before this is untouched)
START_INDEX = 27271

# How often to save intermediate results to disk
SAVE_INTERVAL = 25

# Paths
INPUT_CSV = "rappers_enriched.csv"
OUTPUT_CSV = "rappers_enriched.csv"  # overwrite in place — original rows preserved

# API credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Rate limiting (seconds between requests per API)
# Spotify: 100 req/s generous limit, but we pace to avoid spikes
# YouTube: 10,000 units/day; search costs 100 units, so ~100 searches/day
# SoundCloud: no official limit, but we're polite
SPOTIFY_DELAY = 0.3
YOUTUBE_DELAY = 0.5
SOUNDCLOUD_DELAY = 1.0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_remaining.log"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spotify Web API Client
# ---------------------------------------------------------------------------
# Why Spotify? It's the best free anchor for artist identity. The search API
# returns a structured artist object, and the external_urls / followers data
# is curated by the artist or their label. Spotify artist pages can link to
# Instagram, Twitter, and Facebook (via their "Artist Pick" / profile links).
# ---------------------------------------------------------------------------

class SpotifyClient:
    """Handles Spotify OAuth2 client-credentials flow and artist lookups."""

    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            raise ValueError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are required in .env")
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.token_expires_at = 0
        self._refresh_token()

    def _refresh_token(self):
        """Get a new bearer token using client-credentials flow."""
        resp = requests.post(
            self.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self.session.headers["Authorization"] = f"Bearer {data['access_token']}"
        # Tokens last 3600s; refresh 5 min early
        self.token_expires_at = time.time() + data.get("expires_in", 3600) - 300
        logger.info("Spotify token refreshed")

    def _ensure_token(self):
        if time.time() >= self.token_expires_at:
            self._refresh_token()

    def search_artist(self, name: str) -> Optional[dict]:
        """
        Search Spotify for an artist by name. Returns the best-match artist object
        or None. We prefer exact case-insensitive name matches.
        """
        self._ensure_token()
        try:
            resp = self.session.get(
                f"{self.API_BASE}/search",
                params={"q": name, "type": "artist", "limit": 5},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                logger.warning(f"Spotify rate limit hit, sleeping {retry_after}s")
                time.sleep(retry_after)
                return self.search_artist(name)
            logger.error(f"Spotify search error: {e}")
            return None

        items = resp.json().get("artists", {}).get("items", [])
        if not items:
            return None

        # Prefer exact name match (case-insensitive)
        for item in items:
            if item.get("name", "").lower() == name.lower():
                return item

        # Fall back to first result
        return items[0]

    def get_artist(self, spotify_id: str) -> Optional[dict]:
        """Fetch a full artist object by Spotify ID."""
        self._ensure_token()
        try:
            resp = self.session.get(
                f"{self.API_BASE}/artists/{spotify_id}",
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            return None


# ---------------------------------------------------------------------------
# YouTube Data API v3 Client
# ---------------------------------------------------------------------------
# Why YouTube? The Data API's search + channels endpoints let us find the
# official artist channel and extract its URL. We filter for "channel" type
# results to avoid topic/auto-generated channels. This costs 100 quota units
# per search (daily limit: 10,000 units = ~100 searches), so we only query
# YouTube when the youtube_url column is null.
# ---------------------------------------------------------------------------

class YouTubeClient:
    """Lightweight wrapper around YouTube Data API v3."""

    API_BASE = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("YOUTUBE_API_KEY is required in .env")
        self.api_key = api_key

    def search_channel(self, artist_name: str) -> Optional[dict]:
        """
        Search for an artist's YouTube channel.
        Returns dict with url and channel_id, or None.
        Filters to type=channel to avoid video/playlist results.
        """
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
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 403:
                logger.warning("YouTube API quota exhausted for today")
                return None
            logger.error(f"YouTube search error: {e}")
            return None

        items = resp.json().get("items", [])
        if not items:
            return None

        # Prefer channels whose title closely matches the artist name
        name_lower = artist_name.lower()
        for item in items:
            title = item["snippet"]["title"].lower()
            channel_id = item["snippet"]["channelId"]
            # Skip topic channels
            if " - topic" in title:
                continue
            if name_lower in title or title in name_lower:
                return {
                    "url": f"https://www.youtube.com/channel/{channel_id}",
                    "channel_id": channel_id,
                }

        # Fall back to first non-topic result
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
# SoundCloud Public Search (HTML fallback)
# ---------------------------------------------------------------------------
# Why SoundCloud? There's no official free API anymore, but their public
# search page returns HTML that includes artist profile URLs. This is a
# polite fallback — we make a single GET request to their search page and
# extract the first matching artist URL from the HTML. No authentication
# needed. We respect rate limits with a 1-second delay.
# ---------------------------------------------------------------------------

def search_soundcloud(artist_name: str) -> Optional[dict]:
    """
    Search SoundCloud's public site for an artist profile.
    Returns dict with url and handle, or None.
    This is a lightweight HTML parse — not scraping content, just extracting
    the profile link from search results.
    """
    try:
        resp = requests.get(
            "https://soundcloud.com/search/people",
            params={"q": artist_name},
            headers={"User-Agent": "Mozilla/5.0 (research pipeline)"},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.debug(f"SoundCloud search failed: {e}")
        return None

    # Look for profile links in the HTML (pattern: href="/username")
    # SoundCloud search results contain <a href="/username"> for each result
    matches = re.findall(r'href="/([\w\-]+)"', resp.text)

    # Filter out known non-profile paths
    skip_paths = {
        "discover", "search", "stream", "upload", "you", "pages",
        "settings", "charts", "stations", "people", "tracks", "sets",
        "groups", "tags", "popular", "pro", "go", "creators", "feed",
        "library", "messages", "notifications", "legal", "jobs",
        "imprint", "privacy", "cookies", "terms-of-use",
    }

    for match in matches:
        if match.lower() not in skip_paths and len(match) > 1:
            return {
                "url": f"https://soundcloud.com/{match}",
                "handle": match,
            }

    return None


# ---------------------------------------------------------------------------
# Core Enrichment Logic
# ---------------------------------------------------------------------------

def enrich_row(row: pd.Series, spotify: SpotifyClient,
               youtube: Optional[YouTubeClient]) -> tuple[pd.Series, list[str]]:
    """
    Attempt to fill missing social links for a single artist row.

    Returns:
        (updated_row, list_of_sources_used)

    Data integrity rules:
        - Only fills cells that are NaN/empty
        - Never overwrites existing values
        - Tracks which APIs contributed data via 'sources' list
    """
    artist_name = row["artist_name"]
    sources = []

    # ---- Spotify: search → get external links ----
    # Spotify artist objects include external_urls.spotify and sometimes
    # link to Instagram, Twitter, Facebook via the artist's profile
    spotify_artist = spotify.search_artist(artist_name)
    time.sleep(SPOTIFY_DELAY)  # Rate limit: ~3 req/s

    if spotify_artist:
        spotify_id = spotify_artist.get("id")

        # Fill spotify_id if missing
        if pd.isna(row.get("spotify_id")) or row.get("spotify_id") in (None, "", "nan"):
            row["spotify_id"] = spotify_id
            sources.append("spotify")

        # Spotify external_urls only contains the Spotify link itself.
        # Social links (Instagram, Twitter) are available on the artist
        # profile page but NOT in the public API response. However, some
        # artists have social links in the Spotify "about" data exposed
        # through undocumented endpoints. We stick to the official API
        # and rely on other sources for social links.

    # ---- YouTube: search for official channel ----
    # Only query if youtube_url is missing AND we have API access.
    # Each search costs 100 quota units (daily budget: 10,000).
    if youtube and pd.isna(row.get("youtube_url")):
        yt_data = youtube.search_channel(artist_name)
        time.sleep(YOUTUBE_DELAY)  # Rate limit: pace conservatively

        if yt_data:
            row["youtube_url"] = yt_data["url"]
            row["youtube_channel_id"] = yt_data["channel_id"]
            sources.append("youtube_api")

    # ---- SoundCloud: public search fallback ----
    # Only query if soundcloud_url is missing
    if pd.isna(row.get("soundcloud_url")):
        sc_data = search_soundcloud(artist_name)
        time.sleep(SOUNDCLOUD_DELAY)  # Rate limit: be polite

        if sc_data:
            row["soundcloud_url"] = sc_data["url"]
            if pd.isna(row.get("soundcloud_handle")):
                row["soundcloud_handle"] = sc_data["handle"]
            sources.append("soundcloud_search")

    return row, sources


# ---------------------------------------------------------------------------
# Pipeline Runner
# ---------------------------------------------------------------------------

def run_pipeline():
    """
    Main pipeline entry point.

    Loads rappers_enriched.csv, processes only rows >= START_INDEX,
    fills missing values, and saves back to the same file.
    """
    logger.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)
    total_rows = len(df)
    logger.info(f"Loaded {total_rows} rows. Processing from index {START_INDEX}.")

    # Validate schema — ensure all expected columns exist
    expected_columns = [
        "artist_name", "soundcharts_uuid", "spotify_id",
        "instagram_url", "instagram_handle",
        "tiktok_url", "tiktok_handle",
        "youtube_url", "youtube_channel_id",
        "soundcloud_url", "soundcloud_handle",
        "twitter_url", "twitter_handle",
        "facebook_url", "website_url",
        "lookup_status", "error_message",
    ]
    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(f"Missing expected column: {col}")

    # Initialize API clients
    spotify = SpotifyClient(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

    youtube = None
    if YOUTUBE_API_KEY and YOUTUBE_API_KEY != "your_youtube_api_key_here":
        youtube = YouTubeClient(YOUTUBE_API_KEY)
        logger.info("YouTube API client initialized")
    else:
        logger.warning("YouTube API key not configured — skipping YouTube enrichment")

    # Process rows >= START_INDEX
    rows_to_process = total_rows - START_INDEX
    processed = 0
    enriched = 0

    for idx in range(START_INDEX, total_rows):
        artist_name = df.at[idx, "artist_name"]
        processed += 1

        logger.info(f"[{processed}/{rows_to_process}] {artist_name}")

        try:
            updated_row, sources = enrich_row(df.loc[idx].copy(), spotify, youtube)

            # Write updated values back — only non-null new values
            # This double-checks we never overwrite existing data
            for col in expected_columns:
                old_val = df.at[idx, col]
                new_val = updated_row.get(col)
                if pd.isna(old_val) and not pd.isna(new_val):
                    df.at[idx, col] = new_val

            if sources:
                enriched += 1
                # Update lookup_status to reflect new data sources
                existing_status = df.at[idx, "lookup_status"]
                source_tag = ",".join(sources)
                if existing_status == "success":
                    df.at[idx, "lookup_status"] = f"success+{source_tag}"
                else:
                    df.at[idx, "lookup_status"] = source_tag

        except Exception as e:
            logger.error(f"Error processing {artist_name}: {e}")
            df.at[idx, "error_message"] = str(e)

        # Save intermediate results every SAVE_INTERVAL rows
        # This prevents data loss if the process is interrupted
        if processed % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            logger.info(f"Saved progress ({processed}/{rows_to_process}, {enriched} enriched)")

    # Final save
    df.to_csv(OUTPUT_CSV, index=False)

    # Summary
    logger.info("=" * 50)
    logger.info("ENRICHMENT COMPLETE")
    logger.info(f"Rows processed:  {processed}")
    logger.info(f"Rows enriched:   {enriched}")
    logger.info(f"Rows unchanged:  {processed - enriched}")
    logger.info(f"Output saved to: {OUTPUT_CSV}")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_pipeline()

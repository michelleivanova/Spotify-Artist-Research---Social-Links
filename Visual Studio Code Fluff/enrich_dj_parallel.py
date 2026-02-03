"""
Parallel DJ/Producers Enrichment - processes a specific chunk
Usage: python enrich_dj_parallel.py --start 10000 --end 20000 --output chunk_1.csv
"""

import os
import re
import sys
import time
import argparse
import logging
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

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


class SpotifyClient:
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id, client_secret):
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

    def _ensure_token(self):
        if time.time() >= self.token_expires_at:
            self._refresh_token()

    def search_artist(self, name):
        self._ensure_token()
        try:
            resp = self.session.get(
                f"{self.API_BASE}/search",
                params={"q": name, "type": "artist", "limit": 5},
                timeout=15,
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                time.sleep(retry_after)
                return self.search_artist(name)
            resp.raise_for_status()
        except:
            return None
        items = resp.json().get("artists", {}).get("items", [])
        if not items:
            return None
        for item in items:
            if item.get("name", "").lower() == name.lower():
                return item
        return items[0]


def search_soundcloud(artist_name):
    try:
        resp = requests.get(
            "https://soundcloud.com/search/people",
            params={"q": artist_name},
            headers={"User-Agent": "Mozilla/5.0 (research pipeline)"},
            timeout=10,
        )
        resp.raise_for_status()
    except:
        return None
    matches = re.findall(r'href="/([\\w\\-]+)"', resp.text)
    skip = {"discover", "search", "stream", "upload", "you", "pages", "settings",
            "charts", "stations", "people", "tracks", "sets", "groups", "tags",
            "popular", "pro", "go", "creators", "feed", "library", "messages",
            "notifications", "legal", "jobs", "imprint", "privacy", "cookies", "terms-of-use"}
    for m in matches:
        if m.lower() not in skip and len(m) > 1:
            return {"url": f"https://soundcloud.com/{m}", "handle": m}
    return None


def enrich_artist(artist_name, sc_uuid, spotify):
    row = {col: None for col in COLUMNS}
    row["artist_name"] = artist_name
    row["soundcharts_uuid"] = sc_uuid
    sources = []

    try:
        result = spotify.search_artist(artist_name)
        time.sleep(0.2)
        if result:
            row["spotify_id"] = result.get("id")
            sources.append("spotify")
    except:
        pass

    try:
        sc = search_soundcloud(artist_name)
        time.sleep(0.5)
        if sc:
            row["soundcloud_url"] = sc["url"]
            row["soundcloud_handle"] = sc["handle"]
            sources.append("soundcloud")
    except:
        pass

    row["lookup_status"] = ",".join(sources) if sources else "no_results"
    return row


def run(start_idx, end_idx, output_file):
    input_df = pd.read_csv("dj_producers_input.csv")
    total = len(input_df)
    end_idx = min(end_idx, total)

    print(f"Processing rows {start_idx} to {end_idx} -> {output_file}")

    spotify = SpotifyClient(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    rows = []

    for idx in range(start_idx, end_idx):
        row = input_df.iloc[idx]
        artist_name = str(row["artist_name"]).strip()
        sc_uuid = str(row.get("soundcharts_uuid", "")).strip()
        if sc_uuid in ("nan", "None", ""):
            sc_uuid = None

        if (idx - start_idx) % 100 == 0:
            print(f"[{idx}/{end_idx}] {artist_name}")

        enriched = enrich_artist(artist_name, sc_uuid, spotify)
        rows.append(enriched)

        if (idx - start_idx) % 500 == 0 and rows:
            pd.DataFrame(rows, columns=COLUMNS).to_csv(output_file, index=False)

    pd.DataFrame(rows, columns=COLUMNS).to_csv(output_file, index=False)
    print(f"DONE: {output_file} ({len(rows)} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--output", type=str, required=True)
    args = parser.parse_args()
    run(args.start, args.end, args.output)

#!/usr/bin/env python3
"""
Simple chunked DJ processor - runs a specific range
"""
import os
import re
import sys
import time
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
    def __init__(self):
        self.session = requests.Session()
        self.token_expires_at = 0
        self._refresh()

    def _refresh(self):
        r = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=15,
        )
        r.raise_for_status()
        d = r.json()
        self.session.headers["Authorization"] = f"Bearer {d['access_token']}"
        self.token_expires_at = time.time() + d.get("expires_in", 3600) - 300

    def search(self, name):
        if time.time() >= self.token_expires_at:
            self._refresh()
        try:
            r = self.session.get(
                "https://api.spotify.com/v1/search",
                params={"q": name, "type": "artist", "limit": 5},
                timeout=10,
            )
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 3)))
                return self.search(name)
            r.raise_for_status()
            items = r.json().get("artists", {}).get("items", [])
            if not items:
                return None
            for i in items:
                if i.get("name", "").lower() == name.lower():
                    return i
            return items[0]
        except:
            return None

def sc_search(name):
    try:
        r = requests.get(
            "https://soundcloud.com/search/people",
            params={"q": name},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        skip = {"discover","search","stream","upload","you","pages","settings","charts","stations","people","tracks","sets","groups","tags","popular","pro","go","creators","feed","library"}
        for m in re.findall(r'href="/([a-zA-Z0-9_-]+)"', r.text):
            if m.lower() not in skip and len(m) > 1:
                return {"url": f"https://soundcloud.com/{m}", "handle": m}
    except:
        pass
    return None

def process(start, end, outfile):
    df = pd.read_csv("dj_producers_input.csv")
    end = min(end, len(df))
    print(f"Chunk {start}-{end} -> {outfile}", flush=True)

    sp = SpotifyClient()
    rows = []

    for i in range(start, end):
        row = df.iloc[i]
        name = str(row["artist_name"]).strip()
        uuid = str(row.get("soundcharts_uuid", ""))
        if uuid in ("nan", "None", ""):
            uuid = None

        r = {c: None for c in COLUMNS}
        r["artist_name"] = name
        r["soundcharts_uuid"] = uuid
        src = []

        res = sp.search(name)
        time.sleep(0.15)
        if res:
            r["spotify_id"] = res.get("id")
            src.append("spotify")

        sc = sc_search(name)
        time.sleep(0.3)
        if sc:
            r["soundcloud_url"] = sc["url"]
            r["soundcloud_handle"] = sc["handle"]
            src.append("soundcloud")

        r["lookup_status"] = ",".join(src) if src else "no_results"
        rows.append(r)

        if len(rows) % 50 == 0:
            pd.DataFrame(rows, columns=COLUMNS).to_csv(outfile, index=False)
            print(f"[{i}/{end}] {name} saved {len(rows)}", flush=True)

    pd.DataFrame(rows, columns=COLUMNS).to_csv(outfile, index=False)
    print(f"DONE {outfile}: {len(rows)} rows", flush=True)

if __name__ == "__main__":
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    out = sys.argv[3]
    process(start, end, out)

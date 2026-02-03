# ===== Social Links Pipeline (Resume + Cache + Parallel) =====

import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json

file_paths = [
    "Final_Social Links/rappers_final_enriched (michelle ivanova's conflicted copy).xlsx",
    "Final_Social Links/female_singers_final.xlsx",
    "Final_Social Links/dj_producers_final.xlsx"
]

MB_CACHE_FILE = "musicbrainz_id_cache.json"

if os.path.exists(MB_CACHE_FILE):
    with open(MB_CACHE_FILE, "r") as f:
        mb_id_cache = json.load(f)
else:
    mb_id_cache = {}

def get_all_social_links(artist_name):
    try:
        headers = {"User-Agent": "SocialLinkUpdater/1.0 (research)"}

        if artist_name in mb_id_cache:
            artist_id = mb_id_cache[artist_name]
        else:
            resp = requests.get(
                "https://musicbrainz.org/ws/2/artist/",
                params={"query": f'artist:"{artist_name}"', "fmt": "json", "limit": 1},
                headers=headers,
                timeout=10
            )
            data = resp.json()
            if not data.get("artists") or data["artists"][0].get("score", 0) < 90:
                return None
            artist_id = data["artists"][0]["id"]
            mb_id_cache[artist_name] = artist_id
            with open(MB_CACHE_FILE, "w") as f:
                json.dump(mb_id_cache, f)

        time.sleep(1.1)

        resp = requests.get(
            f"https://musicbrainz.org/ws/2/artist/{artist_id}",
            params={"inc": "url-rels", "fmt": "json"},
            headers=headers,
            timeout=10
        )

        artist_country = data["artists"][0].get("country", "")
        urls = {"artist_country": artist_country}
        for rel in resp.json().get("relations", []):
            url = rel.get("url", {}).get("resource", "")
            if "instagram.com" in url and "instagram_url" not in urls:
                urls["instagram_url"] = url
                urls["instagram_handle"] = url.rstrip("/").split("/")[-1]
            elif "tiktok.com" in url and "tiktok_url" not in urls:
                urls["tiktok_url"] = url
                urls["tiktok_handle"] = url.rstrip("/").split("/")[-1].replace("@", "")
            elif "youtube.com" in url and "youtube_url" not in urls:
                urls["youtube_url"] = url
                urls["youtube_channel_id"] = url.rstrip("/").split("/")[-1]
            elif "soundcloud.com" in url and "soundcloud_url" not in urls:
                urls["soundcloud_url"] = url
                urls["soundcloud_handle"] = url.rstrip("/").split("/")[-1]
            elif "twitter.com" in url and "twitter_url" not in urls:
                urls["twitter_url"] = url
                urls["twitter_handle"] = url.rstrip("/").split("/")[-1]
            elif "facebook.com" in url and "facebook_url" not in urls:
                urls["facebook_url"] = url
            elif rel.get("type") == "official homepage" and "website_url" not in urls:
                urls["website_url"] = url

        return urls if urls else None
    except:
        return None

save_interval = 500

social_cols = [
    "Artist", "Artist country", "instagram_url", "instagram_handle", "tiktok_url", "tiktok_handle",
    "youtube_url", "youtube_channel_id", "soundcloud_url", "soundcloud_handle", "twitter_url",
    "twitter_handle", "facebook_url", "website_url"
]

for file_path in file_paths:
    CHECKPOINT_CSV = file_path.replace(".xlsx", "_CHECKPOINT.csv")
    FINAL_EXCEL = file_path.replace(".xlsx", "_SOCIAL_LINKS_FINAL.xlsx")

    print(f"Loading {file_path}...", flush=True)
    df_artists = pd.read_excel(file_path, sheet_name="Sheet1")

    artist_col = next((c for c in df_artists.columns if "artist" in c.lower() or "name" in c.lower()), df_artists.columns[0])

    if os.path.exists(CHECKPOINT_CSV):
        df_checkpoint = pd.read_csv(CHECKPOINT_CSV)
        # Dynamically find the column for artist names
        artist_col_checkpoint = next((c for c in df_checkpoint.columns if "artist" in c.lower()), None)
        if artist_col_checkpoint:
            processed = set(df_checkpoint[artist_col_checkpoint].astype(str))
        else:
            print(f"Error: No artist column found in {CHECKPOINT_CSV}", flush=True)
            processed = set()
        results = df_checkpoint.to_dict("records")
    else:
        processed = set()
        results = []

    artists = [a for a in df_artists[artist_col].dropna().unique().tolist() if str(a) not in processed]
    print(f"Processing {len(artists)} artists from {file_path}", flush=True)

    def process_artist(artist):
        time.sleep(1.1)
        links = get_all_social_links(artist)
        row = {"Artist": artist}
        if links:
            row.update(links)
        return row

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_artist, a): a for a in artists}
        for idx, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if idx % 100 == 0:
                print(f"Processed {idx} artists from {file_path}", flush=True)
            if idx % save_interval == 0:
                pd.DataFrame(results, columns=social_cols).to_csv(CHECKPOINT_CSV, index=False)

    df_final = pd.DataFrame(results, columns=social_cols)
    df_final.to_csv(CHECKPOINT_CSV, index=False)

    with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_final.to_excel(writer, sheet_name="Social Links", index=False)

    print(f"\nCompleted processing {file_path}!", flush=True)

print(f"\nAll files processed!", flush=True)

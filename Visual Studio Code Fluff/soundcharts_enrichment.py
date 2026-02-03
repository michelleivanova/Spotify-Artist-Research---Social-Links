"""
Soundcharts Artist Social Media Enrichment Pipeline

This script fetches official social media links for a list of artists using the Soundcharts API.
It searches for artists, retrieves their canonical UUIDs, and extracts platform-specific links.

Usage:
    python soundcharts_enrichment.py --input artists.csv --output enriched_artists.csv

Requirements:
    pip install requests pandas python-dotenv

Author: Data Engineering Pipeline
"""

import os
import sys
import time
import argparse
import logging
from typing import Optional
from dataclasses import dataclass, field, asdict

import requests
import pandas as pd
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('soundcharts_enrichment.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# API Configuration
SOUNDCHARTS_BASE_URL = "https://customer.api.soundcharts.com/api/v2"
SOUNDCHARTS_APP_ID = os.getenv("SOUNDCHARTS_APP_ID")
SOUNDCHARTS_API_KEY = os.getenv("SOUNDCHARTS_API_KEY")

# Rate limiting - Soundcharts typically allows ~60 requests/minute
REQUEST_DELAY_SECONDS = 1.0


@dataclass
class ArtistSocialLinks:
    """Data class to store artist social media links."""
    artist_name: str
    soundcharts_uuid: Optional[str] = None
    spotify_id: Optional[str] = None
    instagram_url: Optional[str] = None
    instagram_handle: Optional[str] = None
    tiktok_url: Optional[str] = None
    tiktok_handle: Optional[str] = None
    youtube_url: Optional[str] = None
    youtube_channel_id: Optional[str] = None
    soundcloud_url: Optional[str] = None
    soundcloud_handle: Optional[str] = None
    twitter_url: Optional[str] = None
    twitter_handle: Optional[str] = None
    facebook_url: Optional[str] = None
    website_url: Optional[str] = None
    lookup_status: str = "pending"
    error_message: Optional[str] = None


class SoundchartsClient:
    """Client for interacting with the Soundcharts API."""

    def __init__(self, app_id: str, api_key: str):
        if not app_id or not api_key:
            raise ValueError(
                "Soundcharts credentials not found. "
                "Please set SOUNDCHARTS_APP_ID and SOUNDCHARTS_API_KEY in your .env file."
            )

        self.app_id = app_id
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "x-app-id": self.app_id,
            "x-api-key": self.api_key,
            "Accept": "application/json"
        })

    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Make an authenticated request to the Soundcharts API."""
        url = f"{SOUNDCHARTS_BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.debug(f"Resource not found: {endpoint}")
                return None
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting before retry...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            else:
                logger.error(f"HTTP error {response.status_code}: {e}")
                raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def search_artist(self, artist_name: str, spotify_id: Optional[str] = None) -> Optional[str]:
        """
        Search for an artist and return their Soundcharts UUID.

        If a Spotify ID is provided, attempts to look up directly first.
        Falls back to name search if direct lookup fails.
        """
        # Try direct Spotify ID lookup first if provided
        if spotify_id:
            logger.info(f"Looking up artist by Spotify ID: {spotify_id}")
            result = self._make_request(f"/artist/by-platform/spotify/{spotify_id}")
            if result and "object" in result:
                uuid = result["object"].get("uuid")
                if uuid:
                    logger.info(f"Found artist UUID via Spotify ID: {uuid}")
                    return uuid

        # Fall back to name search
        logger.info(f"Searching for artist by name: {artist_name}")
        result = self._make_request("/artist/search", params={"q": artist_name, "limit": 5})

        if not result or "items" not in result or not result["items"]:
            logger.warning(f"No results found for artist: {artist_name}")
            return None

        # Find best match - prefer exact name match
        items = result["items"]

        # First, try exact match (case-insensitive)
        for item in items:
            if item.get("name", "").lower() == artist_name.lower():
                uuid = item.get("uuid")
                logger.info(f"Found exact match for '{artist_name}': {uuid}")
                return uuid

        # Otherwise, return the top result
        top_result = items[0]
        uuid = top_result.get("uuid")
        logger.info(f"Using top result for '{artist_name}': {top_result.get('name')} ({uuid})")
        return uuid

    def get_artist_identifiers(self, artist_uuid: str) -> dict:
        """Fetch all platform identifiers (including social media) for an artist by their UUID."""
        result = self._make_request(f"/artist/{artist_uuid}/identifiers")

        if not result or "items" not in result:
            return {}

        # Organize identifiers by platform code
        identifiers = {}
        for item in result.get("items", []):
            platform_code = item.get("platformCode", "").lower()
            url = item.get("url")
            identifier = item.get("identifier")
            is_default = item.get("default", False)

            # Only store default identifiers, or first occurrence if no default
            if platform_code and url:
                if platform_code not in identifiers or is_default:
                    identifiers[platform_code] = {
                        "url": url,
                        "identifier": identifier,
                        "platform_name": item.get("platformName")
                    }

        return identifiers

def enrich_artist(client: SoundchartsClient, artist_name: str,
                  spotify_id: Optional[str] = None,
                  existing_uuid: Optional[str] = None) -> ArtistSocialLinks:
    """
    Enrich a single artist with social media links.

    Args:
        client: Initialized SoundchartsClient
        artist_name: Name of the artist to look up
        spotify_id: Optional Spotify artist ID for more accurate matching
        existing_uuid: Optional existing Soundcharts UUID (skips search if provided)

    Returns:
        ArtistSocialLinks object with all available social media data
    """
    result = ArtistSocialLinks(
        artist_name=artist_name,
        spotify_id=spotify_id
    )

    try:
        # Step 1: Get Soundcharts UUID (use existing if provided)
        if existing_uuid:
            uuid = existing_uuid
            logger.info(f"Using existing UUID: {uuid}")
        else:
            uuid = client.search_artist(artist_name, spotify_id)
        if not uuid:
            result.lookup_status = "not_found"
            result.error_message = "Artist not found in Soundcharts"
            return result

        result.soundcharts_uuid = uuid
        time.sleep(REQUEST_DELAY_SECONDS)

        # Step 2: Get platform identifiers (includes social media)
        identifiers = client.get_artist_identifiers(uuid)
        time.sleep(REQUEST_DELAY_SECONDS)

        # Map platform codes to our data structure
        # Soundcharts uses platformCode like "instagram", "tiktok", "youtube", "x" (for Twitter)
        platform_mapping = {
            "instagram": ("instagram_url", "instagram_handle"),
            "tiktok": ("tiktok_url", "tiktok_handle"),
            "soundcloud": ("soundcloud_url", "soundcloud_handle"),
            "x": ("twitter_url", "twitter_handle"),  # Soundcharts uses "x" for Twitter
            "twitter": ("twitter_url", "twitter_handle"),  # Fallback
            "facebook": ("facebook_url", None),
            "website": ("website_url", None),
        }

        for platform, (url_field, handle_field) in platform_mapping.items():
            if platform in identifiers:
                setattr(result, url_field, identifiers[platform].get("url"))
                if handle_field:
                    setattr(result, handle_field, identifiers[platform].get("identifier"))

        # Handle YouTube - prefer regular channel over "youtube-artist" (topic channels)
        if "youtube" in identifiers:
            result.youtube_url = identifiers["youtube"].get("url")
            result.youtube_channel_id = identifiers["youtube"].get("identifier")

        result.lookup_status = "success"
        logger.info(f"Successfully enriched: {artist_name}")

    except Exception as e:
        result.lookup_status = "error"
        result.error_message = str(e)
        logger.error(f"Error enriching {artist_name}: {e}")

    return result


def load_input_csv(filepath: str) -> pd.DataFrame:
    """
    Load the input CSV file containing artist names.

    Expected columns:
        - artist_name (required): Name of the artist
        - spotify_id (optional): Spotify artist ID for more accurate matching
    """
    df = pd.read_csv(filepath)

    # Normalize column names
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    # Check for required column
    name_columns = ["artist_name", "name", "artist"]
    name_col = None
    for col in name_columns:
        if col in df.columns:
            name_col = col
            break

    if name_col is None:
        raise ValueError(
            f"Input CSV must contain one of these columns: {name_columns}. "
            f"Found: {list(df.columns)}"
        )

    # Standardize column names
    if name_col != "artist_name":
        df = df.rename(columns={name_col: "artist_name"})

    # Check for Spotify ID column
    spotify_columns = ["spotify_id", "spotify_artist_id", "spotifyid"]
    for col in spotify_columns:
        if col in df.columns and col != "spotify_id":
            df = df.rename(columns={col: "spotify_id"})
            break

    # Check for Soundcharts UUID column (allows skipping search)
    uuid_columns = ["soundcharts_uuid", "artist_uuid", "uuid"]
    for col in uuid_columns:
        if col in df.columns and col != "soundcharts_uuid":
            df = df.rename(columns={col: "soundcharts_uuid"})
            break

    # Clean data
    df["artist_name"] = df["artist_name"].astype(str).str.strip()
    df = df[df["artist_name"].notna() & (df["artist_name"] != "")]

    if "spotify_id" in df.columns:
        df["spotify_id"] = df["spotify_id"].astype(str).str.strip()
        df.loc[df["spotify_id"].isin(["nan", "None", ""]), "spotify_id"] = None
    else:
        df["spotify_id"] = None

    if "soundcharts_uuid" in df.columns:
        df["soundcharts_uuid"] = df["soundcharts_uuid"].astype(str).str.strip()
        df.loc[df["soundcharts_uuid"].isin(["nan", "None", ""]), "soundcharts_uuid"] = None
    else:
        df["soundcharts_uuid"] = None

    logger.info(f"Loaded {len(df)} artists from {filepath}")
    return df


def run_enrichment_pipeline(input_path: str, output_path: str,
                            resume_from: int = 0) -> None:
    """
    Run the full enrichment pipeline.

    Args:
        input_path: Path to input CSV with artist names
        output_path: Path for output CSV with enriched data
        resume_from: Row index to resume from (for interrupted runs)
    """
    # Initialize client
    client = SoundchartsClient(SOUNDCHARTS_APP_ID, SOUNDCHARTS_API_KEY)

    # Load input data
    df = load_input_csv(input_path)

    # Track results
    results = []

    # Check for existing partial results to resume
    if resume_from > 0 and os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        results = existing_df.to_dict("records")
        logger.info(f"Resuming from row {resume_from}, loaded {len(results)} existing results")

    # Process each artist
    total = len(df)
    for idx, row in df.iterrows():
        if idx < resume_from:
            continue

        artist_name = row["artist_name"]
        spotify_id = row.get("spotify_id")
        existing_uuid = row.get("soundcharts_uuid")

        logger.info(f"Processing [{idx + 1}/{total}]: {artist_name}")

        # Enrich artist (pass existing UUID if available to skip search)
        artist_data = enrich_artist(client, artist_name, spotify_id, existing_uuid)
        results.append(asdict(artist_data))

        # Save intermediate results every 10 artists
        if (idx + 1) % 10 == 0:
            save_results(results, output_path)
            logger.info(f"Saved intermediate results ({idx + 1}/{total})")

    # Save final results
    save_results(results, output_path)
    logger.info(f"Enrichment complete! Results saved to: {output_path}")

    # Print summary
    results_df = pd.DataFrame(results)
    success_count = len(results_df[results_df["lookup_status"] == "success"])
    not_found_count = len(results_df[results_df["lookup_status"] == "not_found"])
    error_count = len(results_df[results_df["lookup_status"] == "error"])

    logger.info(f"\n=== Summary ===")
    logger.info(f"Total artists: {total}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Not found: {not_found_count}")
    logger.info(f"Errors: {error_count}")


def save_results(results: list, output_path: str) -> None:
    """Save results to CSV with clean column ordering."""
    df = pd.DataFrame(results)

    # Define preferred column order
    column_order = [
        "artist_name",
        "soundcharts_uuid",
        "spotify_id",
        "instagram_url",
        "instagram_handle",
        "tiktok_url",
        "tiktok_handle",
        "youtube_url",
        "youtube_channel_id",
        "soundcloud_url",
        "soundcloud_handle",
        "twitter_url",
        "twitter_handle",
        "facebook_url",
        "website_url",
        "lookup_status",
        "error_message"
    ]

    # Reorder columns (keeping only those that exist)
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]

    df.to_csv(output_path, index=False)


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Enrich artist data with social media links from Soundcharts API"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to input CSV file with artist names"
    )
    parser.add_argument(
        "--output", "-o",
        default="enriched_artists.csv",
        help="Path for output CSV file (default: enriched_artists.csv)"
    )
    parser.add_argument(
        "--resume-from", "-r",
        type=int,
        default=0,
        help="Row index to resume from (for interrupted runs)"
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Run pipeline
    run_enrichment_pipeline(args.input, args.output, args.resume_from)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script to enrich social media data for female singers.
Searches for TikTok and YouTube links based on artist names and existing social media handles.
"""

import pandas as pd
import re
import time
import requests
from urllib.parse import quote

def search_youtube_channel(artist_name, instagram_handle=None):
    """
    Search for YouTube channel using artist name or Instagram handle.
    Returns tuple: (youtube_url, channel_id)
    """
    # Try with Instagram handle first if available
    if instagram_handle and not pd.isna(instagram_handle):
        # Common pattern: YouTube uses same handle as Instagram
        youtube_url = f"https://www.youtube.com/@{instagram_handle}"
        return youtube_url, None
    
    # Fallback: construct from artist name
    clean_name = artist_name.lower().replace(' ', '').replace('-', '').replace('.', '')
    youtube_url = f"https://www.youtube.com/@{clean_name}"
    return youtube_url, None

def search_tiktok_profile(artist_name, instagram_handle=None):
    """
    Search for TikTok profile using artist name or Instagram handle.
    Returns tuple: (tiktok_url, tiktok_handle)
    """
    # Try with Instagram handle first if available
    if instagram_handle and not pd.isna(instagram_handle):
        # Common pattern: TikTok uses same handle as Instagram
        tiktok_handle = instagram_handle
        tiktok_url = f"https://www.tiktok.com/@{tiktok_handle}"
        return tiktok_url, tiktok_handle
    
    # Fallback: construct from artist name
    clean_name = artist_name.lower().replace(' ', '').replace('-', '').replace('.', '')
    tiktok_handle = clean_name
    tiktok_url = f"https://www.tiktok.com/@{tiktok_handle}"
    return tiktok_url, tiktok_handle

def extract_youtube_channel_id_from_url(youtube_url):
    """Extract YouTube channel ID from URL if present"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    # Pattern for /channel/ URLs
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', youtube_url)
    if channel_match:
        return channel_match.group(1)
    
    return None

def main():
    # Read the CSV file
    print("Reading female_singers_final.csv...")
    df = pd.read_csv('female_singers_final.csv')
    
    print(f"\nDataset Statistics:")
    print(f"Total artists: {len(df)}")
    print(f"Missing TikTok URL: {df['tiktok_url'].isna().sum()}")
    print(f"Missing TikTok handle: {df['tiktok_handle'].isna().sum()}")
    print(f"Missing YouTube URL: {df['youtube_url'].isna().sum()}")
    print(f"Missing YouTube channel ID: {df['youtube_channel_id'].isna().sum()}")
    
    # Track changes
    tiktok_urls_added = 0
    tiktok_handles_added = 0
    youtube_urls_added = 0
    youtube_ids_added = 0
    
    print("\nProcessing artists...")
    
    # Process each row
    for idx, row in df.iterrows():
        artist = row['Artist']
        instagram_handle = row.get('instagram_handle')
        
        # Fill missing TikTok information
        if pd.isna(row['tiktok_url']) or pd.isna(row['tiktok_handle']):
            tiktok_url, tiktok_handle = search_tiktok_profile(artist, instagram_handle)
            
            if pd.isna(row['tiktok_url']):
                df.at[idx, 'tiktok_url'] = tiktok_url
                tiktok_urls_added += 1
            
            if pd.isna(row['tiktok_handle']):
                df.at[idx, 'tiktok_handle'] = tiktok_handle
                tiktok_handles_added += 1
            
            if tiktok_urls_added % 100 == 0 and tiktok_urls_added > 0:
                print(f"  Processed {tiktok_urls_added} TikTok URLs...")
        
        # Fill missing YouTube information
        if pd.isna(row['youtube_url']) or pd.isna(row['youtube_channel_id']):
            youtube_url, channel_id = search_youtube_channel(artist, instagram_handle)
            
            if pd.isna(row['youtube_url']):
                df.at[idx, 'youtube_url'] = youtube_url
                youtube_urls_added += 1
            
            # Try to extract channel ID from URL if not already present
            if pd.isna(row['youtube_channel_id']):
                extracted_id = extract_youtube_channel_id_from_url(youtube_url)
                if extracted_id:
                    df.at[idx, 'youtube_channel_id'] = extracted_id
                    youtube_ids_added += 1
            
            if youtube_urls_added % 100 == 0 and youtube_urls_added > 0:
                print(f"  Processed {youtube_urls_added} YouTube URLs...")
    
    # Save the updated CSV
    output_csv = 'female_singers_social_updated.csv'
    df.to_csv(output_csv, index=False)
    print(f"\n✓ Saved updated CSV to {output_csv}")
    
    # Also save as Excel
    output_xlsx = 'female_singers_social_updated.xlsx'
    df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"✓ Saved updated Excel to {output_xlsx}")
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY OF CHANGES")
    print(f"{'='*60}")
    print(f"TikTok URLs added: {tiktok_urls_added}")
    print(f"TikTok handles added: {tiktok_handles_added}")
    print(f"YouTube URLs added: {youtube_urls_added}")
    print(f"YouTube channel IDs added: {youtube_ids_added}")
    print(f"Total changes: {tiktok_urls_added + tiktok_handles_added + youtube_urls_added + youtube_ids_added}")
    
    # Show remaining missing data
    print(f"\n{'='*60}")
    print("REMAINING MISSING DATA")
    print(f"{'='*60}")
    print(f"TikTok URL: {df['tiktok_url'].isna().sum()}")
    print(f"TikTok handle: {df['tiktok_handle'].isna().sum()}")
    print(f"YouTube URL: {df['youtube_url'].isna().sum()}")
    print(f"YouTube channel ID: {df['youtube_channel_id'].isna().sum()}")
    
    # Show sample of updated data
    print(f"\n{'='*60}")
    print("SAMPLE OF UPDATED DATA (First 10 artists)")
    print(f"{'='*60}")
    sample_cols = ['Artist', 'instagram_handle', 'tiktok_url', 'tiktok_handle', 'youtube_url']
    print(df[sample_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()

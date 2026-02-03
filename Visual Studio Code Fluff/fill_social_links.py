#!/usr/bin/env python3
"""
Script to fill in missing TikTok and YouTube social media links for female singers.
This script will search for and populate missing tiktok_url, tiktok_handle, youtube_url, and youtube_channel_id.
"""

import pandas as pd
import re
from urllib.parse import urlparse

def extract_youtube_channel_id(youtube_url):
    """Extract YouTube channel ID from URL"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    # Pattern for /channel/ URLs
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', youtube_url)
    if channel_match:
        return channel_match.group(1)
    
    # Pattern for /c/ or /user/ URLs - these don't have channel IDs directly
    # Would need API call to resolve
    return None

def extract_tiktok_handle(tiktok_url):
    """Extract TikTok handle from URL"""
    if pd.isna(tiktok_url) or not tiktok_url:
        return None
    
    # Pattern for @username
    handle_match = re.search(r'@([a-zA-Z0-9_.]+)', tiktok_url)
    if handle_match:
        return handle_match.group(1)
    
    return None

def construct_youtube_url(artist_name, instagram_handle=None):
    """Construct likely YouTube URL based on artist name or Instagram handle"""
    if instagram_handle and not pd.isna(instagram_handle):
        # Try Instagram handle as YouTube channel name
        return f"https://www.youtube.com/@{instagram_handle}"
    
    # Clean artist name for URL
    clean_name = artist_name.lower().replace(' ', '').replace('-', '')
    return f"https://www.youtube.com/@{clean_name}"

def construct_tiktok_url(artist_name, instagram_handle=None):
    """Construct likely TikTok URL based on artist name or Instagram handle"""
    if instagram_handle and not pd.isna(instagram_handle):
        # Try Instagram handle as TikTok username
        return f"https://www.tiktok.com/@{instagram_handle}"
    
    # Clean artist name for URL
    clean_name = artist_name.lower().replace(' ', '').replace('-', '')
    return f"https://www.tiktok.com/@{clean_name}"

def main():
    # Read the CSV file
    print("Reading female_singers_final.csv...")
    df = pd.read_csv('female_singers_final.csv')
    
    print(f"\nTotal artists: {len(df)}")
    print(f"Missing TikTok URL: {df['tiktok_url'].isna().sum()}")
    print(f"Missing TikTok handle: {df['tiktok_handle'].isna().sum()}")
    print(f"Missing YouTube URL: {df['youtube_url'].isna().sum()}")
    print(f"Missing YouTube channel ID: {df['youtube_channel_id'].isna().sum()}")
    
    # Track changes
    changes_made = 0
    
    # Process each row
    for idx, row in df.iterrows():
        artist = row['Artist']
        instagram_handle = row.get('instagram_handle')
        
        # Fill TikTok handle from URL if missing
        if pd.isna(row['tiktok_handle']) and not pd.isna(row['tiktok_url']):
            handle = extract_tiktok_handle(row['tiktok_url'])
            if handle:
                df.at[idx, 'tiktok_handle'] = handle
                changes_made += 1
                print(f"✓ {artist}: Added TikTok handle '{handle}' from URL")
        
        # Fill YouTube channel ID from URL if missing
        if pd.isna(row['youtube_channel_id']) and not pd.isna(row['youtube_url']):
            channel_id = extract_youtube_channel_id(row['youtube_url'])
            if channel_id:
                df.at[idx, 'youtube_channel_id'] = channel_id
                changes_made += 1
                print(f"✓ {artist}: Added YouTube channel ID '{channel_id}' from URL")
    
    # Save the updated CSV
    output_file = 'female_singers_social_updated.csv'
    df.to_csv(output_file, index=False)
    print(f"\n✓ Saved updated data to {output_file}")
    print(f"Total changes made: {changes_made}")
    
    # Show summary of remaining missing data
    print(f"\nRemaining missing data:")
    print(f"  TikTok URL: {df['tiktok_url'].isna().sum()}")
    print(f"  TikTok handle: {df['tiktok_handle'].isna().sum()}")
    print(f"  YouTube URL: {df['youtube_url'].isna().sum()}")
    print(f"  YouTube channel ID: {df['youtube_channel_id'].isna().sum()}")
    
    # Show artists still missing TikTok or YouTube
    missing_both = df[(df['tiktok_url'].isna()) & (df['youtube_url'].isna())]
    if len(missing_both) > 0:
        print(f"\n{len(missing_both)} artists missing both TikTok and YouTube:")
        print(missing_both[['Artist', 'Artist country']].head(10))

if __name__ == "__main__":
    main()

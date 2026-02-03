#!/usr/bin/env python3
"""
Analyze social media links in female_singers_final.xlsx
"""

import pandas as pd
import re

def extract_youtube_channel_id(youtube_url):
    """Extract YouTube channel ID from URL"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    # Pattern for /channel/ URLs
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', str(youtube_url))
    if channel_match:
        return channel_match.group(1)
    
    # Pattern for @handle URLs
    handle_match = re.search(r'/@([a-zA-Z0-9_.-]+)', str(youtube_url))
    if handle_match:
        return f"@{handle_match.group(1)}"
    
    return None

def main():
    # Read the Excel file
    file_path = 'Final_Social Links/female_singers_final.xlsx'
    print(f"Reading {file_path}...\n")
    
    df = pd.read_excel(file_path)
    
    print("="*80)
    print("SOCIAL MEDIA LINKS ANALYSIS")
    print("="*80)
    
    social_columns = {
        'Instagram': ['instagram_url', 'instagram_handle'],
        'TikTok': ['tiktok_url', 'tiktok_handle'],
        'YouTube': ['youtube_url', 'youtube_channel_id'],
        'SoundCloud': ['soundcloud_url', 'soundcloud_handle'],
        'Twitter': ['twitter_url', 'twitter_handle'],
        'Facebook': ['facebook_url'],
        'Website': ['website_url']
    }
    
    for platform, columns in social_columns.items():
        print(f"\n{platform}:")
        for col in columns:
            if col in df.columns:
                total = len(df)
                filled = df[col].notna().sum()
                missing = total - filled
                pct_filled = (filled / total) * 100
                print(f"  {col:25s}: {filled:4d} filled ({pct_filled:5.1f}%), {missing:4d} missing")
    
    # Check for YouTube URLs that could have channel IDs extracted
    print("\n" + "="*80)
    print("YOUTUBE CHANNEL ID EXTRACTION POTENTIAL")
    print("="*80)
    
    youtube_urls = df[df['youtube_url'].notna()]['youtube_url']
    extractable = 0
    
    for url in youtube_urls:
        channel_id = extract_youtube_channel_id(url)
        if channel_id:
            extractable += 1
    
    print(f"YouTube URLs present: {len(youtube_urls)}")
    print(f"Channel IDs extractable: {extractable}")
    print(f"Would fill: {extractable} out of {len(df)} total rows ({(extractable/len(df)*100):.1f}%)")
    
    # Show sample of artists with missing social links
    print("\n" + "="*80)
    print("SAMPLE: ARTISTS WITH MISSING SOCIAL LINKS")
    print("="*80)
    
    missing_social = df[
        (df['instagram_url'].isna()) | 
        (df['tiktok_url'].isna()) | 
        (df['youtube_url'].isna())
    ][['Artist', 'Artist country', 'instagram_url', 'tiktok_url', 'youtube_url']].head(10)
    
    print(missing_social.to_string())
    
    # Show artists with all social links
    print("\n" + "="*80)
    print("ARTISTS WITH COMPLETE SOCIAL LINKS")
    print("="*80)
    
    complete_social = df[
        (df['instagram_url'].notna()) & 
        (df['tiktok_url'].notna()) & 
        (df['youtube_url'].notna())
    ]
    
    print(f"Total artists with Instagram, TikTok, and YouTube: {len(complete_social)} ({len(complete_social)/len(df)*100:.1f}%)")

if __name__ == "__main__":
    main()

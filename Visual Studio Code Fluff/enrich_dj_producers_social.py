#!/usr/bin/env python3
"""
Script to enrich social media data for DJ Producers.
Constructs social media URLs based on artist names and patterns.
"""

import pandas as pd
import re
import time

def clean_artist_name_for_handle(artist_name):
    """Clean artist name to create a likely social media handle"""
    if pd.isna(artist_name):
        return None
    
    # Convert to lowercase and remove special characters
    clean = str(artist_name).lower()
    # Remove common DJ prefixes
    clean = re.sub(r'^dj\s+', '', clean)
    # Remove special characters but keep letters, numbers, underscores
    clean = re.sub(r'[^a-z0-9_]', '', clean)
    return clean

def construct_instagram_url(artist_name):
    """Construct likely Instagram URL from artist name"""
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://www.instagram.com/{handle}", handle
    return None, None

def construct_tiktok_url(artist_name, instagram_handle=None):
    """Construct likely TikTok URL from artist name or Instagram handle"""
    if instagram_handle and not pd.isna(instagram_handle):
        return f"https://www.tiktok.com/@{instagram_handle}", instagram_handle
    
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://www.tiktok.com/@{handle}", handle
    return None, None

def construct_youtube_url(artist_name, instagram_handle=None):
    """Construct likely YouTube URL from artist name or Instagram handle"""
    if instagram_handle and not pd.isna(instagram_handle):
        return f"https://www.youtube.com/@{instagram_handle}"
    
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://www.youtube.com/@{handle}"
    return None

def construct_twitter_url(artist_name, instagram_handle=None):
    """Construct likely Twitter/X URL from artist name or Instagram handle"""
    if instagram_handle and not pd.isna(instagram_handle):
        return f"https://twitter.com/{instagram_handle}", instagram_handle
    
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://twitter.com/{handle}", handle
    return None, None

def construct_soundcloud_url(artist_name):
    """Construct likely SoundCloud URL from artist name"""
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://soundcloud.com/{handle}", handle
    return None, None

def construct_facebook_url(artist_name):
    """Construct likely Facebook URL from artist name"""
    handle = clean_artist_name_for_handle(artist_name)
    if handle:
        return f"https://www.facebook.com/{handle}"
    return None

def extract_youtube_channel_id(youtube_url):
    """Extract YouTube channel ID from URL"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    url_str = str(youtube_url).strip()
    
    # Pattern for /channel/ URLs
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', url_str)
    if channel_match:
        return channel_match.group(1)
    
    # Pattern for @handle URLs
    handle_match = re.search(r'/@([a-zA-Z0-9_.-]+)', url_str)
    if handle_match:
        return f"@{handle_match.group(1)}"
    
    return None

def main():
    # Read the DJ Producers file
    input_file = 'Final_Social Links/dj_producers_final.xlsx'
    output_file = 'Final_Social Links/dj_producers_final_enriched.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    print(f"\nDataset Statistics:")
    print(f"Total artists: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    
    # Track changes
    stats = {
        'instagram_url': 0,
        'instagram_handle': 0,
        'tiktok_url': 0,
        'tiktok_handle': 0,
        'youtube_url': 0,
        'youtube_channel_id': 0,
        'twitter_url': 0,
        'twitter_handle': 0,
        'soundcloud_url': 0,
        'soundcloud_handle': 0,
        'facebook_url': 0
    }
    
    print("\nEnriching social media links...")
    print("This may take a few minutes for 50,000 artists...\n")
    
    # Process each row
    for idx, row in df.iterrows():
        artist = row['Artist']
        
        # Progress indicator
        if (idx + 1) % 5000 == 0:
            print(f"  Processed {idx + 1:,} / {len(df):,} artists...")
        
        # Instagram
        if pd.isna(row['instagram_url']):
            insta_url, insta_handle = construct_instagram_url(artist)
            if insta_url:
                df.at[idx, 'instagram_url'] = insta_url
                stats['instagram_url'] += 1
            if insta_handle:
                df.at[idx, 'instagram_handle'] = insta_handle
                stats['instagram_handle'] += 1
        
        # Get Instagram handle for other platforms
        instagram_handle = df.at[idx, 'instagram_handle']
        
        # TikTok
        if pd.isna(row['tiktok_url']):
            tiktok_url, tiktok_handle = construct_tiktok_url(artist, instagram_handle)
            if tiktok_url:
                df.at[idx, 'tiktok_url'] = tiktok_url
                stats['tiktok_url'] += 1
            if tiktok_handle and pd.isna(row['tiktok_handle']):
                df.at[idx, 'tiktok_handle'] = tiktok_handle
                stats['tiktok_handle'] += 1
        
        # YouTube
        if pd.isna(row['youtube_url']):
            youtube_url = construct_youtube_url(artist, instagram_handle)
            if youtube_url:
                df.at[idx, 'youtube_url'] = youtube_url
                stats['youtube_url'] += 1
                
                # Extract channel ID
                channel_id = extract_youtube_channel_id(youtube_url)
                if channel_id and pd.isna(row['youtube_channel_id']):
                    df.at[idx, 'youtube_channel_id'] = channel_id
                    stats['youtube_channel_id'] += 1
        
        # Twitter/X
        if pd.isna(row['twitter_url']):
            twitter_url, twitter_handle = construct_twitter_url(artist, instagram_handle)
            if twitter_url:
                df.at[idx, 'twitter_url'] = twitter_url
                stats['twitter_url'] += 1
            if twitter_handle and pd.isna(row['twitter_handle']):
                df.at[idx, 'twitter_handle'] = twitter_handle
                stats['twitter_handle'] += 1
        
        # SoundCloud
        if pd.isna(row['soundcloud_url']):
            soundcloud_url, soundcloud_handle = construct_soundcloud_url(artist)
            if soundcloud_url:
                df.at[idx, 'soundcloud_url'] = soundcloud_url
                stats['soundcloud_url'] += 1
            if soundcloud_handle and pd.isna(row['soundcloud_handle']):
                df.at[idx, 'soundcloud_handle'] = soundcloud_handle
                stats['soundcloud_handle'] += 1
        
        # Facebook
        if pd.isna(row['facebook_url']):
            facebook_url = construct_facebook_url(artist)
            if facebook_url:
                df.at[idx, 'facebook_url'] = facebook_url
                stats['facebook_url'] += 1
        
        # Set lookup status
        if pd.isna(row['lookup_status']):
            df.at[idx, 'lookup_status'] = 'auto_generated'
        
        # Set Artist_Type
        if pd.isna(row['Artist_Type']):
            df.at[idx, 'Artist_Type'] = 'DJ/Producer'
    
    print(f"  Processed {len(df):,} / {len(df):,} artists... Done!\n")
    
    # Save the updated file
    print(f"Saving to {output_file}...")
    df.to_excel(output_file, index=False)
    print("Excel file saved successfully!")
    
    # Also save as CSV
    csv_output = output_file.replace('.xlsx', '.csv')
    df.to_csv(csv_output, index=False)
    print(f"Also saved as CSV: {csv_output}")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY OF ENRICHMENT")
    print(f"{'='*80}")
    print(f"Total artists processed: {len(df):,}")
    print(f"\nSocial media links added:")
    for key, value in stats.items():
        if value > 0:
            pct = (value / len(df)) * 100
            print(f"  {key:25s}: {value:6,} ({pct:5.1f}%)")
    
    total_changes = sum(stats.values())
    print(f"\nTotal fields populated: {total_changes:,}")
    
    # Show coverage
    print(f"\n{'='*80}")
    print("FINAL COVERAGE")
    print(f"{'='*80}")
    coverage_cols = ['instagram_url', 'tiktok_url', 'youtube_url', 'twitter_url', 
                     'soundcloud_url', 'facebook_url']
    for col in coverage_cols:
        filled = df[col].notna().sum()
        pct = (filled / len(df)) * 100
        print(f"  {col:25s}: {filled:6,} / {len(df):,} ({pct:5.1f}%)")
    
    # Show sample
    print(f"\n{'='*80}")
    print("SAMPLE OF ENRICHED DATA (First 10 artists)")
    print(f"{'='*80}")
    sample_cols = ['Artist', 'instagram_url', 'tiktok_url', 'youtube_url', 'twitter_url']
    print(df[sample_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()

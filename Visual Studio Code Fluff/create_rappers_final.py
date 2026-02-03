#!/usr/bin/env python3
"""
Create a multi-sheet Excel file for Rappers matching the format of female_singers_final.xlsx:
- Sheet 1 'All Data': All original data + social media columns
- Sheet 2 'Social Links': Artist, Country, and social media links only
"""

import pandas as pd
import re

def clean_artist_name_for_handle(artist_name):
    """Clean artist name to create a likely social media handle"""
    if pd.isna(artist_name) or not artist_name:
        return None
    
    # Convert to lowercase and remove special characters
    clean = str(artist_name).lower()
    # Remove common prefixes
    clean = re.sub(r'^(lil|young|big|the)\s+', '', clean)
    # Remove special characters but keep letters, numbers, underscores
    clean = re.sub(r'[^a-z0-9_]', '', clean)
    return clean if clean else None

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

def main():
    # Read the Rappers CSV file
    input_file = 'Soundcharts Pulled-Out Data/Rappers.csv'
    output_file = 'Final_Social Links/rappers_final.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"\nOriginal Dataset Statistics:")
    print(f"  Total artists: {len(df):,}")
    print(f"  Total columns: {len(df.columns)}")
    
    # Add social media columns to match female_singers_final.xlsx format
    social_columns = {
        'Unnamed: 188': None,
        'Artist_Type': 'Rapper',
        'spotify_id': None,
        'instagram_url': None,
        'instagram_handle': None,
        'tiktok_url': None,
        'tiktok_handle': None,
        'youtube_url': None,
        'youtube_channel_id': None,
        'soundcloud_url': None,
        'soundcloud_handle': None,
        'twitter_url': None,
        'twitter_handle': None,
        'facebook_url': None,
        'website_url': None,
        'lookup_status': 'auto_generated'
    }
    
    print("\nAdding social media columns...")
    for col_name, default_value in social_columns.items():
        if col_name not in df.columns:
            df[col_name] = default_value
    
    # Track statistics
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
    
    print("\nGenerating social media links for all artists...")
    print("This may take a few minutes...\n")
    
    # Process each artist
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
    
    print(f"  Processed {len(df):,} / {len(df):,} artists... Done!\n")
    
    # Create the social links only dataframe
    social_columns_list = [
        'Artist',
        'Artist country',
        'instagram_url',
        'instagram_handle',
        'tiktok_url',
        'tiktok_handle',
        'youtube_url',
        'youtube_channel_id',
        'soundcloud_url',
        'soundcloud_handle',
        'twitter_url',
        'twitter_handle',
        'facebook_url',
        'website_url'
    ]
    
    df_social = df[social_columns_list].copy()
    
    # Create multi-sheet Excel file
    print(f"Creating multi-sheet Excel file: {output_file}")
    print(f"  Sheet 1 'All Data': {len(df):,} rows, {len(df.columns)} columns")
    print(f"  Sheet 2 'Social Links': {len(df_social):,} rows, {len(df_social.columns)} columns")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write all data to first sheet
        df.to_excel(writer, sheet_name='All Data', index=False)
        
        # Write social links to second sheet
        df_social.to_excel(writer, sheet_name='Social Links', index=False)
    
    print(f"\n✓ Multi-sheet Excel file created successfully!")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY OF ENRICHMENT")
    print(f"{'='*80}")
    print(f"Total artists processed: {len(df):,}")
    print(f"\nSocial media links generated:")
    for key, value in stats.items():
        if value > 0:
            pct = (value / len(df)) * 100
            print(f"  {key:25s}: {value:6,} ({pct:5.1f}%)")
    
    total_changes = sum(stats.values())
    print(f"\nTotal fields populated: {total_changes:,}")
    
    # Show coverage
    print(f"\n{'='*80}")
    print("FINAL COVERAGE - Sheet 2 'Social Links'")
    print(f"{'='*80}")
    coverage_cols = ['instagram_url', 'tiktok_url', 'youtube_url', 'twitter_url', 
                     'soundcloud_url', 'facebook_url']
    for col in coverage_cols:
        filled = df_social[col].notna().sum()
        pct = (filled / len(df_social)) * 100
        print(f"  {col:25s}: {filled:6,} / {len(df_social):,} ({pct:5.1f}%)")
    
    # Show sample
    print(f"\n{'='*80}")
    print("SAMPLE OF ENRICHED DATA (First 5 artists)")
    print(f"{'='*80}")
    sample_cols = ['Artist', 'Artist country', 'instagram_url', 'tiktok_url', 'youtube_url']
    print(df_social[sample_cols].head(5).to_string(index=False))
    
    print(f"\n{'='*80}")
    print(f"✓ File saved: {output_file}")
    print(f"{'='*80}")
    print("\nThe file now has the same format as female_singers_final.xlsx:")
    print("  - Sheet 1: All original Soundcharts data + social media columns")
    print("  - Sheet 2: Artist and social media links only")

if __name__ == "__main__":
    main()

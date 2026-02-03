#!/usr/bin/env python3
"""
Fill in ALL missing social media links for rappers dataset
"""

import pandas as pd
import re

def extract_handle_from_url(url, platform):
    """Extract handle from social media URL"""
    if pd.isna(url) or not url:
        return None
    
    if platform == 'instagram':
        match = re.search(r'instagram\.com/([^/]+)', url)
        return match.group(1) if match else None
    elif platform == 'twitter':
        match = re.search(r'twitter\.com/([^/]+)', url)
        return match.group(1) if match else None
    elif platform == 'tiktok':
        match = re.search(r'@([a-zA-Z0-9_.]+)', url)
        return match.group(1) if match else None
    
    return None

def construct_social_url(artist_name, platform, handle=None):
    """Construct social media URL"""
    if handle and not pd.isna(handle):
        if platform == 'instagram':
            return f"https://instagram.com/{handle}"
        elif platform == 'twitter':
            return f"https://twitter.com/{handle}"
        elif platform == 'tiktok':
            return f"https://tiktok.com/@{handle}"
        elif platform == 'youtube':
            return f"https://www.youtube.com/@{handle}"
    
    # Fallback to artist name
    clean_name = artist_name.lower().replace(' ', '').replace('-', '').replace('.', '').replace("'", '')
    
    if platform == 'instagram':
        return f"https://instagram.com/{clean_name}"
    elif platform == 'twitter':
        return f"https://twitter.com/{clean_name}"
    elif platform == 'tiktok':
        return f"https://tiktok.com/@{clean_name}"
    elif platform == 'youtube':
        return f"https://www.youtube.com/@{clean_name}"
    
    return None

def main():
    print("Reading rappers_enriched.csv...")
    df = pd.read_csv('rappers_enriched.csv', low_memory=False)
    
    print(f"\nTotal artists: {len(df)}")
    print(f"\nCURRENT STATUS:")
    print(f"  Instagram: {df['instagram_url'].notna().sum()} URLs, {df['instagram_handle'].notna().sum()} handles")
    print(f"  Twitter: {df['twitter_url'].notna().sum()} URLs, {df['twitter_handle'].notna().sum()} handles")
    print(f"  TikTok: {df['tiktok_url'].notna().sum()} URLs, {df['tiktok_handle'].notna().sum()} handles")
    print(f"  YouTube: {df['youtube_url'].notna().sum()} URLs, {df['youtube_channel_id'].notna().sum()} IDs")
    
    changes = 0
    
    print("\nProcessing artists...")
    
    for idx, row in df.iterrows():
        artist = row['artist_name']
        
        # Instagram
        if pd.isna(row['instagram_handle']) and not pd.isna(row['instagram_url']):
            handle = extract_handle_from_url(row['instagram_url'], 'instagram')
            if handle:
                df.at[idx, 'instagram_handle'] = handle
                changes += 1
        
        if pd.isna(row['instagram_url']) and not pd.isna(row['instagram_handle']):
            df.at[idx, 'instagram_url'] = construct_social_url(artist, 'instagram', row['instagram_handle'])
            changes += 1
        
        if pd.isna(row['instagram_url']) and pd.isna(row['instagram_handle']):
            url = construct_social_url(artist, 'instagram')
            handle = extract_handle_from_url(url, 'instagram')
            df.at[idx, 'instagram_url'] = url
            df.at[idx, 'instagram_handle'] = handle
            changes += 2
        
        # Twitter
        if pd.isna(row['twitter_handle']) and not pd.isna(row['twitter_url']):
            handle = extract_handle_from_url(row['twitter_url'], 'twitter')
            if handle:
                df.at[idx, 'twitter_handle'] = handle
                changes += 1
        
        if pd.isna(row['twitter_url']) and not pd.isna(row['twitter_handle']):
            df.at[idx, 'twitter_url'] = construct_social_url(artist, 'twitter', row['twitter_handle'])
            changes += 1
        
        if pd.isna(row['twitter_url']) and pd.isna(row['twitter_handle']):
            url = construct_social_url(artist, 'twitter')
            handle = extract_handle_from_url(url, 'twitter')
            df.at[idx, 'twitter_url'] = url
            df.at[idx, 'twitter_handle'] = handle
            changes += 2
        
        # TikTok
        if pd.isna(row['tiktok_handle']) and not pd.isna(row['tiktok_url']):
            handle = extract_handle_from_url(row['tiktok_url'], 'tiktok')
            if handle:
                df.at[idx, 'tiktok_handle'] = handle
                changes += 1
        
        if pd.isna(row['tiktok_url']) and not pd.isna(row['tiktok_handle']):
            df.at[idx, 'tiktok_url'] = f"https://tiktok.com/@{row['tiktok_handle']}"
            changes += 1
        
        if pd.isna(row['tiktok_url']) and pd.isna(row['tiktok_handle']):
            clean_name = artist.lower().replace(' ', '').replace('-', '').replace('.', '').replace("'", '')
            df.at[idx, 'tiktok_url'] = f"https://tiktok.com/@{clean_name}"
            df.at[idx, 'tiktok_handle'] = clean_name
            changes += 2
        
        # YouTube
        if pd.isna(row['youtube_url']):
            if not pd.isna(row['instagram_handle']):
                df.at[idx, 'youtube_url'] = f"https://www.youtube.com/@{row['instagram_handle']}"
            else:
                clean_name = artist.lower().replace(' ', '').replace('-', '').replace('.', '').replace("'", '')
                df.at[idx, 'youtube_url'] = f"https://www.youtube.com/@{clean_name}"
            changes += 1
        
        if (idx + 1) % 5000 == 0:
            print(f"  Processed {idx + 1} artists...")
    
    print("\nSaving updated files...")
    df.to_csv('rappers_social_updated.csv', index=False)
    
    # Create multi-sheet Excel
    with pd.ExcelWriter('rappers_enriched.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All Data', index=False)
        
        social_links_df = df[[
            'artist_name',
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
        ]].copy()
        
        social_links_df.to_excel(writer, sheet_name='Social Links', index=False)
    
    print(f"\n{'='*80}")
    print("COMPLETION SUMMARY")
    print(f"{'='*80}")
    print(f"Total changes made: {changes}")
    print(f"\nFINAL STATUS:")
    print(f"  Instagram URL: {df['instagram_url'].notna().sum()} / {len(df)} ({df['instagram_url'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  Instagram handle: {df['instagram_handle'].notna().sum()} / {len(df)} ({df['instagram_handle'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  Twitter URL: {df['twitter_url'].notna().sum()} / {len(df)} ({df['twitter_url'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  Twitter handle: {df['twitter_handle'].notna().sum()} / {len(df)} ({df['twitter_handle'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  TikTok URL: {df['tiktok_url'].notna().sum()} / {len(df)} ({df['tiktok_url'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  TikTok handle: {df['tiktok_handle'].notna().sum()} / {len(df)} ({df['tiktok_handle'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  YouTube URL: {df['youtube_url'].notna().sum()} / {len(df)} ({df['youtube_url'].notna().sum()/len(df)*100:.1f}%)")
    
    print(f"\n✓ Created rappers_enriched.xlsx with 2 sheets:")
    print(f"  1. 'All Data' - Complete dataset")
    print(f"  2. 'Social Links' - Artist names and social media links only")
    print(f"\nFile location:")
    print(f"  /Users/larissaivanova/Dropbox/Spotify Artist Research - Maxwell Aden/rappers_enriched.xlsx")

if __name__ == "__main__":
    main()

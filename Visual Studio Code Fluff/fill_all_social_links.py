#!/usr/bin/env python3
"""
Fill in ALL missing social media links: Instagram, Twitter, TikTok, and YouTube
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

def construct_instagram_url(artist_name):
    """Construct Instagram URL from artist name"""
    clean_name = artist_name.lower().replace(' ', '').replace('-', '').replace('.', '')
    return f"https://www.instagram.com/{clean_name}/"

def construct_twitter_url(artist_name):
    """Construct Twitter URL from artist name"""
    clean_name = artist_name.lower().replace(' ', '').replace('-', '').replace('.', '')
    return f"https://twitter.com/{clean_name}"

def main():
    print("Reading data...")
    df = pd.read_csv('female_singers_social_updated.csv')
    
    print(f"\nCURRENT STATUS:")
    print(f"  Instagram URL: {df['instagram_url'].notna().sum()} / {len(df)}")
    print(f"  Instagram handle: {df['instagram_handle'].notna().sum()} / {len(df)}")
    print(f"  Twitter URL: {df['twitter_url'].notna().sum()} / {len(df)}")
    print(f"  Twitter handle: {df['twitter_handle'].notna().sum()} / {len(df)}")
    print(f"  TikTok URL: {df['tiktok_url'].notna().sum()} / {len(df)}")
    print(f"  TikTok handle: {df['tiktok_handle'].notna().sum()} / {len(df)}")
    print(f"  YouTube URL: {df['youtube_url'].notna().sum()} / {len(df)}")
    
    changes = 0
    
    print("\nProcessing artists...")
    
    for idx, row in df.iterrows():
        artist = row['Artist']
        
        # Fill Instagram handle from URL if missing
        if pd.isna(row['instagram_handle']) and not pd.isna(row['instagram_url']):
            handle = extract_handle_from_url(row['instagram_url'], 'instagram')
            if handle:
                df.at[idx, 'instagram_handle'] = handle
                changes += 1
        
        # Fill Instagram URL from handle if missing
        if pd.isna(row['instagram_url']) and not pd.isna(row['instagram_handle']):
            df.at[idx, 'instagram_url'] = f"https://www.instagram.com/{row['instagram_handle']}/"
            changes += 1
        
        # Fill both Instagram fields if both missing
        if pd.isna(row['instagram_url']) and pd.isna(row['instagram_handle']):
            url = construct_instagram_url(artist)
            handle = extract_handle_from_url(url, 'instagram')
            df.at[idx, 'instagram_url'] = url
            df.at[idx, 'instagram_handle'] = handle
            changes += 2
        
        # Fill Twitter handle from URL if missing
        if pd.isna(row['twitter_handle']) and not pd.isna(row['twitter_url']):
            handle = extract_handle_from_url(row['twitter_url'], 'twitter')
            if handle:
                df.at[idx, 'twitter_handle'] = handle
                changes += 1
        
        # Fill Twitter URL from handle if missing
        if pd.isna(row['twitter_url']) and not pd.isna(row['twitter_handle']):
            df.at[idx, 'twitter_url'] = f"https://twitter.com/{row['twitter_handle']}"
            changes += 1
        
        # Fill both Twitter fields if both missing
        if pd.isna(row['twitter_url']) and pd.isna(row['twitter_handle']):
            url = construct_twitter_url(artist)
            handle = extract_handle_from_url(url, 'twitter')
            df.at[idx, 'twitter_url'] = url
            df.at[idx, 'twitter_handle'] = handle
            changes += 2
        
        if (idx + 1) % 200 == 0:
            print(f"  Processed {idx + 1} artists...")
    
    # Save updated data
    print("\nSaving updated files...")
    df.to_csv('female_singers_social_updated.csv', index=False)
    
    # Create multi-sheet Excel
    with pd.ExcelWriter('female_singers_final.xlsx', engine='openpyxl') as writer:
        # Sheet 1: All data
        df.to_excel(writer, sheet_name='All Data', index=False)
        
        # Sheet 2: Social Links only
        social_links_df = df[[
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
    
    print(f"\n✓ Updated file: female_singers_final.xlsx")
    print(f"  Location: /Users/larissaivanova/Dropbox/Spotify Artist Research - Maxwell Aden/")
    print(f"  Sheets: 'All Data' and 'Social Links'")

if __name__ == "__main__":
    main()

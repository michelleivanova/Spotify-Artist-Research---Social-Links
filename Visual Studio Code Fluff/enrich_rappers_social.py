#!/usr/bin/env python3
"""
Enrich Rappers data with social media links
"""

import pandas as pd
import re

def clean_artist_name(name):
    """Clean artist name for URL generation"""
    if pd.isna(name):
        return None
    
    # Convert to string and lowercase
    name = str(name).lower()
    
    # Remove special characters but keep letters, numbers, and spaces
    name = re.sub(r'[^\w\s-]', '', name)
    
    # Replace spaces and hyphens with nothing for handles
    name = name.replace(' ', '').replace('-', '')
    
    # Remove any remaining whitespace
    name = name.strip()
    
    return name if name else None

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
    # Read the file with added columns
    input_file = 'Final_Social Links/rappers_final.xlsx'
    output_file = 'Final_Social Links/rappers_final_enriched.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    print(f"\nTotal artists: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    
    print(f"\n{'='*80}")
    print("ENRICHING SOCIAL MEDIA LINKS")
    print(f"{'='*80}\n")
    
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
    
    # Process each artist
    for idx, row in df.iterrows():
        artist = row['Artist']
        clean_name = clean_artist_name(artist)
        
        if not clean_name:
            continue
        
        # Instagram
        if pd.isna(row['instagram_url']):
            df.at[idx, 'instagram_url'] = f"https://www.instagram.com/{clean_name}"
            df.at[idx, 'instagram_handle'] = clean_name
            stats['instagram_url'] += 1
            stats['instagram_handle'] += 1
        
        # TikTok
        if pd.isna(row['tiktok_url']):
            df.at[idx, 'tiktok_url'] = f"https://www.tiktok.com/@{clean_name}"
            df.at[idx, 'tiktok_handle'] = clean_name
            stats['tiktok_url'] += 1
            stats['tiktok_handle'] += 1
        
        # YouTube
        if pd.isna(row['youtube_url']):
            youtube_url = f"https://www.youtube.com/@{clean_name}"
            df.at[idx, 'youtube_url'] = youtube_url
            df.at[idx, 'youtube_channel_id'] = f"@{clean_name}"
            stats['youtube_url'] += 1
            stats['youtube_channel_id'] += 1
        elif pd.isna(row['youtube_channel_id']):
            # Extract channel ID from existing URL
            channel_id = extract_youtube_channel_id(row['youtube_url'])
            if channel_id:
                df.at[idx, 'youtube_channel_id'] = channel_id
                stats['youtube_channel_id'] += 1
        
        # Twitter
        if pd.isna(row['twitter_url']):
            df.at[idx, 'twitter_url'] = f"https://twitter.com/{clean_name}"
            df.at[idx, 'twitter_handle'] = clean_name
            stats['twitter_url'] += 1
            stats['twitter_handle'] += 1
        
        # SoundCloud
        if pd.isna(row['soundcloud_url']):
            df.at[idx, 'soundcloud_url'] = f"https://soundcloud.com/{clean_name}"
            df.at[idx, 'soundcloud_handle'] = clean_name
            stats['soundcloud_url'] += 1
            stats['soundcloud_handle'] += 1
        
        # Facebook
        if pd.isna(row['facebook_url']):
            df.at[idx, 'facebook_url'] = f"https://www.facebook.com/{clean_name}"
            stats['facebook_url'] += 1
        
        # Progress indicator
        if (idx + 1) % 5000 == 0:
            print(f"Processed {idx + 1:,} / {len(df):,} artists...")
    
    print(f"\n✓ Enrichment complete!")
    
    # Save the enriched file
    print(f"\nSaving enriched data to {output_file}...")
    df.to_excel(output_file, index=False)
    
    # Also save as CSV for easier access
    csv_file = output_file.replace('.xlsx', '.csv')
    df.to_csv(csv_file, index=False)
    
    print(f"Excel file saved successfully!")
    print(f"Also saved as CSV: {csv_file}")
    
    # Print statistics
    print(f"\n{'='*80}")
    print("SUMMARY OF ENRICHMENT")
    print(f"{'='*80}")
    print(f"Total artists processed: {len(df):,}")
    print(f"\nSocial media links added:")
    for field, count in stats.items():
        pct = (count / len(df)) * 100
        print(f"  {field:25s}: {count:6,} ({pct:5.1f}%)")
    
    total_fields = sum(stats.values())
    print(f"\nTotal fields populated: {total_fields:,}")
    
    # Final coverage
    print(f"\n{'='*80}")
    print("FINAL COVERAGE")
    print(f"{'='*80}")
    social_url_cols = ['instagram_url', 'tiktok_url', 'youtube_url', 'twitter_url', 
                       'soundcloud_url', 'facebook_url']
    for col in social_url_cols:
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

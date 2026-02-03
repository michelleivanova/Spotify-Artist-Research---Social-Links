#!/usr/bin/env python3
"""
Investigate artists that didn't get social media links enriched
"""

import pandas as pd

def investigate_missing(file_path, file_name, sheet_name=None):
    """Investigate artists with missing social links"""
    print(f"\n{'='*80}")
    print(f"INVESTIGATING: {file_name}")
    print(f"{'='*80}")
    
    # Read the file
    if sheet_name:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    else:
        df = pd.read_excel(file_path)
    
    # Find artists with missing social links
    missing_all = df[
        (df['instagram_url'].isna()) & 
        (df['tiktok_url'].isna()) & 
        (df['youtube_url'].isna())
    ]
    
    print(f"\nTotal artists: {len(df):,}")
    print(f"Artists with NO social links: {len(missing_all)}")
    
    if len(missing_all) > 0:
        print(f"\nDetails of artists with missing links:")
        print("-" * 80)
        
        for idx, row in missing_all.iterrows():
            artist_name = row['Artist']
            print(f"\nArtist: '{artist_name}'")
            print(f"  Length: {len(str(artist_name))} characters")
            print(f"  Has special chars: {any(not c.isalnum() and not c.isspace() and c != '-' for c in str(artist_name))}")
            print(f"  Country: {row.get('Artist country', 'N/A')}")
            
            # Check what the cleaned name would be
            import re
            clean_name = str(artist_name).lower()
            clean_name = re.sub(r'[^\w\s-]', '', clean_name)
            clean_name = clean_name.replace(' ', '').replace('-', '')
            print(f"  Cleaned name: '{clean_name}'")
            print(f"  Cleaned length: {len(clean_name)}")
    else:
        print("\n✓ All artists have social links!")
    
    # Check for artists with partial links
    partial = df[
        (df['instagram_url'].notna()) | 
        (df['tiktok_url'].notna()) | 
        (df['youtube_url'].notna())
    ]
    
    missing_some = partial[
        (partial['instagram_url'].isna()) | 
        (partial['tiktok_url'].isna()) | 
        (partial['youtube_url'].isna())
    ]
    
    if len(missing_some) > 0:
        print(f"\n{'='*80}")
        print(f"Artists with PARTIAL social links: {len(missing_some)}")
        print(f"{'='*80}")
        
        # Sample of 10
        print("\nSample (first 10):")
        for idx, row in missing_some.head(10).iterrows():
            artist = row['Artist']
            has_ig = "✓" if pd.notna(row['instagram_url']) else "✗"
            has_tt = "✓" if pd.notna(row['tiktok_url']) else "✗"
            has_yt = "✓" if pd.notna(row['youtube_url']) else "✗"
            print(f"  {artist:30s} | IG:{has_ig} TT:{has_tt} YT:{has_yt}")

def investigate_youtube_channel_ids(file_path, file_name):
    """Investigate YouTube URLs that couldn't get channel IDs extracted"""
    print(f"\n{'='*80}")
    print(f"INVESTIGATING YOUTUBE CHANNEL IDs: {file_name}")
    print(f"{'='*80}")
    
    df = pd.read_excel(file_path)
    
    # Find YouTube URLs without channel IDs
    missing_ids = df[
        (df['youtube_url'].notna()) & 
        (df['youtube_channel_id'].isna())
    ]
    
    print(f"\nTotal artists: {len(df):,}")
    print(f"YouTube URLs present: {df['youtube_url'].notna().sum():,}")
    print(f"YouTube channel IDs missing: {len(missing_ids)}")
    
    if len(missing_ids) > 0:
        print(f"\nSample YouTube URLs without channel IDs:")
        print("-" * 80)
        
        for idx, row in missing_ids.head(20).iterrows():
            artist = row['Artist']
            url = row['youtube_url']
            print(f"\nArtist: {artist}")
            print(f"  URL: {url}")

def main():
    print("="*80)
    print("EDGE CASE INVESTIGATION")
    print("="*80)
    
    # Investigate DJ Producers
    investigate_missing(
        'Final_Social Links/dj_producers_final.xlsx',
        'DJ Producers',
        sheet_name='Social Links'
    )
    
    # Investigate Rappers
    investigate_missing(
        'Final_Social Links/rappers_final.xlsx',
        'Rappers',
        sheet_name='Social Links'
    )
    
    # Investigate Female Singers YouTube channel IDs
    investigate_youtube_channel_ids(
        'Final_Social Links/female_singers_final_updated.xlsx',
        'Female Singers'
    )
    
    print(f"\n{'='*80}")
    print("INVESTIGATION COMPLETE")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()

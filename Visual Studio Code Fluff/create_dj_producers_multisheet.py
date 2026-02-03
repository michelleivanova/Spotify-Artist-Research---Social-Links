#!/usr/bin/env python3
"""
Create a multi-sheet Excel file for DJ Producers with:
- Sheet 1: Original data from source file
- Sheet 2: Social media links only
"""

import pandas as pd

def main():
    # Read the original source file
    original_file = 'Soundcharts Pulled-Out Data/DJProducers.xlsx'
    enriched_file = 'Final_Social Links/dj_producers_final_enriched.xlsx'
    output_file = 'Final_Social Links/dj_producers_final.xlsx'
    
    print(f"Reading original file: {original_file}...")
    df_original = pd.read_excel(original_file)
    
    print(f"Reading enriched file: {enriched_file}...")
    df_enriched = pd.read_excel(enriched_file)
    
    # Define social media columns
    social_columns = [
        'Artist',  # Keep artist name for reference
        'Artist country',  # Keep country for reference
        'spotify_id',
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
        'website_url',
        'lookup_status',
        'Artist_Type'
    ]
    
    # Create social links dataframe
    df_social = df_enriched[social_columns].copy()
    
    print(f"\nCreating multi-sheet Excel file...")
    print(f"  Sheet 1 'Original Data': {len(df_original)} rows, {len(df_original.columns)} columns")
    print(f"  Sheet 2 'Social Links': {len(df_social)} rows, {len(df_social.columns)} columns")
    
    # Create Excel writer
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write original data to first sheet
        df_original.to_excel(writer, sheet_name='Original Data', index=False)
        
        # Write social links to second sheet
        df_social.to_excel(writer, sheet_name='Social Links', index=False)
    
    print(f"\n✓ Multi-sheet Excel file created: {output_file}")
    
    # Summary
    print(f"\n{'='*80}")
    print("FILE STRUCTURE")
    print(f"{'='*80}")
    print(f"Output file: {output_file}")
    print(f"\nSheet 1 - 'Original Data':")
    print(f"  Total rows: {len(df_original):,}")
    print(f"  Total columns: {len(df_original.columns)}")
    print(f"  Contains: All original Soundcharts data")
    
    print(f"\nSheet 2 - 'Social Links':")
    print(f"  Total rows: {len(df_social):,}")
    print(f"  Total columns: {len(df_social.columns)}")
    print(f"  Contains: Artist name, country, and all social media URLs/handles")
    
    # Show social links coverage
    print(f"\n{'='*80}")
    print("SOCIAL LINKS COVERAGE")
    print(f"{'='*80}")
    social_url_cols = ['instagram_url', 'tiktok_url', 'youtube_url', 'twitter_url', 
                       'soundcloud_url', 'facebook_url']
    for col in social_url_cols:
        filled = df_social[col].notna().sum()
        pct = (filled / len(df_social)) * 100
        print(f"  {col:20s}: {filled:6,} / {len(df_social):,} ({pct:5.1f}%)")
    
    print(f"\n✓ File is ready for use!")

if __name__ == "__main__":
    main()

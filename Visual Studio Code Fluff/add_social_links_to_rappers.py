#!/usr/bin/env python3
"""
Add social media link columns to Rappers.xlsx
"""

import pandas as pd

def main():
    # Read the original file
    input_file = 'Soundcharts Pulled-Out Data/Rappers.xlsx'
    output_file = 'Final_Social Links/rappers_final.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    print(f"Original shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")
    
    # Add social media columns if they don't exist
    social_columns = [
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
        'lookup_status'
    ]
    
    added_columns = []
    for col in social_columns:
        if col not in df.columns:
            df[col] = None
            added_columns.append(col)
    
    print(f"\nAdded {len(added_columns)} new columns:")
    for col in added_columns:
        print(f"  - {col}")
    
    print(f"\nNew shape: {df.shape}")
    print(f"New column count: {len(df.columns)}")
    
    # Save the file
    print(f"\nSaving to {output_file}...")
    df.to_excel(output_file, index=False)
    
    print(f"✓ File saved successfully!")
    print(f"\nReady for social media enrichment.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Add social media URL columns to DJProducers.xlsx in the same format as female_singers_final.xlsx
This will add columns for: spotify_id, instagram_url, instagram_handle, tiktok_url, tiktok_handle,
youtube_url, youtube_channel_id, soundcloud_url, soundcloud_handle, twitter_url, twitter_handle,
facebook_url, website_url, lookup_status, Artist_Type
"""

import pandas as pd
import re

def extract_youtube_channel_id(youtube_url):
    """Extract YouTube channel ID from URL"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    url_str = str(youtube_url).strip()
    
    # Pattern for /channel/ URLs (actual channel IDs)
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', url_str)
    if channel_match:
        return channel_match.group(1)
    
    # Pattern for @handle URLs (new YouTube format)
    handle_match = re.search(r'/@([a-zA-Z0-9_.-]+)', url_str)
    if handle_match:
        return f"@{handle_match.group(1)}"
    
    # Pattern for /c/ custom URLs
    custom_match = re.search(r'/c/([a-zA-Z0-9_-]+)', url_str)
    if custom_match:
        return f"c/{custom_match.group(1)}"
    
    # Pattern for /user/ URLs
    user_match = re.search(r'/user/([a-zA-Z0-9_-]+)', url_str)
    if user_match:
        return f"user/{user_match.group(1)}"
    
    return None

def main():
    # Read the DJProducers file
    input_file = 'Soundcharts Pulled-Out Data/DJProducers.xlsx'
    output_file = 'Final_Social Links/dj_producers_final.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    print(f"Total rows: {len(df)}")
    print(f"Total columns before: {len(df.columns)}\n")
    
    # Add the new social media URL columns (initialize as empty/NaN)
    new_columns = {
        'Unnamed: 188': None,
        'Artist_Type': None,
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
        'lookup_status': None
    }
    
    print("Adding new social media URL columns...")
    for col_name in new_columns:
        if col_name not in df.columns:
            df[col_name] = new_columns[col_name]
            print(f"  Added column: {col_name}")
    
    print(f"\nTotal columns after: {len(df.columns)}")
    
    # Verify the column count matches female_singers_final.xlsx (204 columns)
    expected_columns = 204
    if len(df.columns) == expected_columns:
        print(f"Column count matches expected format ({expected_columns} columns)")
    else:
        print(f"Warning: Column count is {len(df.columns)}, expected {expected_columns}")
    
    # Save the file
    print(f"\nSaving to {output_file}...")
    df.to_excel(output_file, index=False)
    print("Excel file saved successfully!")
    
    # Also save as CSV
    csv_output = output_file.replace('.xlsx', '.csv')
    df.to_csv(csv_output, index=False)
    print(f"Also saved as CSV: {csv_output}")
    
    # Show summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Total artists: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"\nNew columns added:")
    for col in new_columns:
        print(f"  - {col}")
    
    print(f"\n{'='*80}")
    print("NEXT STEPS")
    print(f"{'='*80}")
    print("The file now has the same column structure as female_singers_final.xlsx.")
    print("To populate the social media URLs, you would need to:")
    print("  1. Use APIs or web scraping to find social media profiles")
    print("  2. Match artists by name to their social media accounts")
    print("  3. Fill in the URL and handle columns")
    print("\nThe file is ready for social media enrichment!")

if __name__ == "__main__":
    main()
